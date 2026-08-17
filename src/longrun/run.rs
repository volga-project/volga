use std::fs;
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};

use crate::api::CheckpointSpec;
use crate::runtime::consts::{init_runtime_consts_profile, RuntimeConstsProfile};
use crate::runtime::master::LifecycleEvent;
use crate::test_utils::checkpoint::{
    wait_for_checkpoint_completed, wait_for_kill_restore, wait_until_attempt0_running,
    wait_until_attempt_running, wait_until_checkpoints_idle,
};
use crate::test_utils::harness::{RuntimeEnv, VolgaCluster, WorkerKillMode};
use crate::test_utils::recovery::RecoveryTimeouts;

use super::dump::{dump_prom_series, prom_queries};
use super::oracles::{self, KillWindow, PromSeriesSet};
use super::spec::{
    resolved_checkpoint_interval, resolved_checkpoint_timeout, SoakScenario, SoakSpec,
};

const PROM_STEP: &str = "5s";

#[derive(Clone)]
struct ScenarioOutcome {
    kill: Option<KillWindow>,
    started_unix: f64,
}

fn checkpoint_wait(checkpoint: &CheckpointSpec) -> Duration {
    let interval = resolved_checkpoint_interval(checkpoint);
    interval * 3 + interval
}

fn checkpoint_idle(checkpoint: &CheckpointSpec) -> Duration {
    resolved_checkpoint_timeout(checkpoint) + resolved_checkpoint_interval(checkpoint)
}

pub async fn run_soak(spec: SoakSpec) -> Result<()> {
    init_runtime_consts_profile(RuntimeConstsProfile::Prod);

    println!(
        "[volga-longrun] env={} scenario={} duration={:?}",
        spec.env,
        spec.scenario.as_str(),
        spec.duration
    );

    let env = spec.env;
    let duration = spec.duration;
    let scenario = spec.scenario;
    let cluster = VolgaCluster::launch(env, spec.launch.clone()).await?;
    let started_unix = unix_now();
    let result = async {
        let scenario_result = tokio::select! {
            result = run_scenario(
                &cluster,
                env,
                scenario,
                duration,
                started_unix,
                &spec.launch.pipeline.state.checkpoint,
            ) => result,
            failed = watch_pipeline_failed(&cluster) => Err(failed),
            _ = shutdown_signal() => {
                println!("[volga-longrun] interrupt, dumping then shutting down");
                Ok(ScenarioOutcome {
                    kill: None,
                    started_unix,
                })
            }
        };
        let outcome = match &scenario_result {
            Ok(outcome) => outcome.clone(),
            Err(_) => ScenarioOutcome {
                kill: None,
                started_unix,
            },
        };
        let observe = teardown_observe(&cluster, &spec, &outcome).await;
        match scenario_result {
            Err(err) => Err(err),
            Ok(_) => observe,
        }
    }
    .await;
    match cluster.shutdown().await {
        Ok(()) => result,
        Err(shutdown_err) => match result {
            Ok(()) => Err(shutdown_err),
            Err(run_err) => Err(run_err),
        },
    }
}

async fn run_scenario(
    cluster: &VolgaCluster,
    env: RuntimeEnv,
    scenario: SoakScenario,
    duration: Duration,
    started_unix: f64,
    checkpoint: &CheckpointSpec,
) -> Result<ScenarioOutcome> {
    let timeouts = RecoveryTimeouts::for_env(env);
    let mut cursor = 0u64;
    cluster.start_execution().await?;
    let started = Instant::now();

    wait_until_attempt0_running(&cluster.master(), &mut cursor, timeouts.attempt_running).await?;
    println!("[volga-longrun] attempt 0 running");

    let kill = match scenario {
        SoakScenario::Steady => None,
        SoakScenario::KillAfterCheckpoint { after_checkpoint } => Some(
            run_kill_after_checkpoint(
                cluster,
                &timeouts,
                &mut cursor,
                after_checkpoint,
                checkpoint,
            )
            .await?,
        ),
    };

    let remaining = duration.saturating_sub(started.elapsed());
    if remaining > Duration::ZERO {
        println!("[volga-longrun] running for remaining {remaining:?}");
        tokio::time::sleep(remaining).await;
    }

    println!(
        "[volga-longrun] soak finished elapsed={:?}",
        started.elapsed()
    );
    Ok(ScenarioOutcome { kill, started_unix })
}

async fn watch_pipeline_failed(cluster: &VolgaCluster) -> anyhow::Error {
    loop {
        match cluster.master().lifecycle_events_since(0).await {
            Ok(events) => {
                for record in events {
                    if let LifecycleEvent::PipelineFailed { detail } = record.event {
                        return anyhow!("pipeline failed: {detail}");
                    }
                }
            }
            Err(err) => return err,
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn run_kill_after_checkpoint(
    cluster: &VolgaCluster,
    timeouts: &RecoveryTimeouts,
    cursor: &mut u64,
    after_checkpoint: u64,
    checkpoint: &CheckpointSpec,
) -> Result<KillWindow> {
    if after_checkpoint < 1 {
        return Err(anyhow!("kill-after-checkpoint must be >= 1"));
    }

    let mut checkpoint_id = 0u64;
    for n in 1..=after_checkpoint {
        checkpoint_id =
            wait_for_checkpoint_completed(&cluster.master(), cursor, checkpoint_wait(checkpoint)).await?;
        println!("[volga-longrun] checkpoint {checkpoint_id} completed ({n}/{after_checkpoint})");
    }

    wait_until_checkpoints_idle(&cluster.master(), checkpoint_idle(checkpoint)).await?;

    let worker_ids = cluster.worker_ids();
    let target = worker_ids
        .first()
        .ok_or_else(|| anyhow!("expected at least one worker"))?;
    println!("[volga-longrun] kill worker={target} after checkpoint>={checkpoint_id}");
    let seq_lo = *cursor + 1;
    let at_unix = unix_now();
    cluster
        .worker(target)
        .ok_or_else(|| anyhow!("missing worker {target}"))?
        .kill_with(WorkerKillMode::Abrupt)
        .await?;

    let restore_timeout =
        timeouts.recovery_started + timeouts.replacement + timeouts.attempt1_running;
    let attempt_id = wait_for_kill_restore(
        &cluster.master(),
        cursor,
        restore_timeout,
        target,
        checkpoint_id,
        1,
    )
    .await?;
    wait_until_attempt_running(
        &cluster.master(),
        cursor,
        timeouts.attempt1_running,
        attempt_id,
        worker_ids.len(),
    )
    .await?;
    let restore_at_unix = unix_now();
    println!("[volga-longrun] restore attempt={attempt_id} running");
    Ok(KillWindow {
        at_unix,
        restore_at_unix,
        after_checkpoint: checkpoint_id,
        seq_lo,
        seq_hi: *cursor,
    })
}

async fn teardown_observe(
    cluster: &VolgaCluster,
    spec: &SoakSpec,
    outcome: &ScenarioOutcome,
) -> Result<()> {
    let events = cluster.master().lifecycle_events_since(0).await?;
    let end_unix = unix_now();

    let mut prom_dump = None;
    if let Some(prom_url) = spec.prom_url.as_deref() {
        println!("[volga-longrun] dumping Prom query_range from {prom_url}");
        let dump = dump_prom_series(
            prom_url,
            outcome.started_unix,
            end_unix,
            PROM_STEP,
            &prom_queries(&spec.extra_prom_queries),
        )
            .await
            .context("prometheus query_range dump")?;
        prom_dump = Some(dump);
    } else {
        println!("[volga-longrun] no --prom-url; skipping Prom dump and Prom oracles");
    }

    if let Some(dir) = &spec.dump {
        write_dump(dir, &events, prom_dump.as_ref())?;
        println!("[volga-longrun] wrote dump to {}", dir.display());
    }

    let prom_series = match &prom_dump {
        Some(dump) => Some(PromSeriesSet::from_dump(dump)?),
        None => None,
    };
    oracles::evaluate(
        &spec.oracles,
        &events,
        outcome.kill.as_ref(),
        prom_series.as_ref(),
        &spec.launch.pipeline.state.checkpoint,
    )?;
    println!("[volga-longrun] oracles passed");
    Ok(())
}

fn write_dump(
    dir: &Path,
    events: &[crate::runtime::master::LifecycleEventRecord],
    prom: Option<&serde_json::Value>,
) -> Result<()> {
    fs::create_dir_all(dir).with_context(|| format!("create dump dir {}", dir.display()))?;
    fs::write(
        dir.join("lifecycle.json"),
        serde_json::to_vec_pretty(events)?,
    )?;
    if let Some(prom) = prom {
        fs::write(dir.join("prom.json"), serde_json::to_vec_pretty(prom)?)?;
    }
    Ok(())
}

fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs_f64()
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
