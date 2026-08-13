use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};

use crate::runtime::consts::{init_runtime_consts_profile, RuntimeConstsProfile};
use crate::test_utils::checkpoint::{
    wait_for_checkpoint_completed, wait_for_kill_restore, wait_until_attempt0_running,
    wait_until_attempt_running, wait_until_checkpoints_idle,
};
use crate::test_utils::harness::{RuntimeEnv, VolgaCluster, WorkerKillMode};
use crate::test_utils::recovery::RecoveryTimeouts;

use super::spec::{SoakScenario, SoakSpec};

/// Prod interval is 30s; wait up to 3× interval plus slack for the first completed CP.
const CHECKPOINT_WAIT: Duration = Duration::from_secs(120);
/// Prod checkpoint timeout is 60s; idle wait must cover an in-flight CP before kill.
const CHECKPOINT_IDLE: Duration = Duration::from_secs(90);

pub async fn run_soak(spec: SoakSpec) -> Result<()> {
    init_runtime_consts_profile(RuntimeConstsProfile::Prod);

    if let Some(path) = &spec.dump {
        println!(
            "[volga-longrun] dump path {} ignored until PR G",
            path.display()
        );
    }
    println!("[volga-longrun] oracles not evaluated until PR G");

    println!(
        "[volga-longrun] env={} scenario={} duration={:?}",
        spec.env,
        spec.scenario.as_str(),
        spec.duration
    );

    let env = spec.env;
    let duration = spec.duration;
    let scenario = spec.scenario;
    let cluster = VolgaCluster::launch(env, spec.launch).await?;
    let result = run_scenario(&cluster, env, scenario, duration).await;
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
) -> Result<()> {
    let timeouts = RecoveryTimeouts::for_env(env);
    let mut cursor = 0u64;
    cluster.start_execution().await?;
    let started = Instant::now();

    wait_until_attempt0_running(&cluster.master(), &mut cursor, timeouts.attempt_running).await?;
    println!("[volga-longrun] attempt 0 running");

    match scenario {
        SoakScenario::Steady => {}
        SoakScenario::KillAfterCheckpoint { after_checkpoint } => {
            run_kill_after_checkpoint(cluster, &timeouts, &mut cursor, after_checkpoint).await?;
        }
    }

    let remaining = duration.saturating_sub(started.elapsed());
    if remaining > Duration::ZERO {
        println!("[volga-longrun] running for remaining {remaining:?}");
        tokio::select! {
            _ = tokio::time::sleep(remaining) => {}
            _ = shutdown_signal() => {
                println!("[volga-longrun] interrupt, shutting down");
            }
        }
    }

    println!(
        "[volga-longrun] soak finished elapsed={:?}",
        started.elapsed()
    );
    Ok(())
}

async fn run_kill_after_checkpoint(
    cluster: &VolgaCluster,
    timeouts: &RecoveryTimeouts,
    cursor: &mut u64,
    after_checkpoint: u64,
) -> Result<()> {
    if after_checkpoint < 1 {
        return Err(anyhow!("kill-after-checkpoint must be >= 1"));
    }

    let mut checkpoint_id = 0u64;
    for n in 1..=after_checkpoint {
        checkpoint_id =
            wait_for_checkpoint_completed(&cluster.master(), cursor, CHECKPOINT_WAIT).await?;
        println!("[volga-longrun] checkpoint {checkpoint_id} completed ({n}/{after_checkpoint})");
    }

    wait_until_checkpoints_idle(&cluster.master(), CHECKPOINT_IDLE).await?;

    let worker_ids = cluster.worker_ids();
    let target = worker_ids
        .first()
        .ok_or_else(|| anyhow!("expected at least one worker"))?;
    println!("[volga-longrun] kill worker={target} after checkpoint>={checkpoint_id}");
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
    println!("[volga-longrun] restore attempt={attempt_id} running");
    Ok(())
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
