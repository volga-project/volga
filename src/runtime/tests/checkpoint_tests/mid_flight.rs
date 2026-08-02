//! Kill while a checkpoint is in-flight (before it can complete).

use anyhow::{anyhow, Result};

use crate::runtime::master::LifecycleEvent;
use crate::runtime::tests::cluster_harness::{
    LifecycleOracle, PipelineLaunchSpec, RecoveryReport, RuntimeEnv, TestCluster, WorkerKillMode,
};
use crate::runtime::tests::recovery_tests::RecoveryTimeouts;

use super::sink_oracle::assert_sink_matches_offline_datagen;
use super::support::{
    harness_finish_pipeline, shutdown_after, wait_for_checkpoint_completed,
    wait_for_checkpoint_started, wait_until_attempt0_running,
};
use super::CheckpointWorkload;

fn report_has_checkpoint_failed(report: &RecoveryReport, checkpoint_id: u64) -> bool {
    let needle = format!("checkpoint_failed {checkpoint_id}");
    report
        .attempts
        .values()
        .any(|a| a.events.iter().any(|e| e.contains(&needle)))
}

fn report_has_checkpoint_completed(report: &RecoveryReport, checkpoint_id: u64) -> bool {
    let needle = format!("checkpoint_completed {checkpoint_id}");
    report
        .attempts
        .values()
        .any(|attempt| attempt.events.iter().any(|event| event.contains(&needle)))
}

/// Kill while the first checkpoint is in-flight (no prior complete). No sink EO check.
pub async fn run_checkpoint_mid_flight_kill_no_prior(
    env: RuntimeEnv,
    launch: PipelineLaunchSpec,
    mode: WorkerKillMode,
) -> Result<RecoveryReport> {
    let timeouts = RecoveryTimeouts::for_env(env);
    let cluster = TestCluster::launch(env, launch).await?;
    let result = async {
        let target = cluster
            .worker_ids()
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("expected at least one worker"))?;
        let mut cursor = 0;

        cluster.start_execution().await?;
        wait_until_attempt0_running(&cluster.master(), &mut cursor, timeouts.attempt_running)
            .await?;

        // Kill on CheckpointStarted (in-flight). Waiting for BarrierInjected races completion:
        // with flush-on-barrier, CP1 often completes before kill → restore=Some(1).
        let in_flight_id = wait_for_checkpoint_started(
            &cluster.master(),
            &mut cursor,
            timeouts.attempt_running,
            1,
        )
        .await?;

        println!("[TEST] mid-flight kill (no prior) worker={target} in_flight={in_flight_id}");
        cluster
            .worker(&target)
            .ok_or_else(|| anyhow!("missing worker {target}"))?
            .kill_with(mode)
            .await?;

        LifecycleOracle::wait_for(
            &cluster.master(),
            &mut cursor,
            timeouts.recovery_started + timeouts.replacement + timeouts.attempt1_running,
            |event| {
                matches!(
                    event,
                    LifecycleEvent::AttemptStarted { attempt_id: 1, .. }
                )
            },
        )
        .await?;
        LifecycleOracle::wait_for(
            &cluster.master(),
            &mut cursor,
            timeouts.attempt1_running,
            |event| {
                matches!(
                    event,
                    LifecycleEvent::AttemptRunning { attempt_id: 1, .. }
                )
            },
        )
        .await?;

        harness_finish_pipeline(&cluster, &mut cursor, env, 1).await?;

        let events = cluster.master().lifecycle_events_since(0).await?;
        let report = RecoveryReport::from_events(&events);
        report.print();
        assert_mid_flight_restore(&report, None, in_flight_id)?;
        Ok(report)
    }
    .await;
    shutdown_after(&cluster, result).await
}

/// Complete CP1, kill during in-flight CP2, restore from CP1, sink/offline check.
pub async fn run_checkpoint_mid_flight_kill_after_safe(
    env: RuntimeEnv,
    launch: PipelineLaunchSpec,
    mode: WorkerKillMode,
) -> Result<RecoveryReport> {
    let timeouts = RecoveryTimeouts::for_env(env);
    let cluster = TestCluster::launch(env, launch).await?;
    let result = async {
        let target = cluster
            .worker_ids()
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("expected at least one worker"))?;
        let mut cursor = 0;

        cluster.start_execution().await?;
        wait_until_attempt0_running(&cluster.master(), &mut cursor, timeouts.attempt_running)
            .await?;

        let safe_id = wait_for_checkpoint_completed(
            &cluster.master(),
            &mut cursor,
            timeouts.attempt_running,
        )
        .await?;

        // Kill on CheckpointStarted (still a race on kube: pod may finish the CP
        // before force-delete / health poll lands — fail fast below if restore advances).
        let in_flight_id = wait_for_checkpoint_started(
            &cluster.master(),
            &mut cursor,
            timeouts.attempt_running,
            safe_id + 1,
        )
        .await?;

        println!(
            "[TEST] mid-flight kill (after safe={safe_id}) worker={target} in_flight={in_flight_id}"
        );
        cluster
            .worker(&target)
            .ok_or_else(|| anyhow!("missing worker {target}"))?
            .kill_with(mode)
            .await?;

        LifecycleOracle::wait_for(
            &cluster.master(),
            &mut cursor,
            timeouts.recovery_started + timeouts.replacement + timeouts.attempt1_running,
            |event| {
                matches!(
                    event,
                    LifecycleEvent::AttemptStarted { attempt_id: 1, .. }
                )
            },
        )
        .await?;
        LifecycleOracle::wait_for(
            &cluster.master(),
            &mut cursor,
            timeouts.attempt1_running,
            |event| {
                matches!(
                    event,
                    LifecycleEvent::AttemptRunning { attempt_id: 1, .. }
                )
            },
        )
        .await?;

        harness_finish_pipeline(&cluster, &mut cursor, env, 1).await?;

        let snapshot = cluster.storage().snapshot().await?;
        assert_sink_matches_offline_datagen(
            &cluster.master(),
            &snapshot,
            env,
            CheckpointWorkload::PassThrough,
        )
        .await?;

        let events = cluster.master().lifecycle_events_since(0).await?;
        let report = RecoveryReport::from_events(&events);
        report.print();
        assert_mid_flight_restore(&report, Some(safe_id), in_flight_id)?;
        Ok(report)
    }
    .await;
    shutdown_after(&cluster, result).await
}

/// Restore None/prior if in-flight failed, or Some(in_flight) if it completed first.
fn assert_mid_flight_restore(
    report: &RecoveryReport,
    prior: Option<u64>,
    in_flight_id: u64,
) -> Result<()> {
    let trigger = &report.attempt(1)?.trigger;
    let completed = report_has_checkpoint_completed(report, in_flight_id);
    let ok = if completed {
        trigger.contains(&format!("restore=Some({in_flight_id})"))
    } else {
        report_has_checkpoint_failed(report, in_flight_id)
            && match prior {
                None => trigger.contains("restore=None"),
                Some(safe) => trigger.contains(&format!("restore=Some({safe})")),
            }
    };
    if ok {
        Ok(())
    } else {
        Err(anyhow!(
            "in-flight {in_flight_id} completed={completed}, attempt 1: {trigger}"
        ))
    }
}
