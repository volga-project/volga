//! Kill after completed checkpoint(s), restore, sink/offline check.

use anyhow::{anyhow, Result};

use crate::runtime::master::LifecycleEvent;
use crate::e2e::support::cluster_harness::{
    LifecycleOracle, PipelineLaunchSpec, RecoveryReport, RuntimeEnv, TestCluster, WorkerKillMode,
};
use crate::e2e::support::recovery::RecoveryTimeouts;

use super::launch::CheckpointWorkload;
use super::sink_oracle::assert_sink_matches_offline_datagen;
use super::support::{
    advance_lifecycle_cursor, harness_finish_pipeline, shutdown_after,
    wait_for_checkpoint_completed, wait_for_checkpoint_completed_id, wait_for_checkpoint_started,
    wait_until_attempt0_running,
};

async fn wait_for_sink_progress(
    cluster: &TestCluster,
    after_rows: usize,
    timeout: std::time::Duration,
) -> Result<usize> {
    let started = tokio::time::Instant::now();
    loop {
        let rows = cluster.storage().snapshot().await?.row_count();
        if rows > after_rows {
            return Ok(rows);
        }
        if started.elapsed() >= timeout {
            return Err(anyhow!(
                "timed out waiting for sink rows to advance beyond {after_rows}"
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// One kill after the first completed checkpoint, then finish + sink check.
pub async fn run_checkpoint_worker_kill_recovery(
    env: RuntimeEnv,
    launch: PipelineLaunchSpec,
    mode: WorkerKillMode,
    workload: CheckpointWorkload,
) -> Result<RecoveryReport> {
    run_checkpoint_sequential_failures(env, launch, mode, 1, workload).await
}

/// After each completed checkpoint, kill one worker; repeat `failure_count` times;
/// then finish and check sink/offline equality.
pub async fn run_checkpoint_sequential_failures(
    env: RuntimeEnv,
    launch: PipelineLaunchSpec,
    mode: WorkerKillMode,
    failure_count: usize,
    workload: CheckpointWorkload,
) -> Result<RecoveryReport> {
    if failure_count == 0 {
        return Err(anyhow!("failure_count must be >= 1"));
    }
    let timeouts = RecoveryTimeouts::for_env(env);
    let cluster = TestCluster::launch(env, launch).await?;
    let result = async {
        let worker_ids = cluster.worker_ids();
        if worker_ids.is_empty() {
            return Err(anyhow!("expected at least one worker"));
        }
        let mut cursor = 0;
        let mut next_attempt: u64 = 1;

        cluster.start_execution().await?;
        wait_until_attempt0_running(&cluster.master(), &mut cursor, timeouts.attempt_running)
            .await?;

        for failure_idx in 0..failure_count {
            let checkpoint_id = match workload {
                CheckpointWorkload::PassThrough => {
                    wait_for_checkpoint_completed(
                        &cluster.master(),
                        &mut cursor,
                        timeouts.attempt_running,
                    )
                    .await?
                }
                CheckpointWorkload::Window => {
                    wait_for_sink_progress(&cluster, 0, timeouts.attempt_running).await?;
                    advance_lifecycle_cursor(&cluster.master(), &mut cursor).await?;
                    let checkpoint_id = wait_for_checkpoint_started(
                        &cluster.master(),
                        &mut cursor,
                        timeouts.attempt_running,
                        1,
                    )
                    .await?;
                    wait_for_checkpoint_completed_id(
                        &cluster.master(),
                        &mut cursor,
                        timeouts.attempt_running,
                        checkpoint_id,
                    )
                    .await?;
                    let checkpoint_rows = cluster.storage().snapshot().await?.row_count();
                    wait_for_sink_progress(
                        &cluster,
                        checkpoint_rows,
                        timeouts.attempt_running,
                    )
                    .await?;
                    checkpoint_id
                }
            };

            let target = &worker_ids[failure_idx % worker_ids.len()];
            println!(
                "[TEST] sequential-failure {}/{} kill worker={target} after checkpoint>={checkpoint_id}",
                failure_idx + 1,
                failure_count
            );
            cluster
                .worker(target)
                .ok_or_else(|| anyhow!("missing worker {target}"))?
                .kill_with(mode)
                .await?;

            // Restore id may be > waited id if another CP completed before the kill landed.
            let started = LifecycleOracle::wait_for(
                &cluster.master(),
                &mut cursor,
                timeouts.recovery_started + timeouts.replacement + timeouts.attempt1_running,
                |event| {
                    matches!(
                        event,
                        LifecycleEvent::AttemptStarted {
                            restore_checkpoint_id: Some(id),
                            ..
                        } if *id >= checkpoint_id
                    )
                },
            )
            .await?;
            let attempt_id = match started.event {
                LifecycleEvent::AttemptStarted { attempt_id, .. } => attempt_id,
                other => {
                    return Err(anyhow!(
                        "expected AttemptStarted after kill, got {other:?}"
                    ))
                }
            };
            if attempt_id < next_attempt {
                return Err(anyhow!(
                    "expected attempt_id >= {next_attempt} after failure {failure_idx}, got {attempt_id}"
                ));
            }
            next_attempt = attempt_id + 1;

            LifecycleOracle::wait_for(
                &cluster.master(),
                &mut cursor,
                timeouts.attempt1_running,
                |event| {
                    matches!(
                        event,
                        LifecycleEvent::AttemptRunning {
                            attempt_id: id,
                            ..
                        } if *id == attempt_id
                    )
                },
            )
            .await?;

            // Cardinality can remain flat while restored sources replay rows already
            // present in the non-transactional sink. Wait until replay catches up and
            // the recovered attempt produces at least one new output identity.
            let recovered_rows = cluster.storage().snapshot().await?.row_count();
            wait_for_sink_progress(&cluster, recovered_rows, timeouts.attempt1_running).await?;
        }

        harness_finish_pipeline(&cluster, &mut cursor, env, failure_count).await?;

        let snapshot = cluster.storage().snapshot().await?;
        assert_sink_matches_offline_datagen(&cluster.master(), &snapshot, env, workload).await?;

        let events = cluster.master().lifecycle_events_since(0).await?;
        let report = RecoveryReport::from_events(&events);
        report.print();
        Ok(report)
    }
    .await;
    shutdown_after(&cluster, result).await
}

pub fn assert_checkpoint_restore(
    report: &RecoveryReport,
    expected_checkpoint_id: u64,
    expected_workers: usize,
) -> Result<()> {
    // Extra recoveries can follow (e.g. later CP timeout); require restore on attempt 1.
    if report.attempts.len() < 2 {
        return Err(anyhow!(
            "expected at least 2 attempts after checkpoint restore, got {}",
            report.attempts.len()
        ));
    }
    let attempt0 = report.attempt(0)?;
    if attempt0.workers.len() != expected_workers {
        return Err(anyhow!(
            "expected {expected_workers} workers on attempt 0, got {:?}",
            attempt0.workers
        ));
    }
    let attempt1 = report.attempt(1)?;
    if !attempt1.trigger.contains(&format!("restore=Some({expected_checkpoint_id})")) {
        return Err(anyhow!(
            "attempt 1 trigger missing restore=Some({expected_checkpoint_id}): {}",
            attempt1.trigger
        ));
    }
    Ok(())
}

/// At least `min_failures` attempts restored from a checkpoint; multi-worker on attempt 0.
pub fn assert_checkpoint_multi_restore(
    report: &RecoveryReport,
    min_failures: usize,
    expected_workers: usize,
) -> Result<()> {
    let attempt0 = report.attempt(0)?;
    if attempt0.workers.len() != expected_workers {
        return Err(anyhow!(
            "expected {expected_workers} workers on attempt 0, got {:?}",
            attempt0.workers
        ));
    }
    let restore_attempts = report
        .attempts
        .values()
        .filter(|a| a.trigger.contains("restore=Some("))
        .count();
    if restore_attempts < min_failures {
        return Err(anyhow!(
            "expected at least {min_failures} restore attempts, got {restore_attempts} (attempts={})",
            report.attempts.len()
        ));
    }
    Ok(())
}
