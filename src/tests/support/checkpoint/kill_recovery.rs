//! Kill after completed checkpoint(s), restore, sink/offline check.

use anyhow::{anyhow, Result};
use std::time::{Duration, Instant};

use crate::runtime::master::LifecycleEvent;
use crate::tests::support::cluster_harness::{
    LifecycleOracle, MasterHandle, PipelineLaunchSpec, RecoveryReport, RuntimeEnv, TestCluster,
    WorkerKillMode,
};
use crate::tests::support::recovery::RecoveryTimeouts;

use super::launch::CheckpointWorkload;
use super::sink_oracle::assert_sink_matches_offline_datagen;
use super::support::{
    advance_lifecycle_cursor, checkpoint_idle_timeout, harness_finish_pipeline, shutdown_after,
    wait_for_checkpoint_completed, wait_for_checkpoint_completed_id, wait_for_checkpoint_started,
    wait_until_attempt0_running, wait_until_attempt_running, wait_until_checkpoints_idle,
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

/// After an intentional kill: restore AttemptStarted + kill-target replaced.
/// `ReplacementRequested` / `RecoveryStarted` can precede `AttemptStarted`, so both
/// conditions are tracked in one cursor scan.
async fn wait_for_kill_restore(
    master: &MasterHandle,
    cursor: &mut u64,
    timeout: Duration,
    target: &str,
    min_restore_checkpoint_id: u64,
    min_attempt_id: u64,
) -> Result<u64> {
    let started = Instant::now();
    let start_cursor = *cursor;
    let mut restore_attempt: Option<u64> = None;
    let mut target_replaced = false;
    let expecting = format!(
        "AttemptStarted restore>={min_restore_checkpoint_id} attempt>={min_attempt_id} \
         and replace containing {target}"
    );

    while !(restore_attempt.is_some() && target_replaced) {
        for record in master.lifecycle_events_since(*cursor).await? {
            *cursor = record.sequence;
            match &record.event {
                LifecycleEvent::RecoveryStarted {
                    replacement_worker_ids,
                    ..
                } if replacement_worker_ids.iter().any(|id| id == target) => {
                    target_replaced = true;
                }
                LifecycleEvent::ReplacementRequested { worker_ids }
                    if worker_ids.iter().any(|id| id == target) =>
                {
                    target_replaced = true;
                }
                LifecycleEvent::AttemptStarted {
                    attempt_id,
                    restore_checkpoint_id: Some(id),
                } if *id >= min_restore_checkpoint_id && *attempt_id >= min_attempt_id => {
                    restore_attempt = Some(*attempt_id);
                }
                _ => {}
            }
        }
        if let (Some(attempt_id), true) = (restore_attempt, target_replaced) {
            return Ok(attempt_id);
        }
        if started.elapsed() >= timeout {
            return Err(anyhow!(
                "{}",
                LifecycleOracle::timeout_diagnostics(
                    master,
                    &expecting,
                    start_cursor,
                    *cursor,
                    started.elapsed(),
                )
                .await
            ));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(anyhow!("unreachable: wait_for_kill_restore loop exited"))
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
    let idle_timeout = checkpoint_idle_timeout(env);
    let cluster = TestCluster::launch(env, launch).await?;
    let result = async {
        let worker_ids = cluster.worker_ids();
        if worker_ids.is_empty() {
            return Err(anyhow!("expected at least one worker"));
        }
        let expected_workers = worker_ids.len();
        let mut cursor = 0;
        let mut next_attempt: u64 = 1;
        let mut current_attempt: u64 = 0;

        cluster.start_execution().await?;
        wait_until_attempt0_running(&cluster.master(), &mut cursor, timeouts.attempt_running)
            .await?;

        for failure_idx in 0..failure_count {
            // Stabilize before waiting for the next kill CP: attempt running, no in-flight CP.
            if failure_idx > 0 {
                wait_until_attempt_running(
                    &cluster.master(),
                    &mut cursor,
                    timeouts.attempt1_running,
                    current_attempt,
                    expected_workers,
                )
                .await?;
            }
            wait_until_checkpoints_idle(&cluster.master(), idle_timeout).await?;

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

            // Another interval CP may start after the waited completion; idle again before kill.
            wait_until_checkpoints_idle(&cluster.master(), idle_timeout).await?;

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

            // Per-cycle signal: restore from a completed CP + kill target replaced.
            // Do not require AttemptRunning / sink growth here (non-transactional sink
            // often stays flat during replay); that runs once before finish.
            let attempt_id = wait_for_kill_restore(
                &cluster.master(),
                &mut cursor,
                timeouts.recovery_started + timeouts.replacement + timeouts.attempt1_running,
                target,
                checkpoint_id,
                next_attempt,
            )
            .await?;
            next_attempt = attempt_id + 1;
            current_attempt = attempt_id;
        }

        wait_until_attempt_running(
            &cluster.master(),
            &mut cursor,
            timeouts.attempt1_running,
            current_attempt,
            expected_workers,
        )
        .await?;

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

fn parse_restore_checkpoint_id(trigger: &str) -> Option<u64> {
    let marker = "restore=Some(";
    let start = trigger.find(marker)? + marker.len();
    let end = start + trigger[start..].find(')')?;
    trigger[start..end].parse().ok()
}

pub fn assert_checkpoint_restore(
    report: &RecoveryReport,
    min_checkpoint_id: u64,
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
    match parse_restore_checkpoint_id(&attempt1.trigger) {
        Some(id) if id >= min_checkpoint_id => Ok(()),
        _ => Err(anyhow!(
            "attempt 1 trigger missing restore=Some(id>={min_checkpoint_id}): {}",
            attempt1.trigger
        )),
    }
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
