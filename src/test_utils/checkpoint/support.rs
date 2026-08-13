//! Shared waits / harness finish for checkpoint suite runners.

use anyhow::{anyhow, Result};
use std::collections::HashSet;
use std::time::{Duration, Instant};

use crate::runtime::master::LifecycleEvent;
use crate::test_utils::harness::{LifecycleOracle, MasterHandle, RuntimeEnv, VolgaCluster};

pub async fn wait_until_attempt0_running(
    master: &MasterHandle,
    cursor: &mut u64,
    timeout: Duration,
) -> Result<()> {
    LifecycleOracle::wait_for(
        master,
        cursor,
        timeout,
        "AttemptRunning attempt=0",
        |event| matches!(event, LifecycleEvent::AttemptRunning { attempt_id: 0, .. }),
    )
    .await?;
    Ok(())
}

pub async fn wait_until_attempt_running(
    master: &MasterHandle,
    cursor: &mut u64,
    timeout: Duration,
    attempt_id: u64,
    expected_workers: usize,
) -> Result<()> {
    LifecycleOracle::wait_for(
        master,
        cursor,
        timeout,
        &format!("AttemptRunning attempt={attempt_id} with {expected_workers} workers"),
        |event| {
            matches!(
                event,
                LifecycleEvent::AttemptRunning {
                    attempt_id: id,
                    worker_ids,
                } if *id == attempt_id && worker_ids.len() == expected_workers
            )
        },
    )
    .await?;
    Ok(())
}

/// Wait until every started checkpoint has completed or failed (no in-flight CP).
///
/// The timeout budget resets whenever the in-flight set changes so a chain of
/// interval checkpoints (2 → 3 → 4) each get a full settle window rather than
/// sharing one deadline from the first open CP.
pub async fn wait_until_checkpoints_idle(master: &MasterHandle, timeout: Duration) -> Result<()> {
    let mut started = Instant::now();
    let mut last_open = HashSet::new();
    loop {
        let mut open = HashSet::new();
        for record in master.lifecycle_events_since(0).await? {
            match record.event {
                LifecycleEvent::CheckpointStarted { checkpoint_id, .. } => {
                    open.insert(checkpoint_id);
                }
                LifecycleEvent::CheckpointCompleted { checkpoint_id }
                | LifecycleEvent::CheckpointFailed { checkpoint_id, .. } => {
                    open.remove(&checkpoint_id);
                }
                _ => {}
            }
        }
        if open.is_empty() {
            return Ok(());
        }
        if open != last_open {
            started = Instant::now();
            last_open = open.clone();
        }
        if started.elapsed() >= timeout {
            return Err(anyhow!(
                "timed out waiting for in-flight checkpoint(s) {open:?} to settle"
            ));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub(super) fn checkpoint_idle_timeout(env: RuntimeEnv) -> Duration {
    match env {
        RuntimeEnv::Local => Duration::from_secs(4),
        RuntimeEnv::Kube | RuntimeEnv::Docker => Duration::from_secs(45),
    }
}

pub async fn wait_for_checkpoint_completed(
    master: &MasterHandle,
    cursor: &mut u64,
    timeout: Duration,
) -> Result<u64> {
    let record = LifecycleOracle::wait_for(
        master,
        cursor,
        timeout,
        "CheckpointCompleted (any)",
        |event| matches!(event, LifecycleEvent::CheckpointCompleted { .. }),
    )
    .await?;
    match record.event {
        LifecycleEvent::CheckpointCompleted { checkpoint_id } => Ok(checkpoint_id),
        other => Err(anyhow!("expected CheckpointCompleted, got {other:?}")),
    }
}

pub async fn wait_for_checkpoint_completed_id(
    master: &MasterHandle,
    cursor: &mut u64,
    timeout: Duration,
    checkpoint_id: u64,
) -> Result<()> {
    LifecycleOracle::wait_for(
        master,
        cursor,
        timeout,
        &format!("CheckpointCompleted id={checkpoint_id}"),
        |event| {
            matches!(
                event,
                LifecycleEvent::CheckpointCompleted {
                    checkpoint_id: id
                } if *id == checkpoint_id
            )
        },
    )
    .await?;
    Ok(())
}

pub(super) async fn advance_lifecycle_cursor(
    master: &MasterHandle,
    cursor: &mut u64,
) -> Result<()> {
    for record in master.lifecycle_events_since(*cursor).await? {
        *cursor = record.sequence;
    }
    Ok(())
}

pub async fn wait_for_checkpoint_started(
    master: &MasterHandle,
    cursor: &mut u64,
    timeout: Duration,
    min_checkpoint_id: u64,
) -> Result<u64> {
    let record = LifecycleOracle::wait_for(
        master,
        cursor,
        timeout,
        &format!("CheckpointStarted id>={min_checkpoint_id}"),
        |event| {
            matches!(
                event,
                LifecycleEvent::CheckpointStarted {
                    checkpoint_id,
                    ..
                } if *checkpoint_id >= min_checkpoint_id
            )
        },
    )
    .await?;
    match record.event {
        LifecycleEvent::CheckpointStarted { checkpoint_id, .. } => Ok(checkpoint_id),
        other => Err(anyhow!("expected CheckpointStarted, got {other:?}")),
    }
}

fn finish_timeout(env: RuntimeEnv, failure_count: usize) -> Duration {
    let base = match env {
        RuntimeEnv::Local => Duration::from_secs(30),
        RuntimeEnv::Kube | RuntimeEnv::Docker => Duration::from_secs(180),
    };
    // Drain after harness StopSources; slack grows with prior recoveries.
    base + Duration::from_secs(20 * failure_count as u64)
}

/// Stop sources (after any in-flight CP settles), then wait for `PipelineFinished`.
pub(super) async fn harness_finish_pipeline(
    cluster: &VolgaCluster,
    cursor: &mut u64,
    env: RuntimeEnv,
    failure_count: usize,
) -> Result<()> {
    // Avoid StopSources during an in-flight CP (timeout → empty-replace recovery).
    wait_until_checkpoints_idle(&cluster.master(), checkpoint_idle_timeout(env)).await?;

    // Sets lifecycle intent=Finish; drain runs now or after the next stable attempt.
    cluster.master().stop_sources().await?;
    LifecycleOracle::wait_for(
        &cluster.master(),
        cursor,
        finish_timeout(env, failure_count),
        "PipelineFinished",
        |event| matches!(event, LifecycleEvent::PipelineFinished),
    )
    .await?;
    Ok(())
}

/// Always tear down cluster resources (kube `VolgaPipeline` delete, local workers, etc.).
pub(super) async fn shutdown_after<T>(cluster: &VolgaCluster, result: Result<T>) -> Result<T> {
    match cluster.shutdown().await {
        Ok(()) => result,
        Err(shutdown_err) => match result {
            Ok(_) => Err(shutdown_err),
            Err(test_err) => Err(test_err),
        },
    }
}
