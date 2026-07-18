//! Shared waits / harness finish for checkpoint e2e runners.

use anyhow::{anyhow, Result};
use std::time::Duration;

use crate::runtime::master::LifecycleEvent;
use crate::runtime::tests::cluster_harness::{LifecycleOracle, MasterHandle, RuntimeEnv, TestCluster};

pub(super) async fn wait_until_attempt0_running(
    master: &MasterHandle,
    cursor: &mut u64,
    timeout: Duration,
) -> Result<()> {
    LifecycleOracle::wait_for(master, cursor, timeout, |event| {
        matches!(
            event,
            LifecycleEvent::AttemptRunning { attempt_id: 0, .. }
        )
    })
    .await?;
    Ok(())
}

pub(super) async fn wait_for_checkpoint_completed(
    master: &MasterHandle,
    cursor: &mut u64,
    timeout: Duration,
) -> Result<u64> {
    let record = LifecycleOracle::wait_for(master, cursor, timeout, |event| {
        matches!(event, LifecycleEvent::CheckpointCompleted { .. })
    })
    .await?;
    match record.event {
        LifecycleEvent::CheckpointCompleted { checkpoint_id } => Ok(checkpoint_id),
        other => Err(anyhow!("expected CheckpointCompleted, got {other:?}")),
    }
}

pub(super) async fn wait_for_checkpoint_started(
    master: &MasterHandle,
    cursor: &mut u64,
    timeout: Duration,
    min_checkpoint_id: u64,
) -> Result<u64> {
    let record = LifecycleOracle::wait_for(master, cursor, timeout, |event| {
        matches!(
            event,
            LifecycleEvent::CheckpointStarted {
                checkpoint_id,
                ..
            } if *checkpoint_id >= min_checkpoint_id
        )
    })
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

/// After the scenario is done: stop sources, then wait for `PipelineFinished`.
pub(super) async fn harness_finish_pipeline(
    cluster: &TestCluster,
    cursor: &mut u64,
    env: RuntimeEnv,
    failure_count: usize,
) -> Result<()> {
    cluster.master().stop_sources().await?;
    LifecycleOracle::wait_for(
        &cluster.master(),
        cursor,
        finish_timeout(env, failure_count),
        |event| matches!(event, LifecycleEvent::PipelineFinished),
    )
    .await?;
    Ok(())
}

/// Always tear down cluster resources (kube `VolgaPipeline` delete, local workers, etc.).
pub(super) async fn shutdown_after<T>(cluster: &TestCluster, result: Result<T>) -> Result<T> {
    match cluster.shutdown().await {
        Ok(()) => result,
        Err(shutdown_err) => match result {
            Ok(_) => Err(shutdown_err),
            Err(test_err) => Err(test_err),
        },
    }
}
