//! Interval checkpoint barrier propagation path.

use anyhow::{anyhow, Result};

use crate::runtime::master::{
    CheckpointPropagationPhase, LifecycleEvent, LifecycleEventRecord,
};
use crate::runtime::tests::cluster_harness::{
    PipelineLaunchSpec, RecoveryReport, RuntimeEnv, TestCluster,
};
use crate::runtime::tests::recovery_tests::RecoveryTimeouts;

use super::support::{
    harness_finish_pipeline, wait_for_checkpoint_completed, wait_until_attempt0_running,
};

/// Wait for an interval checkpoint and verify barrier propagation events.
pub async fn run_checkpoint_barrier_path(
    env: RuntimeEnv,
    launch: PipelineLaunchSpec,
) -> Result<u64> {
    let timeouts = RecoveryTimeouts::for_env(env);
    let cluster = TestCluster::launch(env, launch).await?;
    let mut cursor = 0;

    cluster.start_execution().await?;
    wait_until_attempt0_running(&cluster.master(), &mut cursor, timeouts.attempt_running).await?;

    let checkpoint_id = wait_for_checkpoint_completed(
        &cluster.master(),
        &mut cursor,
        timeouts.attempt_running,
    )
    .await?;

    let events = cluster.master().lifecycle_events_since(0).await?;
    assert_checkpoint_barrier_path(&events, checkpoint_id)?;

    harness_finish_pipeline(&cluster, &mut cursor, env, 0).await?;

    let events = cluster.master().lifecycle_events_since(0).await?;
    RecoveryReport::from_events(&events).print();
    Ok(checkpoint_id)
}

/// Assert barrier path for one checkpoint.
///
/// Required: `CheckpointStarted`, ≥1 `BarrierInjected` and ≥1 `Aligned` before
/// `CheckpointCompleted` (completion waits for full align).
pub fn assert_checkpoint_barrier_path(
    events: &[LifecycleEventRecord],
    checkpoint_id: u64,
) -> Result<()> {
    let mut saw_started = false;
    let mut injected = 0usize;
    let mut aligned = 0usize;
    let mut completed = false;
    let mut injected_before_completed = 0usize;
    let mut aligned_before_completed = 0usize;

    for record in events {
        match &record.event {
            LifecycleEvent::CheckpointStarted {
                checkpoint_id: id, ..
            } if *id == checkpoint_id => {
                saw_started = true;
            }
            LifecycleEvent::CheckpointPropagation {
                checkpoint_id: id,
                phase,
                ..
            } if *id == checkpoint_id => {
                if !saw_started {
                    return Err(anyhow!(
                        "checkpoint {checkpoint_id} propagation before CheckpointStarted"
                    ));
                }
                if completed {
                    return Err(anyhow!(
                        "checkpoint {checkpoint_id} propagation after CheckpointCompleted"
                    ));
                }
                match phase {
                    CheckpointPropagationPhase::BarrierInjected => {
                        injected += 1;
                        injected_before_completed += 1;
                    }
                    CheckpointPropagationPhase::Aligned => {
                        aligned += 1;
                        aligned_before_completed += 1;
                    }
                }
            }
            LifecycleEvent::CheckpointCompleted {
                checkpoint_id: id,
            } if *id == checkpoint_id => {
                completed = true;
            }
            _ => {}
        }
    }

    if !saw_started {
        return Err(anyhow!(
            "checkpoint {checkpoint_id} missing CheckpointStarted"
        ));
    }
    if !completed {
        return Err(anyhow!(
            "checkpoint {checkpoint_id} missing CheckpointCompleted (injected={injected} aligned={aligned})"
        ));
    }
    if injected_before_completed == 0 {
        return Err(anyhow!(
            "checkpoint {checkpoint_id} completed with no BarrierInjected beforehand"
        ));
    }
    if aligned_before_completed == 0 {
        return Err(anyhow!(
            "checkpoint {checkpoint_id} completed with no Aligned beforehand (injected={injected})"
        ));
    }
    println!(
        "[TEST] barrier path ok checkpoint_id={checkpoint_id} injected={injected} aligned={aligned}"
    );
    Ok(())
}
