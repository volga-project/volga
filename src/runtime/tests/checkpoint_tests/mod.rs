//! Checkpoint + fail + restore e2e (local + kube), same shape as recovery_tests.

pub mod kube;
pub mod local;

use anyhow::{anyhow, Result};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_integration_test::schema_to_json;
use datafusion::common::ScalarValue;
use std::collections::HashMap;
use std::time::Duration;

use crate::api::spec::connectors::{SourceSpec, SourceSpecKind};
use crate::api::spec::pipeline::ExecutionProfile;
use crate::api::{PipelineSpecBuilder, TaskWorkerAssignmentStrategyType};
use crate::runtime::functions::source::datagen_source::{DatagenSpec, FieldGenerator};
use crate::runtime::master::{
    CheckpointPropagationPhase, LifecycleEvent, LifecycleEventRecord,
};
use crate::runtime::tests::cluster_harness::{
    LifecycleOracle, PipelineLaunchSpec, RecoveryReport, RuntimeEnv, TestCluster, WorkerKillMode,
};
use crate::runtime::tests::recovery_tests::RecoveryTimeouts;

pub fn checkpoint_recovery_launch_spec() -> PipelineLaunchSpec {
    let parallelism = 2;
    let schema = Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]);
    let mut fields = HashMap::new();
    fields.insert(
        "timestamp".to_string(),
        FieldGenerator::IncrementalTimestamp {
            start_ms: 1_000,
            step_ms: 1,
        },
    );
    fields.insert(
        "key".to_string(),
        FieldGenerator::Key {
            num_unique: parallelism * 8,
        },
    );
    fields.insert(
        "value".to_string(),
        FieldGenerator::Values {
            values: vec![ScalarValue::Float64(Some(1.0)), ScalarValue::Float64(Some(2.0))],
        },
    );

    let pipeline = PipelineSpecBuilder::new()
        .with_parallelism(parallelism)
        .with_execution_profile(ExecutionProfile::MasterWorker {
            num_threads_per_task: 2,
        })
        .with_task_assignment_strategy(TaskWorkerAssignmentStrategyType::Pipelined {
            slots_per_node: 2,
        })
        .with_source(SourceSpec::new(
            "datagen_source",
            SourceSpecKind::Datagen(DatagenSpec {
                rate: Some(200.0),
                limit: None,
                run_for_s: None,
                batch_size: 20,
                fields,
                replayable: true,
            }),
            schema_to_json(&schema),
        ))
        .sql("SELECT timestamp, key, value FROM datagen_source")
        .build();

    PipelineLaunchSpec::new(pipeline, 1, 0).with_dedup_sink(true)
}

async fn wait_until_attempt0_running(
    master: &crate::runtime::tests::cluster_harness::MasterHandle,
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

/// Force a checkpoint, kill a worker, recover, stop sources, assert restore + output.
pub async fn run_checkpoint_worker_kill_recovery(
    env: RuntimeEnv,
    launch: PipelineLaunchSpec,
    mode: WorkerKillMode,
) -> Result<RecoveryReport> {
    let timeouts = RecoveryTimeouts::for_env(env);
    let cluster = TestCluster::launch(env, launch).await?;
    let target = cluster
        .worker_ids()
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("expected at least one worker"))?;
    let mut cursor = 0;

    cluster.start_execution().await?;
    wait_until_attempt0_running(&cluster.master(), &mut cursor, timeouts.attempt_running).await?;

    // Let the source produce some state before checkpointing.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let checkpoint_id = cluster.master().trigger_checkpoint().await?;
    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        timeouts.attempt_running,
        |event| {
            matches!(
                event,
                LifecycleEvent::CheckpointCompleted { checkpoint_id: id } if *id == checkpoint_id
            )
        },
    )
    .await?;

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
                LifecycleEvent::AttemptStarted {
                    attempt_id: 1,
                    restore_checkpoint_id: Some(id),
                } if *id == checkpoint_id
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

    // Produce a bit more after restore, then stop and drain.
    tokio::time::sleep(Duration::from_millis(500)).await;
    cluster.master().stop_sources().await?;
    // Allow in-flight batches to drain to the dedup sink before sampling stats.
    tokio::time::sleep(Duration::from_millis(750)).await;
    let (_tasks, total_generated) = cluster.master().get_source_stats().await?;

    let finish_timeout = match env {
        RuntimeEnv::Local => Duration::from_secs(30),
        RuntimeEnv::Kube | RuntimeEnv::Docker => Duration::from_secs(180),
    };
    LifecycleOracle::wait_for(&cluster.master(), &mut cursor, finish_timeout, |event| {
        matches!(event, LifecycleEvent::PipelineFinished)
    })
    .await?;

    let snapshot = cluster.storage().snapshot().await?;
    let dedup_rows = snapshot.dedup_row_count();
    if dedup_rows == 0 {
        return Err(anyhow!("expected dedup sink rows after checkpoint recovery"));
    }
    if dedup_rows as u64 != total_generated {
        return Err(anyhow!(
            "dedup sink rows {dedup_rows} != source stats total {total_generated}"
        ));
    }

    let events = cluster.master().lifecycle_events_since(0).await?;
    let report = RecoveryReport::from_events(&events);
    report.print();
    Ok(report)
}

pub fn assert_checkpoint_restore(report: &RecoveryReport, expected_checkpoint_id: u64) -> Result<()> {
    report.assert_attempt_count(2)?;
    let attempt1 = report.attempt(1)?;
    if !attempt1.trigger.contains(&format!("restore=Some({expected_checkpoint_id})")) {
        return Err(anyhow!(
            "attempt 1 trigger missing restore=Some({expected_checkpoint_id}): {}",
            attempt1.trigger
        ));
    }
    Ok(())
}

/// Force a checkpoint and verify barrier propagation events precede completion.
pub async fn run_checkpoint_barrier_path(
    env: RuntimeEnv,
    launch: PipelineLaunchSpec,
) -> Result<u64> {
    let timeouts = RecoveryTimeouts::for_env(env);
    let cluster = TestCluster::launch(env, launch).await?;
    let mut cursor = 0;

    cluster.start_execution().await?;
    wait_until_attempt0_running(&cluster.master(), &mut cursor, timeouts.attempt_running).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let checkpoint_id = cluster.master().trigger_checkpoint().await?;
    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        timeouts.attempt_running,
        |event| {
            matches!(
                event,
                LifecycleEvent::CheckpointCompleted { checkpoint_id: id } if *id == checkpoint_id
            )
        },
    )
    .await?;

    let events = cluster.master().lifecycle_events_since(0).await?;
    assert_checkpoint_barrier_path(&events, checkpoint_id)?;

    cluster.master().stop_sources().await?;
    let finish_timeout = match env {
        RuntimeEnv::Local => Duration::from_secs(30),
        RuntimeEnv::Kube | RuntimeEnv::Docker => Duration::from_secs(180),
    };
    LifecycleOracle::wait_for(&cluster.master(), &mut cursor, finish_timeout, |event| {
        matches!(event, LifecycleEvent::PipelineFinished)
    })
    .await?;
    Ok(checkpoint_id)
}

/// Assert barrier path ordering for one checkpoint:
/// `CheckpointStarted` → ≥1 `BarrierInjected` → ≥1 `Aligned` → `CheckpointCompleted`.
pub fn assert_checkpoint_barrier_path(
    events: &[LifecycleEventRecord],
    checkpoint_id: u64,
) -> Result<()> {
    let mut saw_started = false;
    let mut injected = 0usize;
    let mut aligned = 0usize;
    let mut completed = false;

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
                    CheckpointPropagationPhase::BarrierInjected => injected += 1,
                    CheckpointPropagationPhase::Aligned => aligned += 1,
                }
            }
            LifecycleEvent::CheckpointCompleted {
                checkpoint_id: id,
            } if *id == checkpoint_id => {
                if injected == 0 {
                    return Err(anyhow!(
                        "checkpoint {checkpoint_id} completed with no BarrierInjected"
                    ));
                }
                if aligned == 0 {
                    return Err(anyhow!(
                        "checkpoint {checkpoint_id} completed with no Aligned"
                    ));
                }
                completed = true;
            }
            _ => {}
        }
    }

    if !completed {
        return Err(anyhow!(
            "checkpoint {checkpoint_id} missing CheckpointCompleted (injected={injected} aligned={aligned})"
        ));
    }
    println!(
        "[TEST] barrier path ok checkpoint_id={checkpoint_id} injected={injected} aligned={aligned}"
    );
    Ok(())
}
