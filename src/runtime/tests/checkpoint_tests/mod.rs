//! Checkpoint + fail + restore e2e (local + kube), same shape as recovery_tests.

pub mod kube;
pub mod local;

use anyhow::{anyhow, Result};
use arrow::array::{Array, Float64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_integration_test::schema_to_json;
use datafusion::common::ScalarValue;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::api::spec::connectors::{SourceSpec, SourceSpecKind};
use crate::api::spec::pipeline::ExecutionProfile;
use crate::api::{PipelineSpecBuilder, TaskWorkerAssignmentStrategyType};
use crate::runtime::functions::source::datagen_source::{
    DatagenSourceConfig, DatagenSourceFunction, DatagenSpec, FieldGenerator,
};
use crate::runtime::master::{
    CheckpointPropagationPhase, LifecycleEvent, LifecycleEventRecord,
};
use crate::runtime::observability::{task_meta, PipelineSnapshot};
use crate::runtime::tests::cluster_harness::{
    LifecycleOracle, PipelineLaunchSpec, RecoveryReport, RuntimeEnv, TestCluster, WorkerKillMode,
};
use crate::runtime::tests::recovery_tests::RecoveryTimeouts;
use crate::storage::InMemoryStorageSnapshot;

const CHECKPOINT_PARALLELISM: i32 = 2;

fn checkpoint_datagen_parts() -> (Arc<Schema>, DatagenSpec) {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));
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
            num_unique: CHECKPOINT_PARALLELISM as usize * 8,
        },
    );
    fields.insert(
        "value".to_string(),
        FieldGenerator::Values {
            values: vec![ScalarValue::Float64(Some(1.0)), ScalarValue::Float64(Some(2.0))],
        },
    );
    (
        schema,
        DatagenSpec {
            rate: Some(200.0),
            // Finite run so PipelineFinished happens without a stop RPC.
            // Long enough for an interval CP + kill/restore before sources exhaust.
            limit: Some(800),
            run_for_s: None,
            batch_size: 20,
            fields,
            replayable: true,
        },
    )
}

pub fn checkpoint_recovery_launch_spec() -> PipelineLaunchSpec {
    let (schema, datagen) = checkpoint_datagen_parts();
    let pipeline = PipelineSpecBuilder::new()
        .with_parallelism(CHECKPOINT_PARALLELISM as usize)
        .with_execution_profile(ExecutionProfile::MasterWorker {
            num_threads_per_task: 2,
        })
        .with_task_assignment_strategy(TaskWorkerAssignmentStrategyType::Pipelined {
            slots_per_node: 2,
        })
        .with_source(SourceSpec::new(
            "datagen_source",
            SourceSpecKind::Datagen(datagen),
            schema_to_json(schema.as_ref()),
        ))
        .sql("SELECT timestamp, key, value FROM datagen_source")
        .build();

    PipelineLaunchSpec::new(pipeline, 1, 0).with_upsert_key_columns(vec![
        "key".to_string(),
        "timestamp".to_string(),
    ])
}

/// Logical sink row identity for upsert-key `(key, timestamp)` plus value for content checks.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ExpectedUpsertRow {
    map_key: String,
    value_bits: u64,
}

fn row_from_batch(batch: &RecordBatch, row_idx: usize) -> Result<ExpectedUpsertRow> {
    let key = batch
        .column_by_name("key")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| anyhow!("missing key column"))?
        .value(row_idx)
        .to_string();
    let ts = batch
        .column_by_name("timestamp")
        .and_then(|c| c.as_any().downcast_ref::<TimestampMillisecondArray>())
        .ok_or_else(|| anyhow!("missing timestamp column"))?
        .value(row_idx);
    let value_bits = batch
        .column_by_name("value")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| anyhow!("missing value column"))?
        .value(row_idx)
        .to_bits();
    Ok(ExpectedUpsertRow {
        map_key: format!("{key}|{ts}"),
        value_bits,
    })
}

fn materialize_datagen_for_task(
    config: DatagenSourceConfig,
    task_index: i32,
    parallelism: i32,
    num_records: usize,
) -> Result<Vec<RecordBatch>> {
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    let mut source = DatagenSourceFunction::new(config);
    source.task_index = Some(task_index);
    source.parallelism = Some(parallelism);
    source.rng = Some(StdRng::seed_from_u64(42 + task_index as u64));
    source.init_keys();
    let batch_size = source.config.spec.batch_size.max(1);
    let mut out = Vec::new();
    while source.records_generated() < num_records {
        let n = std::cmp::min(batch_size, num_records - source.records_generated());
        out.push(source.generate_batch(n)?);
    }
    Ok(out)
}

fn expected_rows_offline(task_counts: &[(i32, u64)]) -> Result<HashSet<ExpectedUpsertRow>> {
    let (schema, spec) = checkpoint_datagen_parts();
    let config = DatagenSourceConfig::new(schema, spec);
    let mut expected = HashSet::new();
    for &(task_index, num_records) in task_counts {
        let batches = materialize_datagen_for_task(
            config.clone(),
            task_index,
            CHECKPOINT_PARALLELISM,
            num_records as usize,
        )?;
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                expected.insert(row_from_batch(&batch, row_idx)?);
            }
        }
    }
    Ok(expected)
}

fn sink_rows(snapshot: &InMemoryStorageSnapshot) -> Result<HashSet<ExpectedUpsertRow>> {
    let mut actual = HashSet::new();
    for (map_key, message) in snapshot.keyed_messages() {
        let batch = message.record_batch();
        if batch.num_rows() != 1 {
            return Err(anyhow!(
                "expected 1-row upsert message for key {map_key}, got {}",
                batch.num_rows()
            ));
        }
        let row = row_from_batch(batch, 0)?;
        if row.map_key != *map_key {
            return Err(anyhow!(
                "sink map key {map_key} != row-derived {}",
                row.map_key
            ));
        }
        actual.insert(row);
    }
    Ok(actual)
}

fn task_record_counts(snapshot: &PipelineSnapshot) -> Result<Vec<(i32, u64)>> {
    let mut counts = Vec::new();
    for worker in snapshot.worker_states.values() {
        for (vertex_id, meta) in &worker.task_metadata {
            let Some(records) = meta.get(task_meta::RECORDS_GENERATED) else {
                continue;
            };
            let task_index = meta
                .get(task_meta::TASK_INDEX)
                .ok_or_else(|| anyhow!("missing task_index metadata for {vertex_id}"))?
                .parse::<i32>()
                .map_err(|e| anyhow!("bad task_index for {vertex_id}: {e}"))?;
            let n = records
                .parse::<u64>()
                .map_err(|e| anyhow!("bad records_generated for {vertex_id}: {e}"))?;
            counts.push((task_index, n));
        }
    }
    counts.sort_by_key(|(task_index, _)| *task_index);
    if counts.is_empty() {
        return Err(anyhow!(
            "no task metadata with {} in pipeline snapshot",
            task_meta::RECORDS_GENERATED
        ));
    }
    Ok(counts)
}

async fn assert_sink_matches_offline_datagen(
    master: &crate::runtime::tests::cluster_harness::MasterHandle,
    storage: &InMemoryStorageSnapshot,
) -> Result<()> {
    let snapshot = master
        .latest_pipeline_snapshot()
        .await?
        .ok_or_else(|| anyhow!("missing pipeline snapshot after PipelineFinished"))?;
    let task_counts = task_record_counts(&snapshot)?;
    let total: u64 = task_counts.iter().map(|(_, n)| n).sum();
    let expected = expected_rows_offline(&task_counts)?;
    let actual = sink_rows(storage)?;
    if expected != actual {
        return Err(anyhow!(
            "sink/offline datagen set mismatch: expected={} actual={} total_generated={total} task_counts={task_counts:?}",
            expected.len(),
            actual.len()
        ));
    }
    println!(
        "[TEST] sink set equality ok rows={} task_counts={task_counts:?}",
        actual.len()
    );
    Ok(())
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

async fn wait_for_first_checkpoint_completed(
    master: &crate::runtime::tests::cluster_harness::MasterHandle,
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

fn finish_timeout(env: RuntimeEnv) -> Duration {
    match env {
        RuntimeEnv::Local => Duration::from_secs(30),
        RuntimeEnv::Kube | RuntimeEnv::Docker => Duration::from_secs(180),
    }
}

/// Wait for an interval checkpoint, kill a worker, recover, finish sources, assert restore + output.
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

    let checkpoint_id = wait_for_first_checkpoint_completed(
        &cluster.master(),
        &mut cursor,
        timeouts.attempt_running,
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

    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        finish_timeout(env),
        |event| matches!(event, LifecycleEvent::PipelineFinished),
    )
    .await?;

    let snapshot = cluster.storage().snapshot().await?;
    assert_sink_matches_offline_datagen(&cluster.master(), &snapshot).await?;

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

/// Wait for an interval checkpoint and verify barrier propagation precedes completion.
pub async fn run_checkpoint_barrier_path(
    env: RuntimeEnv,
    launch: PipelineLaunchSpec,
) -> Result<u64> {
    let timeouts = RecoveryTimeouts::for_env(env);
    let cluster = TestCluster::launch(env, launch).await?;
    let mut cursor = 0;

    cluster.start_execution().await?;
    wait_until_attempt0_running(&cluster.master(), &mut cursor, timeouts.attempt_running).await?;

    let checkpoint_id = wait_for_first_checkpoint_completed(
        &cluster.master(),
        &mut cursor,
        timeouts.attempt_running,
    )
    .await?;

    let events = cluster.master().lifecycle_events_since(0).await?;
    assert_checkpoint_barrier_path(&events, checkpoint_id)?;

    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        finish_timeout(env),
        |event| matches!(event, LifecycleEvent::PipelineFinished),
    )
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
