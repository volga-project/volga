//! Shared launch specs / datagen for checkpoint e2e.

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_integration_test::schema_to_json;
use datafusion::common::ScalarValue;
use std::collections::HashMap;
use std::sync::Arc;

use crate::api::spec::connectors::{SinkSpec, SourceSpec, SourceSpecKind};
use crate::api::spec::pipeline::ExecutionProfile;
use crate::api::{PipelineSpecBuilder, TaskWorkerAssignmentStrategyType};
use crate::runtime::functions::source::datagen_source::{DatagenSpec, FieldGenerator};
use crate::runtime::tests::cluster_harness::PipelineLaunchSpec;
use crate::runtime::tests::launch_specs::worker_count_for;

/// Pipelined slot packing; worker_count = parallelism / slots.
const SLOTS_PER_NODE: usize = 2;
/// Parallelism 2 → 1 worker.
pub const SINGLE_WORKER_PARALLELISM: usize = 2;
/// Parallelism 4 → 2 workers (partial kill + peer reuse).
pub const MULTI_WORKER_PARALLELISM: usize = 4;
/// Sequential kill→restore cycles for the multi-failure stress test.
pub const MULTI_FAILURE_COUNT: usize = 2;
/// Hang guard only (per attempt `open`); harness finishes via `MasterHandle::stop_sources`.
const SAFETY_DATAGEN_RUN_FOR_S: f64 = 300.0;
const DATAGEN_RATE: f32 = 200.0;

pub(super) fn checkpoint_datagen_parts(parallelism: usize) -> (Arc<Schema>, DatagenSpec) {
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
            num_unique: parallelism * 8,
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
            rate: Some(DATAGEN_RATE),
            // Scenario length is harness-driven (StopSources); run_for is hang guard only.
            limit: None,
            run_for_s: Some(SAFETY_DATAGEN_RUN_FOR_S),
            batch_size: 20,
            fields,
            replayable: true,
        },
    )
}

/// Pipelined launch: `worker_count = parallelism / SLOTS_PER_NODE`.
pub fn checkpoint_recovery_launch_spec(parallelism: usize) -> PipelineLaunchSpec {
    let assignment = TaskWorkerAssignmentStrategyType::Pipelined {
        slots_per_node: SLOTS_PER_NODE,
    };
    let worker_count = worker_count_for(&assignment, parallelism);
    let (schema, datagen) = checkpoint_datagen_parts(parallelism);
    let pipeline = PipelineSpecBuilder::new()
        .with_parallelism(parallelism)
        .with_execution_profile(ExecutionProfile::MasterWorker {
            num_threads_per_task: 2,
        })
        .with_task_assignment_strategy(assignment)
        .with_source(SourceSpec::new(
            "datagen_source",
            SourceSpecKind::Datagen(datagen),
            schema_to_json(schema.as_ref()),
        ))
        .sql("SELECT timestamp, key, value FROM datagen_source")
        .with_sink(SinkSpec::in_memory_upsert(vec![
            "key".to_string(),
            "timestamp".to_string(),
        ]))
        .build();

    PipelineLaunchSpec::new(pipeline, worker_count, 0)
}

/// Multi-worker launch for sequential multi-failure stress (same indefinite datagen).
pub fn checkpoint_multi_failure_launch_spec() -> PipelineLaunchSpec {
    checkpoint_recovery_launch_spec(MULTI_WORKER_PARALLELISM)
}
