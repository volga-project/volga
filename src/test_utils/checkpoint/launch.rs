//! Shared launch specs / datagen for checkpoint suite.

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_integration_test::schema_to_json;
use datafusion::common::ScalarValue;
use std::collections::HashMap;
use std::sync::Arc;

use crate::api::spec::connectors::{SinkSpec, SourceSpec, SourceSpecKind};
use crate::api::spec::operators::{OperatorOverride, OperatorTuningSpec};
use crate::api::spec::pipeline::ExecutionProfile;
use crate::api::{CheckpointSpec, PipelineSpecBuilder, TaskWorkerAssignmentStrategyType};
use crate::runtime::functions::source::datagen_source::{DatagenSpec, FieldGenerator};
use crate::runtime::operators::window::spec::WindowSpec;
use crate::runtime::operators::window::{TileConfig, TimeGranularity};
use crate::test_utils::harness::PipelineLaunchSpec;
use crate::test_utils::launch_specs::worker_count_for;

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
pub const WINDOW_RANGE_MS: i64 = 10_000;
/// Shared-key window workload: every source task emits this many keys (`key-0`..).
pub const SHARED_WINDOW_KEYS: usize = 4;

pub fn local_checkpoint_spec() -> CheckpointSpec {
    CheckpointSpec {
        interval_ms: Some(500),
        timeout_ms: Some(5_000),
        retention: Some(2),
    }
}

pub fn kube_checkpoint_spec() -> CheckpointSpec {
    CheckpointSpec {
        interval_ms: Some(2_000),
        timeout_ms: Some(60_000),
        retention: Some(2),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointWorkload {
    PassThrough,
    Window,
    /// Same window SQL as `Window`, but all source tasks share `key-0`..`key-{SHARED_WINDOW_KEYS-1}`.
    WindowSharedKeys,
}

impl CheckpointWorkload {
    pub fn is_window(self) -> bool {
        matches!(self, Self::Window | Self::WindowSharedKeys)
    }

    fn timestamp_step_ms(self) -> i64 {
        match self {
            Self::PassThrough => 1,
            Self::Window | Self::WindowSharedKeys => 1_000,
        }
    }

    fn key_generator(self, parallelism: usize) -> FieldGenerator {
        match self {
            Self::PassThrough => FieldGenerator::key(parallelism * 8),
            Self::Window => FieldGenerator::key(parallelism * 4),
            Self::WindowSharedKeys => FieldGenerator::shared_key(SHARED_WINDOW_KEYS),
        }
    }
}

pub(super) fn checkpoint_datagen_parts(
    parallelism: usize,
    workload: CheckpointWorkload,
) -> (Arc<Schema>, DatagenSpec) {
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
            step_ms: workload.timestamp_step_ms(),
        },
    );
    fields.insert("key".to_string(), workload.key_generator(parallelism));
    fields.insert(
        "value".to_string(),
        FieldGenerator::Values {
            values: vec![
                ScalarValue::Float64(Some(1.0)),
                ScalarValue::Float64(Some(2.0)),
            ],
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
pub fn checkpoint_recovery_launch_spec(
    parallelism: usize,
    workload: CheckpointWorkload,
) -> PipelineLaunchSpec {
    let assignment = TaskWorkerAssignmentStrategyType::Pipelined {
        slots_per_node: SLOTS_PER_NODE,
    };
    let worker_count = worker_count_for(&assignment, parallelism);
    let (schema, datagen) = checkpoint_datagen_parts(parallelism, workload);
    let mut builder = PipelineSpecBuilder::new()
        .with_parallelism(parallelism)
        .with_execution_profile(ExecutionProfile::MasterWorker {
            num_threads_per_task: 2,
        })
        .with_task_assignment_strategy(assignment)
        .with_source(SourceSpec::new(
            "datagen_source",
            SourceSpecKind::Datagen(datagen),
            schema_to_json(schema.as_ref()),
        ));

    let sql = match workload {
        CheckpointWorkload::PassThrough => "SELECT timestamp, key, value FROM datagen_source",
        CheckpointWorkload::Window | CheckpointWorkload::WindowSharedKeys => {
            let tiling = TileConfig::new(vec![
                TimeGranularity::Seconds(1),
                TimeGranularity::Seconds(5),
            ])
            .expect("valid checkpoint window tiling");
            builder = builder
                .with_out_of_orderness_ms(0)
                .with_window_allowed_lateness_ms(0)
                .with_operator_overrides_defaults(OperatorOverride {
                    tuning: Some(OperatorTuningSpec::Window(WindowSpec {
                        lateness: 0,
                        tiling: Some(tiling),
                    })),
                    ..OperatorOverride::default()
                });
            "SELECT timestamp, key, value, \
             SUM(value) OVER w AS sum_val, \
             COUNT(value) OVER w AS cnt_val, \
             AVG(value) OVER w AS avg_val, \
             MIN(value) OVER w AS min_val, \
             MAX(value) OVER w AS max_val \
             FROM datagen_source \
             WINDOW w AS (PARTITION BY key ORDER BY timestamp \
             RANGE BETWEEN INTERVAL '10000' MILLISECOND PRECEDING AND CURRENT ROW)"
        }
    };

    let checkpoint = local_checkpoint_spec();
    let pipeline = builder
        .sql(sql)
        .with_sink(SinkSpec::in_memory_upsert(vec![
            "key".to_string(),
            "timestamp".to_string(),
        ]))
        .with_checkpoint(
            checkpoint.interval_ms,
            checkpoint.timeout_ms,
            checkpoint.retention,
        )
        .build();

    PipelineLaunchSpec::new(pipeline, worker_count, None)
}

/// Multi-worker launch for sequential multi-failure stress (same indefinite datagen).
pub fn checkpoint_multi_failure_launch_spec() -> PipelineLaunchSpec {
    checkpoint_recovery_launch_spec(MULTI_WORKER_PARALLELISM, CheckpointWorkload::PassThrough)
}

/// Window recovery with shared keys across source tasks (hash repartition + multi-input WM merge).
pub fn checkpoint_shared_key_window_launch_spec(parallelism: usize) -> PipelineLaunchSpec {
    checkpoint_recovery_launch_spec(parallelism, CheckpointWorkload::WindowSharedKeys)
}
