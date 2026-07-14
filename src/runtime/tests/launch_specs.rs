//! Shared pipeline launch specs for cluster smoke / recovery tests.

use arrow::datatypes::{DataType, Field, Schema};
use arrow_integration_test::schema_to_json;
use datafusion::common::ScalarValue;

use crate::api::spec::connectors::{SourceSpec, SourceSpecKind};
use crate::api::spec::pipeline::ExecutionProfile;
use crate::api::{PipelineSpecBuilder, TaskWorkerAssignmentStrategyType};
use crate::runtime::functions::source::datagen_source::{DatagenSpec, FieldGenerator};
use crate::runtime::tests::cluster_harness::PipelineLaunchSpec;

pub fn default_pipelined_smoke_launch_spec() -> PipelineLaunchSpec {
    smoke_launch_spec(
        TaskWorkerAssignmentStrategyType::Pipelined { slots_per_node: 2 },
        6,
        3,
    )
}

pub fn smoke_launch_spec(
    assignment_strategy: TaskWorkerAssignmentStrategyType,
    parallelism: usize,
    worker_count: usize,
) -> PipelineLaunchSpec {
    const VALUES_PER_SOURCE: usize = 10;
    let rows = VALUES_PER_SOURCE * parallelism;

    let schema = Schema::new(vec![Field::new("value", DataType::Utf8, false)]);
    let mut fields = std::collections::HashMap::new();
    fields.insert(
        "value".to_string(),
        FieldGenerator::Values {
            values: (0..VALUES_PER_SOURCE)
                .map(|index| ScalarValue::Utf8(Some(format!("value_{index}"))))
                .collect(),
        },
    );
    let pipeline = PipelineSpecBuilder::new()
        .with_parallelism(parallelism)
        .with_execution_profile(ExecutionProfile::MasterWorker {
            num_threads_per_task: 4,
        })
        .with_task_assignment_strategy(assignment_strategy)
        .with_source(SourceSpec::new(
            "test_table",
            SourceSpecKind::Datagen(DatagenSpec {
                rate: None,
                limit: Some(rows),
                run_for_s: None,
                batch_size: 5,
                fields,
                replayable: true,
            }),
            schema_to_json(&schema),
        ))
        .sql("SELECT value FROM test_table")
        .build();
    PipelineLaunchSpec::new(pipeline, worker_count, rows)
}

pub fn deployment_smoke_launch_spec(generator: FieldGenerator) -> PipelineLaunchSpec {
    const WORKER_COUNT: usize = 3;
    const SLOTS_PER_NODE: usize = 2;
    const PARALLELISM: usize = WORKER_COUNT * SLOTS_PER_NODE;
    const EXPECTED_ROWS: usize = 60;

    let schema = Schema::new(vec![Field::new("value", DataType::Utf8, false)]);
    let mut fields = std::collections::HashMap::new();
    fields.insert("value".to_string(), generator);
    let pipeline = PipelineSpecBuilder::new()
        .with_parallelism(PARALLELISM)
        .with_execution_profile(ExecutionProfile::MasterWorker {
            num_threads_per_task: 4,
        })
        .with_task_assignment_strategy(TaskWorkerAssignmentStrategyType::Pipelined {
            slots_per_node: SLOTS_PER_NODE,
        })
        .with_source(SourceSpec::new(
            "test_table",
            SourceSpecKind::Datagen(DatagenSpec {
                rate: None,
                limit: Some(EXPECTED_ROWS),
                run_for_s: None,
                batch_size: 5,
                fields,
                replayable: true,
            }),
            schema_to_json(&schema),
        ))
        .sql("SELECT value FROM test_table")
        .build();
    PipelineLaunchSpec::new(pipeline, WORKER_COUNT, EXPECTED_ROWS)
}

pub fn worker_count_for(
    assignment_strategy: &TaskWorkerAssignmentStrategyType,
    total_parallelism: usize,
) -> usize {
    match assignment_strategy {
        TaskWorkerAssignmentStrategyType::SingleWorker => 1,
        TaskWorkerAssignmentStrategyType::OperatorPerWorker => 3,
        TaskWorkerAssignmentStrategyType::Pipelined { slots_per_node } => {
            assert!(*slots_per_node > 0);
            assert_eq!(total_parallelism % slots_per_node, 0);
            total_parallelism / slots_per_node
        }
    }
}
