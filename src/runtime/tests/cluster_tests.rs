use anyhow::Result;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_integration_test::schema_to_json;
use datafusion::common::ScalarValue;
use std::time::Duration;

use crate::api::spec::connectors::{SourceSpec, SourceSpecKind};
use crate::api::spec::pipeline::ExecutionProfile;
use crate::api::{PipelineSpecBuilder, TaskWorkerAssignmentStrategyType};
use crate::runtime::functions::source::datagen_source::{DatagenSpec, FieldGenerator};
use crate::runtime::tests::cluster_harness::{
    LifecycleOracle, OutputOracle, PipelineLaunchSpec, RuntimeEnv, TestCluster,
};
use crate::runtime::master::LifecycleEvent;

// tests pass individually but can fail when running the full suite (likely cross-test interference).

#[tokio::test]
async fn test_local_single_worker_assignment() -> Result<()> {
    run_local_assignment_smoke(TaskWorkerAssignmentStrategyType::SingleWorker, 4).await
}

#[tokio::test]
async fn test_local_operator_per_worker_assignment() -> Result<()> {
    run_local_assignment_smoke(TaskWorkerAssignmentStrategyType::OperatorPerWorker, 4).await
}

#[tokio::test]
async fn test_local_pipelined_assignment() -> Result<()> {
    run_local_assignment_smoke(
        TaskWorkerAssignmentStrategyType::Pipelined { slots_per_node: 2 },
        4,
    )
    .await
}

#[tokio::test]
async fn test_local_worker_crash_recovers() -> Result<()> {
    let cluster = TestCluster::launch(
        RuntimeEnv::Local,
        recovery_launch_spec(),
    )
    .await?;
    let target_worker = cluster
        .worker_ids()
        .into_iter()
        .next()
        .expect("recovery cluster must have a worker");
    let mut cursor = 0;
    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        Duration::from_secs(2),
        |event| matches!(event, LifecycleEvent::WorkerRegistered { worker_id, .. } if worker_id == &target_worker),
    )
    .await?;
    cluster.start_execution().await?;

    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        Duration::from_secs(10),
        |event| matches!(event, LifecycleEvent::AttemptRunning { attempt_id: 0, .. }),
    )
    .await?;
    cluster.worker(&target_worker).unwrap().kill().await?;

    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        Duration::from_secs(20),
        |event| matches!(event, LifecycleEvent::WorkerFailure { attempt_id: 0, .. }),
    )
    .await?;
    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        Duration::from_secs(10),
        |event| matches!(event, LifecycleEvent::RecoveryStarted { attempt_id: 0, .. }),
    )
    .await?;
    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        Duration::from_secs(20),
        |event| matches!(event, LifecycleEvent::ReplacementRequested { worker_ids }
            if worker_ids.iter().any(|worker_id| worker_id == &target_worker)),
    )
    .await?;
    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        Duration::from_secs(10),
        |event| matches!(event, LifecycleEvent::WorkerRegistered { worker_id, .. } if worker_id == &target_worker),
    )
    .await?;
    LifecycleOracle::wait_for(
        &cluster.master(),
        &mut cursor,
        Duration::from_secs(20),
        |event| matches!(event, LifecycleEvent::AttemptRunning { attempt_id, .. } if *attempt_id >= 1),
    )
    .await?;

    cluster.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_docker_smoke() -> Result<()> {
    let cluster = TestCluster::launch(
        RuntimeEnv::Docker,
        docker_smoke_launch_spec(),
    )
    .await?;
    cluster.start_execution().await?;
    cluster.wait_for_completion().await?;
    OutputOracle::assert_string_column_matches(
        &cluster.storage().snapshot().await?,
        0,
        60,
        "all Docker output values to equal 'v'",
        |value| value == "v",
    )?;
    cluster.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[ignore]
async fn test_kube_smoke() -> Result<()> {
    let cluster = TestCluster::launch(
        RuntimeEnv::Kube,
        kube_smoke_launch_spec(),
    )
    .await?;
    cluster.start_execution().await?;
    cluster.wait_for_completion().await?;
    OutputOracle::assert_string_column_matches(
        &cluster.storage().snapshot().await?,
        0,
        60,
        "all Kubernetes output values to start with 'key-'",
        |value| value.starts_with("key-"),
    )?;
    cluster.shutdown().await?;
    Ok(())
}

async fn run_local_assignment_smoke(
    assignment_strategy: TaskWorkerAssignmentStrategyType,
    parallelism: usize,
) -> Result<()> {
    let worker_count = worker_count_for(&assignment_strategy, parallelism);
    let expected_rows = 10 * parallelism;
    let cluster = TestCluster::launch(
        RuntimeEnv::Local,
        smoke_launch_spec(assignment_strategy, parallelism, worker_count),
    )
    .await?;
    assert_eq!(cluster.worker_ids().len(), worker_count);
    cluster.start_execution().await?;
    cluster.wait_for_completion().await?;
    let expected_values = (0..10).map(|index| format!("value_{index}")).collect::<Vec<_>>();
    OutputOracle::assert_string_column_values(
        &cluster.storage().snapshot().await?,
        0,
        &expected_values,
        expected_rows,
    )?;
    cluster.shutdown().await
}

fn default_smoke_launch_spec() -> PipelineLaunchSpec {
    smoke_launch_spec(
        TaskWorkerAssignmentStrategyType::Pipelined { slots_per_node: 2 },
        6,
        3,
    )
}

fn docker_smoke_launch_spec() -> PipelineLaunchSpec {
    deployment_smoke_launch_spec(FieldGenerator::Values {
        values: vec![ScalarValue::Utf8(Some("v".to_string()))],
    })
}

fn kube_smoke_launch_spec() -> PipelineLaunchSpec {
    deployment_smoke_launch_spec(FieldGenerator::Key { num_unique: 6 })
}

fn deployment_smoke_launch_spec(generator: FieldGenerator) -> PipelineLaunchSpec {
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

fn smoke_launch_spec(
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

fn recovery_launch_spec() -> PipelineLaunchSpec {
    let mut launch = default_smoke_launch_spec();
    launch.expected_output_rows = 0;
    if let SourceSpecKind::Datagen(datagen) = &mut launch.pipeline.sources[0].source {
        datagen.rate = Some(50.0);
        datagen.limit = None;
    }
    launch
}

fn worker_count_for(
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
