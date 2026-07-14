use anyhow::Result;
use datafusion::common::ScalarValue;

use crate::api::TaskWorkerAssignmentStrategyType;
use crate::runtime::functions::source::datagen_source::FieldGenerator;
use crate::runtime::tests::cluster_harness::{
    OutputOracle, RuntimeEnv, TestCluster,
};
use crate::runtime::tests::launch_specs::{
    deployment_smoke_launch_spec, smoke_launch_spec, worker_count_for,
};

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
#[ignore]
async fn test_docker_smoke() -> Result<()> {
    let cluster = TestCluster::launch(
        RuntimeEnv::Docker,
        deployment_smoke_launch_spec(FieldGenerator::Values {
            values: vec![ScalarValue::Utf8(Some("v".to_string()))],
        }),
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
        deployment_smoke_launch_spec(FieldGenerator::Key { num_unique: 6 }),
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
