//! Shared smoke-test runners (local + docker + kube).

pub mod docker;
pub mod kube;
pub mod local;

use anyhow::Result;

use crate::api::TaskWorkerAssignmentStrategyType;
use crate::runtime::functions::source::datagen_source::FieldGenerator;
use crate::runtime::tests::cluster_harness::{
    OutputOracle, PipelineLaunchSpec, RuntimeEnv, TestCluster,
};
use crate::runtime::tests::launch_specs::{
    deployment_smoke_launch_spec, smoke_launch_spec, worker_count_for,
};

/// Local assignment matrix smoke: launch, run to completion, check output values.
pub async fn run_assignment_smoke(
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
    let expected_values = (0..10)
        .map(|index| format!("value_{index}"))
        .collect::<Vec<_>>();
    OutputOracle::assert_string_column_values(
        &cluster.storage().snapshot().await?,
        0,
        &expected_values,
        expected_rows,
    )?;
    cluster.shutdown().await
}

/// Deployment smoke (docker/kube): launch, run to completion, predicate-check output.
pub async fn run_deployment_smoke(
    env: RuntimeEnv,
    launch: PipelineLaunchSpec,
    expected_rows: usize,
    description: &str,
    predicate: impl Fn(&str) -> bool,
) -> Result<()> {
    let cluster = TestCluster::launch(env, launch).await?;
    cluster.start_execution().await?;
    cluster.wait_for_completion().await?;
    OutputOracle::assert_string_column_matches(
        &cluster.storage().snapshot().await?,
        0,
        expected_rows,
        description,
        predicate,
    )?;
    cluster.shutdown().await?;
    Ok(())
}

pub fn docker_smoke_launch_spec() -> PipelineLaunchSpec {
    deployment_smoke_launch_spec(FieldGenerator::Values {
        values: vec![datafusion::common::ScalarValue::Utf8(Some("v".to_string()))],
    })
}

pub fn kube_smoke_launch_spec() -> PipelineLaunchSpec {
    deployment_smoke_launch_spec(FieldGenerator::Key { num_unique: 6 })
}
