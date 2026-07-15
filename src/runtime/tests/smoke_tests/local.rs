use anyhow::Result;

use crate::api::TaskWorkerAssignmentStrategyType;
use crate::runtime::tests::smoke_tests::run_assignment_smoke;

// tests pass individually but can fail when running the full suite (likely cross-test interference).

#[tokio::test]
async fn test_local_single_worker_assignment() -> Result<()> {
    run_assignment_smoke(TaskWorkerAssignmentStrategyType::SingleWorker, 4).await
}

#[tokio::test]
async fn test_local_operator_per_worker_assignment() -> Result<()> {
    run_assignment_smoke(TaskWorkerAssignmentStrategyType::OperatorPerWorker, 4).await
}

#[tokio::test]
async fn test_local_pipelined_assignment() -> Result<()> {
    run_assignment_smoke(
        TaskWorkerAssignmentStrategyType::Pipelined { slots_per_node: 2 },
        4,
    )
    .await
}
