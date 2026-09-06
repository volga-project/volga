use anyhow::Result;

use crate::test_utils::harness::RuntimeEnv;
use crate::test_utils::request_correctness::{
    run_request_correctness, run_request_correctness_with_fault, RequestFault, RequestProfile,
    RequestStoreBackend,
};

#[tokio::test]
async fn request_correctness_inmem_grpc_smoke() -> Result<()> {
    run_request_correctness(
        RuntimeEnv::Local,
        RequestStoreBackend::InMemoryGrpc,
        RequestProfile::Smoke,
    )
    .await
}

#[tokio::test]
async fn request_correctness_kill_write_worker() -> Result<()> {
    run_request_correctness_with_fault(
        RuntimeEnv::Local,
        RequestStoreBackend::InMemoryGrpc,
        RequestProfile::Smoke,
        RequestFault::KillWriteWorker,
    )
    .await
}

#[tokio::test]
async fn request_correctness_kill_read_worker() -> Result<()> {
    run_request_correctness_with_fault(
        RuntimeEnv::Local,
        RequestStoreBackend::InMemoryGrpc,
        RequestProfile::Smoke,
        RequestFault::KillReadWorker,
    )
    .await
}
