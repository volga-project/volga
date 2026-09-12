use anyhow::Result;

use crate::test_utils::harness::RuntimeEnv;
use crate::test_utils::request_correctness::{
    run_request_correctness, run_request_correctness_with_fault, RequestFault, RequestProfile,
};

/// Needs Scylla (`VOLGA_SCYLLA_CONTACT`, optional `VOLGA_SCYLLA_KEYSPACE` / `VOLGA_SCYLLA_DC`).
#[tokio::test]
#[ignore]
async fn request_correctness_scylla_smoke() -> Result<()> {
    run_request_correctness(RuntimeEnv::Local, RequestProfile::Smoke).await
}

#[tokio::test]
#[ignore]
async fn request_correctness_kill_write_worker() -> Result<()> {
    run_request_correctness_with_fault(
        RuntimeEnv::Local,
        RequestProfile::Smoke,
        RequestFault::KillWriteWorker,
    )
    .await
}

#[tokio::test]
#[ignore]
async fn request_correctness_kill_read_worker() -> Result<()> {
    run_request_correctness_with_fault(
        RuntimeEnv::Local,
        RequestProfile::Smoke,
        RequestFault::KillReadWorker,
    )
    .await
}
