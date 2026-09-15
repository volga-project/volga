use anyhow::Result;

use crate::test_utils::harness::RuntimeEnv;
use crate::test_utils::request_correctness::{run_request_correctness, RequestProfile};

/// Needs Scylla in the cluster (`VOLGA_SCYLLA_*`).
#[tokio::test]
#[ignore]
async fn request_correctness_scylla_smoke() -> Result<()> {
    run_request_correctness(RuntimeEnv::Kube, RequestProfile::Smoke).await
}
