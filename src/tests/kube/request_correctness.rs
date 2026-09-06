use anyhow::Result;

use crate::test_utils::harness::RuntimeEnv;
use crate::test_utils::request_correctness::{
    run_request_correctness, RequestProfile, RequestStoreBackend,
};

#[tokio::test]
#[ignore]
async fn request_correctness_inmem_grpc_smoke() -> Result<()> {
    run_request_correctness(
        RuntimeEnv::Kube,
        RequestStoreBackend::InMemoryGrpc,
        RequestProfile::Smoke,
    )
    .await
}
