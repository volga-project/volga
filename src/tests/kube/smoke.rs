use anyhow::Result;

use crate::test_utils::harness::RuntimeEnv;
use crate::test_utils::smoke::{kube_smoke_launch_spec, run_deployment_smoke};

#[tokio::test]
#[ignore]
async fn test_kube_smoke() -> Result<()> {
    run_deployment_smoke(
        RuntimeEnv::Kube,
        kube_smoke_launch_spec(),
        60,
        "all Kubernetes output values to start with 'key-'",
        |value| value.starts_with("key-"),
    )
    .await
}
