use anyhow::Result;

use crate::runtime::tests::cluster_harness::RuntimeEnv;
use crate::runtime::tests::smoke_tests::{kube_smoke_launch_spec, run_deployment_smoke};

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
