use anyhow::Result;

use crate::tests::support::cluster_harness::RuntimeEnv;
use crate::tests::support::smoke::{kube_smoke_launch_spec, run_deployment_smoke};

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
