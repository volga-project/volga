use anyhow::Result;

use crate::test_utils::harness::RuntimeEnv;
use crate::test_utils::smoke::{docker_smoke_launch_spec, run_deployment_smoke};

#[tokio::test]
#[ignore]
async fn test_docker_smoke() -> Result<()> {
    run_deployment_smoke(
        RuntimeEnv::Docker,
        docker_smoke_launch_spec(),
        60,
        "all Docker output values to equal 'v'",
        |value| value == "v",
    )
    .await
}
