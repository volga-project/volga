use std::collections::HashMap;

use testcontainers::core::WaitFor;
use testcontainers::{clients::Cli, GenericImage, RunnableImage};

use crate::test_utils::parquet::{parquet_roundtrip_via_sink_and_source, test_schema};

async fn wait_for_localstack_ready(addr: &str) {
    for _ in 0..40 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    panic!("LocalStack did not become ready at {}", addr);
}

async fn create_localstack_bucket(endpoint: &str, bucket: &str) {
    let url = format!("{}/{}", endpoint, bucket);
    let client = reqwest::Client::new();
    for _ in 0..10 {
        match client.put(&url).send().await {
            Ok(resp) if resp.status().is_success() || resp.status().as_u16() == 409 => {
                return;
            }
            _ => {}
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    panic!("Failed to create LocalStack bucket at {}", url);
}

#[tokio::test]
#[ignore]
async fn parquet_s3_roundtrip_localstack() {
    let docker = Cli::default();
    let image = GenericImage::new("localstack/localstack", "3.0")
        .with_env_var("SERVICES", "s3")
        .with_env_var("DEFAULT_REGION", "us-east-1")
        .with_exposed_port(4566)
        .with_wait_for(WaitFor::seconds(3));
    let runnable = RunnableImage::from(image).with_mapped_port((4566, 4566));
    let container = docker.run(runnable);
    let endpoint = format!("http://127.0.0.1:{}", container.get_host_port_ipv4(4566));
    wait_for_localstack_ready("127.0.0.1:4566").await;
    create_localstack_bucket(&endpoint, "volga-test").await;

    let schema = test_schema();
    let opts = HashMap::from([
        ("endpoint_url".to_string(), endpoint),
        ("region".to_string(), "us-east-1".to_string()),
        ("access_key_id".to_string(), "test".to_string()),
        ("secret_access_key".to_string(), "test".to_string()),
    ]);
    parquet_roundtrip_via_sink_and_source(
        schema,
        "s3://volga-test/output".to_string(),
        opts.clone(),
        "s3://volga-test/output".to_string(),
        opts,
    )
    .await;
}
