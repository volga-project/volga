use std::env;
use std::fs;

use anyhow::Result;
use testcontainers::{clients::Cli, images::generic::GenericImage};

use volga_stream::api::spec::pipeline::PipelineSpecBuilder;
use volga_stream::api::ExecutionProfile;
use volga_stream::control_plane::types::{AttemptId, ExecutionIds};
use volga_stream::executor::k8s_runtime_adapter::{K8sRuntimeAdapter, K8sRuntimeConfig};
use volga_stream::executor::runtime_adapter::StartAttemptRequest;
use volga_stream::runtime::execution_graph::ExecutionGraph;

#[tokio::test]
async fn k8s_pipeline_lifecycle() -> Result<()> {
    let image = match env::var("VOLGA_STREAM_IMAGE") {
        Ok(value) => value,
        Err(_) => {
            eprintln!("VOLGA_STREAM_IMAGE not set; skipping k8s lifecycle test");
            return Ok(());
        }
    };

    let docker = Cli::default();
    let k3s_image = GenericImage::new("rancher/k3s", "v1.28.2-k3s1")
        .with_env_var("K3S_KUBECONFIG_MODE", "644")
        .with_exposed_port(6443);
    let node = docker.run(k3s_image);
    let host_port = node.get_host_port_ipv4(6443);

    let kubeconfig_raw = String::from_utf8(
        node.exec(vec!["/bin/sh", "-c", "cat /etc/rancher/k3s/k3s.yaml"])
            .stdout,
    )?;
    let kubeconfig = kubeconfig_raw.replace(
        "https://127.0.0.1:6443",
        &format!("https://127.0.0.1:{host_port}"),
    );
    let kubeconfig_path = env::temp_dir().join(format!("kubeconfig-{}.yaml", uuid::Uuid::new_v4()));
    fs::write(&kubeconfig_path, kubeconfig)?;
    env::set_var("KUBECONFIG", &kubeconfig_path);

    let adapter = K8sRuntimeAdapter::new(K8sRuntimeConfig {
        master_image: image.clone(),
        worker_image: image,
        ..K8sRuntimeConfig::default()
    })
    .await?;

    let mut spec = PipelineSpecBuilder::new()
        .with_execution_profile(ExecutionProfile::Orchestrated {
            num_workers_per_operator: 1,
        })
        .build();
    spec.sql = None;

    let execution_ids = ExecutionIds::fresh(AttemptId(1));
    let handle = adapter
        .start_attempt(StartAttemptRequest {
            execution_ids: execution_ids.clone(),
            pipeline_spec: spec.clone(),
            execution_graph: ExecutionGraph::new(),
            num_workers_per_operator: 1,
            node_assign_strategy: spec.node_assign_strategy.clone(),
            transport_overrides_queue_records: spec.transport_overrides_queue_records(),
            worker_runtime: spec.worker_runtime.clone(),
            operator_type_storage_overrides: spec.operator_type_storage_overrides(),
        })
        .await?;

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    adapter.stop_attempt(handle).await?;

    Ok(())
}
