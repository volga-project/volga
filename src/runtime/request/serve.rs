//! Request worker entry: pull the pipeline spec from the master and serve.

use std::env;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::api::compile_pipeline;
use crate::common::grpc::master::{master_client, worker_register};
use crate::common::grpc::GrpcConfig;
use crate::common::types::PipelineId;
use crate::runtime::master::server::master_service::GetRequestConfigRequest;

use super::{RequestExecutor, RequestExecutorOptions};

pub async fn serve_from_env() -> Result<()> {
    let bind_address = env::var("VOLGA_REQUEST_BIND_ADDR")
        .context("VOLGA_REQUEST_BIND_ADDR is required for a request worker")?;
    let master_addr = env::var("MASTER_SERVICE_ADDR")
        .or_else(|_| env::var("VOLGA_MASTER_SERVICE_ADDR"))
        .context("MASTER_SERVICE_ADDR is required for a request worker")?;

    let cfg = worker_register();
    loop {
        match fetch_config(&master_addr, &cfg).await {
            Ok(Some((pipeline_id, spec))) => {
                let request = compile_pipeline(&spec, None).request.ok_or_else(|| {
                    anyhow::anyhow!("pipeline has no request graph")
                })?;
                let worker_id = env::var("VOLGA_WORKER_ID").unwrap_or_else(|_| "request".to_string());
                let executor = RequestExecutor::start(
                    request,
                    RequestExecutorOptions {
                        pipeline_id: PipelineId(pipeline_id),
                        worker_id,
                        wro_store: None,
                        bind_address: Some(bind_address.clone()),
                    },
                )
                .await?;
                println!(
                    "[REQUEST_WORKER] serving {}",
                    executor.bind_address()
                );
                wait_for_shutdown().await;
                drop(executor);
                return Ok(());
            }
            Ok(None) => {
                println!("[REQUEST_WORKER] master has no request config yet");
            }
            Err(error) => {
                println!("[REQUEST_WORKER] config fetch failed: {error:#}");
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn fetch_config(
    master_addr: &str,
    cfg: &GrpcConfig,
) -> Result<Option<(String, crate::api::PipelineSpec)>> {
    let mut client = master_client(master_addr, cfg).await?;
    let response = client
        .get_request_config(tonic::Request::new(GetRequestConfigRequest {}))
        .await?
        .into_inner();
    if !response.ready {
        return Ok(None);
    }
    let spec = serde_json::from_slice(&response.pipeline_spec)
        .context("request config spec is not a pipeline spec")?;
    if response.pipeline_id.is_empty() {
        bail!("request config is missing pipeline_id");
    }
    Ok(Some((response.pipeline_id, spec)))
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
