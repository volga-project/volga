use std::env;
use std::sync::Arc;

use anyhow::{Result, bail};
use volga::orchestrator::docker::DockerWorkerOrchestrator;
use volga::orchestrator::kube::KubeWorkerOrchestrator;
use volga::orchestrator::orchestrator::WorkerOrchestrator;
use volga::runtime::health::{init_worker_health_bus, report_worker_fatal, WorkerFatalReason};
use volga::runtime::worker_server::WorkerServer;

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn build_worker_bootstrap() -> Result<(String, Arc<dyn WorkerOrchestrator>)> {
    let kind = env::var("VOLGA_ORCHESTRATOR_KIND").unwrap_or_else(|_| "docker".to_string());
    match kind.as_str() {
        "kube" => {
            let orchestrator = Arc::new(KubeWorkerOrchestrator::from_env()?);
            let worker_id = orchestrator.resolve_worker_id().await?;
            let orchestrator: Arc<dyn WorkerOrchestrator> = orchestrator;
            Ok((worker_id, orchestrator))
        }
        "docker" => {
            let orchestrator = Arc::new(DockerWorkerOrchestrator::from_env()?);
            let worker_id = orchestrator.resolve_worker_id().await?;
            let orchestrator: Arc<dyn WorkerOrchestrator> = orchestrator;
            Ok((worker_id, orchestrator))
        }
        _ => bail!(
            "unsupported VOLGA_ORCHESTRATOR_KIND={}, expected kube|docker",
            kind
        ),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    init_worker_health_bus();
    std::panic::set_hook(Box::new(|panic_info| {
        let panic_msg = panic_info.to_string();
        let reason = if panic_msg.contains("[GRPC_BACKEND]") {
            WorkerFatalReason::TransportDisconnect
        } else {
            WorkerFatalReason::Panic
        };
        report_worker_fatal(reason, panic_msg);
    }));

    let bind_addr =
        env::var("VOLGA_WORKER_BIND_ADDR").expect("VOLGA_WORKER_BIND_ADDR is not set");
    let hold_on_finish = env::var("VOLGA_WORKER_HOLD_ON_FINISH")
        .ok()
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1" || v.eq_ignore_ascii_case("yes"))
        .unwrap_or(false);
    let (worker_id, orchestrator) = build_worker_bootstrap().await?;
    let mut worker_server = WorkerServer::new(worker_id.clone(), orchestrator);
    worker_server.start(&bind_addr).await?;
    worker_server.register_with_master().await?;

    println!(
        "[VOLGA_WORKER] started worker_id={} bind_addr={}",
        worker_id, bind_addr
    );

    if hold_on_finish {
        println!("[VOLGA_WORKER] hold_on_finish enabled, waiting for shutdown signal");
        shutdown_signal().await;
        println!("[VOLGA_WORKER] shutdown signal received");
    } else {
        tokio::select! {
            _ = worker_server.wait_for_close_request() => {
                println!("[VOLGA_WORKER] close_worker requested, shutting down");
            }
            _ = shutdown_signal() => {
                println!("[VOLGA_WORKER] shutdown signal received");
            }
        }
    }
    worker_server.stop().await;
    Ok(())
}
