use std::env;
use std::sync::Arc;

use anyhow::{Result, bail};
use volga::api::compile_logical_graph;
use volga::orchestrator::docker::DockerMasterOrchestrator;
use volga::orchestrator::kube::KubeMasterOrchestrator;
use volga::orchestrator::orchestrator::MasterOrchestrator;
use volga::runtime::master::MasterConfig;
use volga::runtime::master::server::MasterServer;

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

fn build_orchestrator() -> Result<Arc<dyn MasterOrchestrator>> {
    let kind = env::var("VOLGA_ORCHESTRATOR_KIND").unwrap_or_else(|_| "docker".to_string());
    let orchestrator: Arc<dyn MasterOrchestrator> = match kind.as_str() {
        "kube" => Arc::new(KubeMasterOrchestrator::from_env()?),
        "docker" => Arc::new(DockerMasterOrchestrator::from_env()?),
        _ => bail!(
            "unsupported VOLGA_ORCHESTRATOR_KIND={}, expected kube|docker",
            kind
        ),
    };
    Ok(orchestrator)
}

#[tokio::main]
async fn main() -> Result<()> {
    let bind_addr =
        env::var("VOLGA_MASTER_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:50051".to_string());
    let hold_on_finish = env::var("VOLGA_MASTER_HOLD_ON_FINISH")
        .ok()
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1" || v.eq_ignore_ascii_case("yes"))
        .unwrap_or(false);
    let orchestrator = build_orchestrator()?;
    orchestrator.bootstrap().await?;

    let spec = orchestrator.get_spec().await;
    let logical_graph = compile_logical_graph(&spec, None);
    let execution_graph = logical_graph.to_execution_graph();
    let expected_workers = orchestrator.get_num_expected_workers().await;

    let mut master_server = MasterServer::new(orchestrator.clone());
    master_server
        .configure(MasterConfig::with_spec(
            spec,
            execution_graph,
            expected_workers,
        ))
        .await;
    master_server.start(&bind_addr).await?;

    println!(
        "[VOLGA_MASTER] started on {}, expected_workers={}",
        bind_addr, expected_workers
    );

    tokio::select! {
        res = master_server.execute() => {
            res?;
            println!("[VOLGA_MASTER] execute() finished");
            if hold_on_finish {
                println!("[VOLGA_MASTER] hold_on_finish enabled, waiting for shutdown signal");
                shutdown_signal().await;
                println!("[VOLGA_MASTER] shutdown signal received");
            }
        }
        _ = shutdown_signal() => {
            println!("[VOLGA_MASTER] shutdown signal received");
        }
    }

    master_server.stop().await;
    Ok(())
}
