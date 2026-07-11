use std::sync::Arc;

use anyhow::Result;
use uuid::Uuid;

use crate::api::{LogicalGraph, PipelineSpec};
use crate::orchestrator::local::{LocalMasterOrchestrator, LocalWorkerOrchestrator};
use crate::orchestrator::orchestrator::{MasterOrchestrator, WorkerOrchestrator};
use crate::common::test_utils::gen_unique_grpc_port;
use crate::runtime::master::MasterConfig;
use crate::runtime::master::server::MasterServer;
use crate::runtime::observability::PipelineSnapshot;
use crate::runtime::worker_server::WorkerServer;

// Locally simulates a master and worker executing a pipeline
pub async fn execute(
    spec: PipelineSpec,
    logical_graph: LogicalGraph,
    num_workers: usize,
) -> Result<PipelineSnapshot> {
    let master_port = gen_unique_grpc_port();
    let master_addr = format!("127.0.0.1:{}", master_port);
    let pipeline_id = Uuid::new_v4().to_string();
    let local_master_orchestrator = Arc::new(
        LocalMasterOrchestrator::new(num_workers.max(1), pipeline_id)
            .with_spec(spec.clone()),
    );
    let master_orchestrator: Arc<dyn MasterOrchestrator> = local_master_orchestrator.clone();
    let worker_orchestrator: Arc<dyn WorkerOrchestrator> =
        Arc::new(LocalWorkerOrchestrator::new(master_addr.clone()));

    let worker_nodes_map = master_orchestrator.get_worker_nodes().await;
    let expected_workers = master_orchestrator.get_num_expected_workers().await;
    let execution_graph = logical_graph.to_execution_graph();
    let master_config = MasterConfig::with_spec(spec.clone(), execution_graph, expected_workers);
    let mut master_server = MasterServer::new(master_orchestrator);
    master_server.configure(master_config).await;
    master_server.start(&master_addr).await?;

    let mut worker_servers = Vec::new();

    for node in worker_nodes_map.values() {
        let worker_id = node.worker_id.clone();
        let addr = format!("{}:{}", node.worker_ip, node.worker_port);
        let mut worker_server = WorkerServer::new(worker_id, worker_orchestrator.clone());
        worker_server.start(&addr).await?;
        worker_server.register_with_master().await?;
        worker_servers.push(worker_server);
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    master_server.execute().await?;
    let worker_states = master_server.get_worker_states().await;
    let mut servers = worker_servers;
    for s in servers.iter_mut() {
        s.stop().await;
    }
    let mut ms = master_server;
    ms.stop().await;
    Ok(PipelineSnapshot::new(worker_states))
}