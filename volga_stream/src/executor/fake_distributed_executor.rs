use crate::{
    cluster::{
        cluster_provider::create_test_cluster_nodes,
        node_assignment::{node_to_vertex_ids, ExecutionVertexNodeMapping, NodeAssignStrategy, OperatorPerNodeStrategy},
    },
    runtime::{
        execution_graph::ExecutionGraph,
        master::Master,
        worker::{WorkerConfig, WorkerState},
        worker_server::WorkerServer,
    },
    transport::transport_backend_actor::TransportBackendType,
    common::test_utils::gen_unique_grpc_port,
};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use super::executor::{Executor, ExecutionState};

/// Emulates distributed execution by creating multiple worker servers and a master
pub struct FakeDistributedExecutor {
    num_workers_per_operator: usize,

    worker_servers: Arc<Mutex<Vec<WorkerServer>>>,
}

lazy_static::lazy_static! {
    static ref WORKER_SERVERS: Arc<Mutex<Vec<WorkerServer>>> = Arc::new(Mutex::new(Vec::new()));
}

impl FakeDistributedExecutor {
    pub fn new(num_workers_per_operator: usize) -> Self {
        Self {
            num_workers_per_operator,
            worker_servers: WORKER_SERVERS.clone(),
        }
    }

    /// Starts worker servers and returns their addresses
    async fn start_worker_servers(
        &self,
        execution_graph: ExecutionGraph,
        vertex_to_node: ExecutionVertexNodeMapping,
    ) -> Result<Vec<String>> {
        let mut worker_servers = Vec::new();
        let mut worker_addresses = Vec::new();

        let node_to_vertex_ids = node_to_vertex_ids(&vertex_to_node);
        
        // Start worker servers
        for node_id in node_to_vertex_ids.keys() {
            let port = gen_unique_grpc_port();
            let addr = format!("127.0.0.1:{}", port);
            
            let vertex_ids = node_to_vertex_ids.get(node_id).unwrap().clone();

            // Create and start worker server
            let worker_config = WorkerConfig::new(
                node_id.clone(),
                execution_graph.clone(),
                vertex_ids,
                1,
                TransportBackendType::Grpc,
            );
            let mut worker_server = WorkerServer::new(worker_config);
            worker_server.start(&addr).await.map_err(|e| anyhow::anyhow!("Failed to start worker server: {}", e))?;
            
            worker_servers.push(worker_server);
            worker_addresses.push(addr.clone());
        }

        // Store worker servers
        {
            let mut servers = self.worker_servers.lock().await;
            *servers = worker_servers;
        }

        Ok(worker_addresses)
    }
}

#[async_trait(?Send)]
impl Executor for FakeDistributedExecutor {
    async fn execute(
        &mut self, 
        mut execution_graph: ExecutionGraph, 
        state_sender: Option<mpsc::Sender<WorkerState>>
    ) -> Result<ExecutionState> {
        // Create test cluster nodes
        let num_operators = execution_graph.get_vertices().len();
        let num_workers = num_operators * self.num_workers_per_operator;
        let cluster_nodes = create_test_cluster_nodes(num_workers);

        // Use OperatorPerNodeStrategy to assign vertices to nodes
        let strategy = OperatorPerNodeStrategy;
        let vertex_to_node = strategy.assign_nodes(&execution_graph, &cluster_nodes);

        // Configure channels with node mapping for distributed execution
        execution_graph.update_channels_with_node_mapping(Some(&vertex_to_node));

        // Start worker servers
        let worker_addresses = self.start_worker_servers(execution_graph, vertex_to_node).await?;

        // Wait a bit for servers to start
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Create and execute master
        let mut master = Master::new();
        master.execute(worker_addresses).await.map_err(|e| anyhow::anyhow!("{}", e))?;

        // For now, we don't have a way to get worker states from distributed execution
        // This would need to be implemented in the master/worker communication
        // Return empty execution state as placeholder
        Ok(ExecutionState::new(vec![]))
    }
}