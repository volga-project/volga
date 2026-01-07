use crate::{
    cluster::{
        cluster_provider::create_test_cluster_nodes,
        node_assignment::{ExecutionVertexNodeMapping, NodeAssignStrategy, OperatorPerNodeStrategy, node_to_vertex_ids},
    }, common::test_utils::gen_unique_grpc_port, runtime::{
        execution_graph::ExecutionGraph,
        master::{Master, PipelineState},
        master_server::MasterServer,
        master_server::TaskKey,
        worker::WorkerConfig,
        worker_server::WorkerServer,
    }, transport::transport_backend_actor::TransportBackendType
};
use crate::runtime::operators::operator::operator_config_requires_checkpoint;
use anyhow::Result;
use async_trait::async_trait;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{Mutex, mpsc};
use super::executor::Executor;

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
        master_addr: String,
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
                vertex_ids.into_iter().map(|v| std::sync::Arc::<str>::from(v)).collect(),
                1,
                TransportBackendType::Grpc,
            ).with_master_addr(master_addr.clone());
            let mut worker_server = WorkerServer::new(worker_config);
            worker_server.start(&addr).await?;
            
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

#[async_trait]
impl Executor for FakeDistributedExecutor {
    async fn execute(
        &mut self, 
        mut execution_graph: ExecutionGraph, 
        _state_updates_sender: Option<mpsc::Sender<PipelineState>>
    ) -> Result<PipelineState> {
        // Create test cluster nodes
        let num_operators = execution_graph.get_vertices().len();
        let num_workers = num_operators * self.num_workers_per_operator;
        let cluster_nodes = create_test_cluster_nodes(num_workers);

        // Use OperatorPerNodeStrategy to assign vertices to nodes
        let strategy = OperatorPerNodeStrategy;
        let vertex_to_node = strategy.assign_nodes(&execution_graph, &cluster_nodes);

        // Configure channels with node mapping for distributed execution
        execution_graph.update_channels_with_node_mapping(Some(&vertex_to_node));

        // Start master service server (task -> master communication)
        let master_port = gen_unique_grpc_port();
        let master_addr = format!("127.0.0.1:{}", master_port);
        let mut master_server = MasterServer::new();
        let expected_tasks = execution_graph
            .get_vertices()
            .values()
            .filter(|v| operator_config_requires_checkpoint(&v.operator_config))
            .map(|v| TaskKey { vertex_id: v.vertex_id.as_ref().to_string(), task_index: v.task_index })
            .collect::<Vec<_>>();
        master_server.set_checkpointable_tasks(expected_tasks).await;
        master_server.start(&master_addr).await?;

        // Start worker servers
        let worker_addresses = self.start_worker_servers(execution_graph, vertex_to_node, master_addr.clone()).await?;

        // Wait a bit for servers to start
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Create and execute master
        let mut master = Master::new();
        master.execute(worker_addresses).await?;

        master_server.stop().await;

        // For now, we don't have a way to get worker states from distributed execution
        // This would need to be implemented in the master/worker communication
        // Return empty execution state as placeholder
        Ok(PipelineState::new(HashMap::new()))
    }
}