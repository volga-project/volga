use crate::runtime::{
    execution_graph::ExecutionGraph, master::PipelineState, master_server::MasterServer, worker::{Worker, WorkerConfig, WorkerState}
};
use crate::transport::transport_backend_actor::TransportBackendType;
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use tokio::sync::mpsc;
use super::executor::Executor;
use crate::common::test_utils::gen_unique_grpc_port;
use crate::runtime::master_server::TaskKey;

/// Executes the job locally in a single process using a Worker instance
pub struct LocalExecutor;

impl LocalExecutor {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Executor for LocalExecutor {
    async fn execute(
        &mut self, 
        mut execution_graph: ExecutionGraph, 
        state_updates_sender: Option<mpsc::Sender<PipelineState>>
    ) -> Result<PipelineState> {
        // Configure channels for local execution
        execution_graph.update_channels_with_node_mapping(None);

        // Get all vertex IDs from the graph
        let vertex_ids = execution_graph.get_vertices().keys().cloned().collect();

        // Create worker config
        let worker_id = "local_worker".to_string();

        // Start master service server (task -> master communication)
        let master_port = gen_unique_grpc_port();
        let master_addr = format!("127.0.0.1:{}", master_port);
        let mut master_server = MasterServer::new();
        let expected_tasks = execution_graph
            .get_vertices()
            .values()
            .map(|v| TaskKey { vertex_id: v.vertex_id.clone(), task_index: v.task_index })
            .collect::<Vec<_>>();
        master_server.set_expected_tasks(expected_tasks).await;
        master_server.start(&master_addr).await?;

        let worker_config = WorkerConfig::new(
            worker_id.clone(),
            execution_graph,
            vertex_ids,
            4,
            TransportBackendType::InMemory,
        ).with_master_addr(master_addr);

        // Create worker
        let mut worker = Worker::new(worker_config);
        
        // Execute the worker
        if let Some(pipeline_state_sender) = state_updates_sender {
            // Create a worker_state_sender that pipes WorkerState into PipelineState
            let (worker_state_sender, mut worker_state_receiver) = mpsc::channel::<WorkerState>(100);
            
            // Spawn a task to forward WorkerState updates as PipelineState
            let pipeline_sender = pipeline_state_sender.clone();
            let worker_id_clone = worker_id.clone();
            tokio::spawn(async move {
                while let Some(worker_state) = worker_state_receiver.recv().await {
                    let mut worker_states = HashMap::new();
                    worker_states.insert(worker_id_clone.clone(), worker_state);
                    let pipeline_state = PipelineState::new(worker_states);
                    let _ = pipeline_sender.send(pipeline_state).await;
                }
            });

            worker.execute_worker_lifecycle_for_testing_with_state_updates(worker_state_sender).await;
        } else {
            worker.execute_worker_lifecycle_for_testing().await;
        }

        // Get final state
        let worker_state = worker.get_state().await;
        worker.close().await;
        
        // Create PipelineState from worker state
        let mut worker_states = HashMap::new();
        worker_states.insert(worker_id, worker_state);
        master_server.stop().await;
        Ok(PipelineState::new(worker_states))
    }
}