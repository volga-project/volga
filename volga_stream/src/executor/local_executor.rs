use crate::runtime::{
    execution_graph::ExecutionGraph,
    worker::{Worker, WorkerConfig, WorkerState},
};
use crate::transport::transport_backend_actor::TransportBackendType;
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;
use super::executor::{Executor, ExecutionState};

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
        state_sender: Option<mpsc::Sender<WorkerState>>
    ) -> Result<ExecutionState> {
        // Configure channels for local execution
        execution_graph.update_channels_with_node_mapping(None);

        // Get all vertex IDs from the graph
        let vertex_ids = execution_graph.get_vertices().keys().cloned().collect();

        // Create worker config
        let worker_config = WorkerConfig::new(
            "local_worker".to_string(),
            execution_graph,
            vertex_ids,
            1,
            TransportBackendType::InMemory,
        );

        // Create worker
        let mut worker = Worker::new(worker_config);
        
        // Execute the worker
        if let Some(sender) = state_sender {
            worker.execute_worker_lifecycle_for_testing_with_metrics(sender).await;
        } else {
            worker.execute_worker_lifecycle_for_testing().await;
        }

        // Get final state
        let worker_state = worker.get_state().await;
        worker.close().await;
        Ok(ExecutionState::new(vec![worker_state]))
    }
}