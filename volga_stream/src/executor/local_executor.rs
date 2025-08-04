use crate::runtime::{
    execution_graph::ExecutionGraph,
    worker::{Worker, WorkerConfig, WorkerState},
};
use crate::transport::transport_backend_actor::TransportBackendType;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use super::executor::{Executor, ExecutionState};

/// Executes the job locally in a single process using a Worker instance
pub struct LocalExecutor {
    worker: Arc<Mutex<Option<Worker>>>,
}

impl LocalExecutor {
    pub fn new() -> Self {
        Self {
            worker: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait(?Send)]
impl Executor for LocalExecutor {
    async fn execute(&mut self, mut execution_graph: ExecutionGraph) -> Result<mpsc::Receiver<WorkerState>> {
        // Configure channels for local execution
        execution_graph.update_channels_with_node_mapping(None);

        // Get all vertex IDs from the graph
        let vertex_ids = execution_graph.get_vertices().keys().cloned().collect();

        // Create worker config
        let worker_config = WorkerConfig::new(
            execution_graph,
            vertex_ids,
            1, // Single worker ID
            TransportBackendType::InMemory,
        );

        // Create worker and store it before execution
        let worker = Worker::new(worker_config);
        
        // Store the worker before execution
        {
            let mut worker_guard = self.worker.lock().await;
            *worker_guard = Some(worker);
        }
        
        // Create channel for worker state updates
        let (sender, receiver) = mpsc::channel(1000);
        
        // Execute the worker with metrics
        let mut worker_guard = self.worker.lock().await;
        if let Some(ref mut worker) = worker_guard.as_mut() {
            worker.execute_worker_lifecycle_for_testing_with_metrics(sender).await;
        }

        Ok(receiver)
    }

    async fn get_final_execution_state(&self) -> Result<ExecutionState> {
        let worker_guard = self.worker.lock().await;
        match worker_guard.as_ref() {
            Some(worker) => {
                let state = worker.get_state().await;
                Ok(ExecutionState::new(vec![state]))
            }
            None => panic!("have you run execute() before getting state?"),
        }
    }
}