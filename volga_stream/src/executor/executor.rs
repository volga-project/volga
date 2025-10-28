use crate::runtime::{
    execution_graph::ExecutionGraph,
    worker::WorkerState,
};
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;

/// Contains execution state information from all workers
#[derive(Debug, Clone)]
pub struct ExecutionState {
    /// States from all workers involved in the execution
    pub worker_states: Vec<WorkerState>,
}

impl ExecutionState {
    pub fn new(worker_states: Vec<WorkerState>) -> Self {
        Self { worker_states }
    }
}

/// Trait for executing a stream processing job
#[async_trait]
pub trait Executor: Send + Sync {
    /// Execute the given execution graph
    /// 
    /// # Parameters
    /// - `execution_graph`: The graph to execute
    /// - `state_sender`: Optional sender for broadcasting worker states during execution
    /// 
    /// # Returns
    /// The final execution state after completion
    async fn execute(
        &mut self, 
        execution_graph: ExecutionGraph, 
        state_sender: Option<mpsc::Sender<WorkerState>>
    ) -> Result<ExecutionState>;
}