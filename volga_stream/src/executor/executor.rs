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
#[async_trait(?Send)]
pub trait Executor: Send {
    /// Execute the given execution graph and return a channel receiver for worker states
    async fn execute(&mut self, execution_graph: ExecutionGraph) -> Result<mpsc::Receiver<WorkerState>>;

    /// Get the final execution state after completion
    async fn get_final_execution_state(&self) -> Result<ExecutionState>;
}