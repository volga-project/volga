use crate::runtime::{
    execution_graph::ExecutionGraph, master::PipelineState
};
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;

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
        state_updates_sender: Option<mpsc::Sender<PipelineState>>
    ) -> Result<PipelineState>;
}