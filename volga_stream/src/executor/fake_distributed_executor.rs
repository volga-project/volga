use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::master::PipelineState;
use crate::control_plane::types::{AttemptId, ExecutionIds};
use crate::executor::local_runtime_adapter::LocalRuntimeAdapter;
use crate::executor::runtime_adapter::{RuntimeAdapter, StartAttemptRequest};
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;
use super::executor::Executor;

/// Emulates distributed execution by creating multiple worker servers and a master
pub struct FakeDistributedExecutor {
    num_workers_per_operator: usize,
}

impl FakeDistributedExecutor {
    pub fn new(num_workers_per_operator: usize) -> Self {
        Self {
            num_workers_per_operator,
        }
    }
}

#[async_trait]
impl Executor for FakeDistributedExecutor {
    async fn execute(
        &mut self, 
        execution_graph: ExecutionGraph, 
        _state_updates_sender: Option<mpsc::Sender<PipelineState>>
    ) -> Result<PipelineState> {
        let execution_ids = ExecutionIds::fresh(AttemptId(1));
        let adapter = LocalRuntimeAdapter::new();
        let handle = adapter
            .start_attempt(StartAttemptRequest {
                execution_ids,
                execution_graph,
                num_workers_per_operator: self.num_workers_per_operator,
            })
            .await?;
        handle.wait().await
    }
}