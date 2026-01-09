use anyhow::Result;
use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::control_plane::types::ExecutionIds;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::master::PipelineState;

#[derive(Debug, Clone)]
pub struct StartAttemptRequest {
    pub execution_ids: ExecutionIds,
    pub execution_graph: ExecutionGraph,
    pub num_workers_per_operator: usize,
}

pub struct AttemptHandle {
    pub execution_ids: ExecutionIds,
    pub master_addr: String,
    pub worker_addrs: Vec<String>,
    pub(crate) join: JoinHandle<Result<PipelineState>>,
}

impl AttemptHandle {
    pub async fn wait(self) -> Result<PipelineState> {
        self.join.await?
    }

    pub fn abort(&self) {
        self.join.abort();
    }

    pub fn is_finished(&self) -> bool {
        self.join.is_finished()
    }
}

#[async_trait]
pub trait RuntimeAdapter: Send + Sync {
    async fn start_attempt(&self, req: StartAttemptRequest) -> Result<AttemptHandle>;
}

