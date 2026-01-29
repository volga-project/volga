use anyhow::Result;
use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::control_plane::types::PipelineExecutionContext;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::observability::PipelineSnapshot;
use crate::api::spec::pipeline::PipelineSpec;
use crate::executor::placement::TaskPlacementStrategyName;

#[derive(Clone)]
pub struct StartAttemptRequest {
    pub pipeline_execution_context: PipelineExecutionContext,
    pub pipeline_spec: PipelineSpec,
    pub execution_graph: ExecutionGraph,
}

pub struct AttemptHandle {
    pub pipeline_execution_context: PipelineExecutionContext,
    pub master_addr: String,
    pub worker_addrs: Vec<String>,
    pub(crate) join: JoinHandle<Result<PipelineSnapshot>>,
    pub(crate) stop_sender: Option<tokio::sync::oneshot::Sender<()>>,
}

impl AttemptHandle {
    pub async fn wait(self) -> Result<PipelineSnapshot> {
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
    async fn stop_attempt(&self, handle: AttemptHandle) -> Result<()>;
    fn supported_task_placement_strategies(&self) -> &[TaskPlacementStrategyName];
}

