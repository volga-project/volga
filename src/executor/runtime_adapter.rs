use anyhow::Result;
use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::control_plane::types::ExecutionIds;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::observability::PipelineSnapshot;
use crate::api::spec::runtime_adapter::RuntimeAdapterKind;
use crate::api::spec::pipeline::PipelineSpec;
use crate::executor::placement::TaskPlacementStrategyName;
use crate::api::WorkerRuntimeSpec;
use crate::api::StorageSpec;

#[derive(Clone)]
pub struct StartAttemptRequest {
    pub execution_ids: ExecutionIds,
    pub pipeline_spec: PipelineSpec,
    pub execution_graph: ExecutionGraph,
    // TODO do we need this? Why is it not inside worker_runtime.transport?
    pub transport_overrides_queue_records: std::collections::HashMap<String, u32>,
    
    // TODO why is it on request? How does it map to WorkerConfig?
    pub worker_runtime: WorkerRuntimeSpec,
    // TODO do we need this? Why is it not inside worker_runtime.storage?
    pub operator_type_storage_overrides: std::collections::HashMap<String, StorageSpec>,
}

pub struct AttemptHandle {
    pub execution_ids: ExecutionIds,
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
    fn runtime_kind(&self) -> RuntimeAdapterKind;
}

