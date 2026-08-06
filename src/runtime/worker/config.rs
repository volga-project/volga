use std::collections::HashMap;

use crate::api::spec::state::{OperatorStateBackendConfig, RequestStoreConfig};
use crate::common::types::PipelineId;
use crate::runtime::checkpoint::{SerializedRestore, TaskKey};
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::VertexId;
use crate::transport::transport_backend_actor::TransportBackendType;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub worker_id: String,
    pub pipeline_id: PipelineId,
    pub execution_attempt_id: u64,
    pub graph: ExecutionGraph,
    pub vertex_ids: Vec<VertexId>,
    pub num_threads_per_task: usize,
    pub transport_backend_type: TransportBackendType,
    pub master_addr: Option<String>,
    pub task_restore_data: HashMap<TaskKey, SerializedRestore>,
    pub operator_state_backend: OperatorStateBackendConfig,
    pub request_store: Option<RequestStoreConfig>,
}

impl WorkerConfig {
    pub fn new(
        worker_id: String,
        pipeline_id: PipelineId,
        graph: ExecutionGraph,
        vertex_ids: Vec<VertexId>,
        num_threads_per_task: usize,
        transport_backend_type: TransportBackendType,
    ) -> Self {
        Self {
            worker_id,
            pipeline_id,
            execution_attempt_id: 0,
            graph,
            vertex_ids,
            num_threads_per_task,
            transport_backend_type,
            master_addr: None,
            task_restore_data: HashMap::new(),
            operator_state_backend: OperatorStateBackendConfig::default(),
            request_store: None,
        }
    }

    pub fn with_master_addr(mut self, master_addr: String) -> Self {
        self.master_addr = Some(master_addr);
        self
    }
}

/// Snapshot of worker attempt binding (actor is source of truth).
#[derive(Debug, Clone)]
pub struct WorkerIdentity {
    pub worker_id: String,
    pub pipeline_id: Option<String>,
    pub execution_attempt_id: u64,
    pub configured: bool,
}
