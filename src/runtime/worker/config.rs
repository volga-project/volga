use std::collections::HashMap;

use crate::api::spec::state::{OperatorStateBackendConfig, RequestStoreConfig, StateSpec};
use crate::common::types::PipelineId;
use crate::runtime::checkpoint::{SerializedRestore, TaskKey};
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::VertexId;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub worker_id: String,
    pub pipeline_id: PipelineId,
    pub execution_attempt_id: u64,
    pub graph: ExecutionGraph,
    pub vertex_ids: Vec<VertexId>,
    pub num_threads_per_task: usize,
    pub master_addr: Option<String>,
    pub task_restore_data: HashMap<TaskKey, SerializedRestore>,
    pub operator_state_backend: OperatorStateBackendConfig,
    pub request_store: Option<RequestStoreConfig>,
    /// When true, worker runs periodic state store maintenance.
    pub state_maintenance_enabled: bool,
    /// Cleaner tick interval when maintenance is enabled.
    pub state_maintenance_interval_ms: u64,
}

impl WorkerConfig {
    pub fn new(
        worker_id: String,
        pipeline_id: PipelineId,
        graph: ExecutionGraph,
        vertex_ids: Vec<VertexId>,
        num_threads_per_task: usize,
    ) -> Self {
        Self {
            worker_id,
            pipeline_id,
            execution_attempt_id: 0,
            graph,
            vertex_ids,
            num_threads_per_task,
            master_addr: None,
            task_restore_data: HashMap::new(),
            operator_state_backend: OperatorStateBackendConfig::default(),
            request_store: None,
            state_maintenance_enabled: true,
            state_maintenance_interval_ms: 1_000,
        }
    }

    pub fn with_master_addr(mut self, master_addr: String) -> Self {
        self.master_addr = Some(master_addr);
        self
    }

    /// Apply pipeline [`StateSpec`] backend + maintenance knobs.
    pub fn with_state_spec(mut self, state: &StateSpec) -> Self {
        self.operator_state_backend = state.operator_backend.clone();
        self.request_store = state.request_store.clone();
        self.state_maintenance_enabled = state.maintenance_enabled;
        self.state_maintenance_interval_ms = state.maintenance_interval_ms.max(1);
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
    pub last_fatal: Option<crate::runtime::health::WorkerFatalEvent>,
}
