use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::executor::placement::WorkerEndpoint;
use crate::api::spec::pipeline::PipelineSpec;
use crate::api::WorkerRuntimeSpec;
use crate::control_plane::types::ExecutionIds;
use crate::api::StorageSpec;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerBootstrapPayload {
    pub execution_ids: ExecutionIds,
    pub pipeline_spec: PipelineSpec,
    pub worker_endpoints: Vec<WorkerEndpoint>,
    pub worker_task_ids: HashMap<String, Vec<String>>,
    pub transport_overrides_queue_records: HashMap<String, u32>,
    pub worker_runtime: WorkerRuntimeSpec,
    pub operator_type_storage_overrides: HashMap<String, StorageSpec>,
}
