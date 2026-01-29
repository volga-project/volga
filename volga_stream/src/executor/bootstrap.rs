use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::api::spec::pipeline::PipelineSpec;
use crate::control_plane::types::PipelineExecutionContext;
use crate::executor::placement::WorkerEndpoint;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MasterBootstrapPayload {
    pub pipeline_execution_context: PipelineExecutionContext,
    pub pipeline_spec: PipelineSpec,
    pub worker_endpoints: Vec<WorkerEndpoint>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerBootstrapPayload {
    pub pipeline_execution_context: PipelineExecutionContext,
    pub pipeline_spec: PipelineSpec,
    pub worker_endpoints: Vec<WorkerEndpoint>,
    pub worker_task_ids: HashMap<String, Vec<String>>,
    pub worker_id: String,
    pub assigned_task_ids: Vec<String>,
}
