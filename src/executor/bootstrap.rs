use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::api::spec::pipeline::PipelineSpec;
use crate::control_plane::types::PipelineExecutionContext;
use crate::executor::placement::{TaskPlacementMapping, WorkerEndpoint};
use crate::api::spec::worker_runtime::WorkerRuntimeSpec;
use crate::runtime::execution_plan::ExecutionPlan;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionPlanPayload {
    pub pipeline_spec: PipelineSpec,
    pub worker_endpoints: Vec<WorkerEndpoint>,
    pub task_placement_mapping: TaskPlacementMapping,
    pub worker_task_ids: HashMap<String, Vec<String>>,
    pub worker_runtime: WorkerRuntimeSpec,
}

impl ExecutionPlanPayload {
    pub fn from_plan(plan: &ExecutionPlan) -> Self {
        Self {
            pipeline_spec: plan.pipeline_spec.clone(),
            worker_endpoints: plan.worker_endpoints.clone(),
            task_placement_mapping: plan.task_placement_mapping.clone(),
            worker_task_ids: plan.worker_task_ids.clone(),
            worker_runtime: plan.worker_runtime.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MasterBootstrapPayload {
    pub pipeline_execution_context: PipelineExecutionContext,
    pub plan: ExecutionPlanPayload,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerBootstrapPayload {
    pub pipeline_execution_context: PipelineExecutionContext,
    pub plan: ExecutionPlanPayload,
    pub worker_id: String,
    pub assigned_task_ids: Vec<String>,
}
