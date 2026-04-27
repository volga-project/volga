use serde::{Deserialize, Serialize};

use crate::control_plane::types::ExecutionIds;
use crate::runtime::execution_plan::ExecutionPlan;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MasterBootstrapPayload {
    pub execution_ids: ExecutionIds,
    pub plan: ExecutionPlan,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerBootstrapPayload {
    pub execution_ids: ExecutionIds,
    pub plan: ExecutionPlan,
    pub worker_id: String,
    pub assigned_task_ids: Vec<String>,
}
