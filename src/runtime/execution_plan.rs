use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::api::{PipelineSpec, WorkerRuntimeSpec};
use crate::control_plane::types::ExecutionIds;
use crate::executor::placement::TaskPlacementMapping;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub execution_ids: ExecutionIds,
    pub pipeline_spec: PipelineSpec,
    pub worker_runtime: WorkerRuntimeSpec,
    pub task_placement_mapping: HashMap<String, String>,
    pub worker_task_ids: HashMap<String, Vec<String>>,
}

impl ExecutionPlan {
    pub fn from_spec(
        execution_ids: ExecutionIds,
        pipeline_spec: PipelineSpec,
        task_placement_mapping: TaskPlacementMapping,
        worker_task_ids: HashMap<String, Vec<String>>,
    ) -> Self {
        let worker_runtime = pipeline_spec.worker_runtime.clone();
        let task_placement_mapping = task_placement_mapping
            .into_iter()
            .map(|(vertex_id, cluster_node)| (vertex_id, cluster_node.node_id))
            .collect();
        Self {
            execution_ids,
            pipeline_spec,
            worker_runtime,
            task_placement_mapping,
            worker_task_ids,
        }
    }
}
