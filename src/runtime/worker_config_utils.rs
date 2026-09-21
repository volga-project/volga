use crate::api::{compile_logical_graph, PipelineSpec};
use crate::orchestrator::orchestrator::WorkerRole;
use crate::orchestrator::task_assignment::TaskWorkerMapping;
use crate::runtime::execution_graph::ExecutionGraph;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInitPayload {
    pub worker_id: String,
    pub pipeline_id: String,
    pub pipeline_spec: PipelineSpec,
    pub vertex_ids: Vec<String>,
    pub task_worker_mapping: TaskWorkerMapping,
    #[serde(default)]
    pub role: WorkerRole,
    #[serde(default)]
    pub request_bind_address: Option<String>,
}

pub fn build_execution_graph(
    spec: &PipelineSpec,
    vertex_to_node: &TaskWorkerMapping,
) -> ExecutionGraph {
    let mut execution_graph = compile_logical_graph(spec, None).to_execution_graph();
    execution_graph.configure_channels(Some(vertex_to_node), Some(spec));
    execution_graph
}

pub fn resolve_num_threads_per_task(spec: &PipelineSpec) -> usize {
    match spec.execution_profile.clone() {
        Some(crate::api::ExecutionProfile::MasterWorker {
            num_threads_per_task,
        }) => num_threads_per_task,
        Some(crate::api::ExecutionProfile::SingleWorker {
            num_threads_per_task,
        }) => num_threads_per_task,
        None => 4,
    }
}
