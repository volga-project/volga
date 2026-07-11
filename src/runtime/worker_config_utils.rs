use crate::api::{compile_logical_graph, PipelineSpec};
use crate::orchestrator::task_assignment::TaskWorkerMapping;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::transport::channel::Channel;
use crate::transport::transport_backend_actor::TransportBackendType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInitPayload {
    pub worker_id: String,
    pub pipeline_id: String,
    pub pipeline_spec: PipelineSpec,
    pub vertex_ids: Vec<String>,
    pub task_worker_mapping: TaskWorkerMapping,
    /// If set, restore operator state from this completed checkpoint on (re)configure.
    #[serde(default)]
    pub restore_checkpoint_id: Option<u64>,
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
        Some(crate::api::ExecutionProfile::MasterWorker { num_threads_per_task }) => num_threads_per_task,
        Some(crate::api::ExecutionProfile::SingleWorker { num_threads_per_task }) => num_threads_per_task,
        None => 4,
    }
}

pub fn resolve_transport_backend_type(
    execution_graph: &ExecutionGraph,
    vertex_ids: &[String],
) -> TransportBackendType {
    let has_remote_channels = vertex_ids.iter().any(|vertex_id| {
        if let Some((in_edges, out_edges)) = execution_graph.get_edges_for_vertex(vertex_id) {
            in_edges
                .iter()
                .chain(out_edges.iter())
                .any(|e| matches!(e.channel.as_ref(), Some(Channel::Remote { .. })))
        } else {
            false
        }
    });

    if has_remote_channels {
        TransportBackendType::Grpc
    } else {
        TransportBackendType::InMemory
    }
}
