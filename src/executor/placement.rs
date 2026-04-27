use std::collections::HashMap;

use crate::cluster::cluster_provider::ClusterNode;
use crate::cluster::node_assignment::{
    node_to_vertex_ids, ExecutionVertexNodeMapping, NodeAssignStrategy, SingleWorkerStrategy,
};
use crate::runtime::execution_graph::ExecutionGraph;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskPlacementStrategyName {
    SingleWorker,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerEndpoint {
    pub worker_id: String,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerTaskPlacement {
    pub endpoint: WorkerEndpoint,
    pub task_ids: Vec<String>,
}

pub type TaskPlacementMapping = ExecutionVertexNodeMapping;

pub fn strategy_from_name(name: TaskPlacementStrategyName) -> Box<dyn NodeAssignStrategy> {
    match name {
        TaskPlacementStrategyName::SingleWorker => Box::new(SingleWorkerStrategy),
    }
}

pub fn build_task_placement_mapping(
    execution_graph: &ExecutionGraph,
    strategy: &dyn NodeAssignStrategy,
    cluster_nodes: &[ClusterNode],
) -> TaskPlacementMapping {
    strategy.assign_nodes(execution_graph, cluster_nodes)
}

pub fn worker_to_task_ids(mapping: &TaskPlacementMapping) -> HashMap<String, Vec<String>> {
    node_to_vertex_ids(mapping)
}
