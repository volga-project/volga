use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::VertexId;

/// Addressable worker endpoint for task placement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerEndpoint {
    pub worker_id: String,
    pub host: String,
    pub port: u16,
}

impl WorkerEndpoint {
    pub fn new(worker_id: String, host: String, port: u16) -> Self {
        Self {
            worker_id,
            host,
            port,
        }
    }
}

/// Mapping from execution task ID (vertex id) to worker endpoint.
pub type TaskPlacementMapping = HashMap<String, WorkerEndpoint>;

pub fn worker_to_task_ids(mapping: &TaskPlacementMapping) -> HashMap<String, Vec<String>> {
    let mut worker_to_task_ids = HashMap::new();
    for (task_id, worker) in mapping {
        worker_to_task_ids
            .entry(worker.worker_id.clone())
            .or_insert_with(Vec::new)
            .push(task_id.clone());
    }
    worker_to_task_ids
}

pub fn build_task_placement_mapping(
    placements: &[WorkerTaskPlacement],
    endpoints: &[WorkerEndpoint],
) -> TaskPlacementMapping {
    let mut mapping = TaskPlacementMapping::new();
    for (index, placement) in placements.iter().enumerate() {
        let endpoint = endpoints
            .get(index)
            .unwrap_or_else(|| panic!("missing worker endpoint for placement index {index}"));
        for task_id in &placement.task_ids {
            mapping.insert(task_id.as_ref().to_string(), endpoint.clone());
        }
    }
    mapping
}

#[derive(Debug, Clone)]
pub struct WorkerTaskPlacement {
    pub task_ids: Vec<VertexId>,
}

impl WorkerTaskPlacement {
    pub fn new(task_ids: Vec<VertexId>) -> Self {
        Self { task_ids }
    }
}

/// Strategy for placing execution tasks (vertices) onto logical workers.
pub trait TaskPlacementStrategy: Send + Sync {
    /// Place execution tasks onto logical workers.
    fn place_tasks(&self, execution_graph: &ExecutionGraph) -> Vec<WorkerTaskPlacement>;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskPlacementStrategyName {
    SingleNode,
    OperatorPerNode,
}

impl Default for TaskPlacementStrategyName {
    fn default() -> Self {
        Self::SingleNode
    }
}

/// Strategy that places all vertices on a single worker.
pub struct SingleNodeStrategy;

impl TaskPlacementStrategy for SingleNodeStrategy {
    fn place_tasks(&self, execution_graph: &ExecutionGraph) -> Vec<WorkerTaskPlacement> {
        let task_ids = execution_graph.get_vertices().keys().cloned().collect();
        vec![WorkerTaskPlacement::new(task_ids)]
    }
}

/// Strategy that places all vertices with the same operator_id on a single worker.
pub struct OperatorPerNodeStrategy;

impl TaskPlacementStrategy for OperatorPerNodeStrategy {
    fn place_tasks(&self, execution_graph: &ExecutionGraph) -> Vec<WorkerTaskPlacement> {
        let mut operator_to_vertices: HashMap<String, Vec<VertexId>> = HashMap::new();
        for vertex_id in execution_graph.get_vertices().keys() {
            let vertex = execution_graph
                .get_vertices()
                .get(vertex_id)
                .expect("vertex should exist");
            operator_to_vertices
                .entry(vertex.operator_id.clone())
                .or_insert_with(Vec::new)
                .push(vertex_id.clone());
        }

        operator_to_vertices
            .into_values()
            .map(WorkerTaskPlacement::new)
            .collect()
    }
}

pub fn strategy_from_name(name: &TaskPlacementStrategyName) -> Box<dyn TaskPlacementStrategy> {
    match name {
        TaskPlacementStrategyName::SingleNode => Box::new(SingleNodeStrategy),
        TaskPlacementStrategyName::OperatorPerNode => Box::new(OperatorPerNodeStrategy),
    }
}

pub fn create_test_worker_endpoints(num_workers: usize) -> Vec<WorkerEndpoint> {
    (0..num_workers)
        .map(|i| {
            WorkerEndpoint::new(
                format!("worker{}", i + 1),
                format!("192.168.1.{}", 10 + i),
                8080 + i as u16,
            )
        })
        .collect()
}
