use crate::orchestrator::orchestrator::WorkerNode;
use crate::runtime::execution_graph::ExecutionGraph;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Mapping from execution vertex ID (task ID) to worker node
pub type TaskWorkerMapping = HashMap<String, WorkerNode>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum TaskWorkerAssignmentStrategyType {
    SingleWorker,
    OperatorPerWorker,
    /// Flink-like slot scheduling: each task-index slice across all operators
    /// is placed into one slot on one worker; `slots_per_node` controls slices per worker.
    Pipelined { slots_per_node: usize },
}

// inverse mapping
pub fn worker_to_tasks(mapping: &TaskWorkerMapping) -> HashMap<String, Vec<String>> {
    let mut worker_to_tasks = HashMap::new();
    for (vertex_id, node) in mapping {
        worker_to_tasks
            .entry(node.worker_id.clone())
            .or_insert_with(Vec::new)
            .push(vertex_id.clone());
    }
    worker_to_tasks
}

/// Strategy for assigning execution vertices to cluster nodes
pub trait TaskWorkerAssignStrategy: Send + Sync {
    /// Assign execution vertices to cluster nodes
    fn assign_tasks(
        &self,
        execution_graph: &ExecutionGraph,
        nodes: &[WorkerNode],
    ) -> TaskWorkerMapping;
}

pub fn assign_tasks_with_strategy(
    strategy: &TaskWorkerAssignmentStrategyType,
    execution_graph: &ExecutionGraph,
    nodes: &[WorkerNode],
) -> TaskWorkerMapping {
    match strategy {
        // TODO add pipeline first strategy
        TaskWorkerAssignmentStrategyType::SingleWorker => {
            SingleWorkerStrategy.assign_tasks(execution_graph, nodes)
        }
        TaskWorkerAssignmentStrategyType::OperatorPerWorker => {
            OperatorPerWorkerStrategy.assign_tasks(execution_graph, nodes)
        }
        TaskWorkerAssignmentStrategyType::Pipelined { slots_per_node } => {
            PipelinedStrategy {
                slots_per_node: *slots_per_node,
            }
            .assign_tasks(execution_graph, nodes)
        }
    }
}

/// Strategy that assigns all vertices to the first (single) worker node.
pub struct SingleWorkerStrategy;

impl TaskWorkerAssignStrategy for SingleWorkerStrategy {
    fn assign_tasks(
        &self,
        execution_graph: &ExecutionGraph,
        nodes: &[WorkerNode],
    ) -> TaskWorkerMapping {
        let mut mapping = TaskWorkerMapping::new();
        if nodes.is_empty() {
            return mapping;
        }
        let node = nodes[0].clone();
        for vertex_id in execution_graph.get_vertices().keys() {
            mapping.insert(vertex_id.as_ref().to_string(), node.clone());
        }
        mapping
    }
}

/// Strategy that places all vertices with the same operator_id on a single node
pub struct OperatorPerWorkerStrategy;

impl TaskWorkerAssignStrategy for OperatorPerWorkerStrategy {
    fn assign_tasks(
        &self,
        execution_graph: &ExecutionGraph,
        nodes: &[WorkerNode],
    ) -> TaskWorkerMapping {
        let mut mapping = TaskWorkerMapping::new();

        if nodes.is_empty() {
            return mapping;
        }

        // Group vertices by operator_id
        let mut operator_to_vertices: HashMap<String, Vec<String>> = HashMap::new();

        for vertex_id in execution_graph.get_vertices().keys() {
            let vertex = execution_graph
                .get_vertices()
                .get(vertex_id)
                .expect("vertex should exist");
            let operator_id = vertex.operator_id.clone();
            operator_to_vertices
                .entry(operator_id)
                .or_insert_with(Vec::new)
                .push(vertex_id.as_ref().to_string());
        }

        // Check if we have enough nodes for all operators
        if operator_to_vertices.len() > nodes.len() {
            panic!(
                "Not enough worker nodes ({}) to assign all operators ({})",
                nodes.len(),
                operator_to_vertices.len()
            );
        }

        // Assign each operator group to a worker node
        let mut node_index = 0;
        for (_operator_id, vertex_ids) in operator_to_vertices {
            let worker_node = &nodes[node_index % nodes.len()];

            // Assign all vertices of this operator to the same node
            for vertex_id in vertex_ids {
                mapping.insert(vertex_id, worker_node.clone());
            }

            node_index += 1;
        }

        mapping
    }
}

/// Flink-like pipelined strategy:
/// place one end-to-end task slice (same task_index across operators) into one slot,
/// and place slots on workers in node-filling order.
pub struct PipelinedStrategy {
    pub slots_per_node: usize,
}

impl TaskWorkerAssignStrategy for PipelinedStrategy {
    fn assign_tasks(
        &self,
        execution_graph: &ExecutionGraph,
        nodes: &[WorkerNode],
    ) -> TaskWorkerMapping {
        let mut mapping = TaskWorkerMapping::new();

        if nodes.is_empty() {
            return mapping;
        }
        if self.slots_per_node == 0 {
            panic!("Pipelined strategy requires slots_per_node > 0");
        }

        let mut unique_parallelism: Vec<i32> = execution_graph
            .get_vertices()
            .values()
            .map(|v| v.parallelism)
            .collect();
        unique_parallelism.sort_unstable();
        unique_parallelism.dedup();

        if unique_parallelism.is_empty() {
            return mapping;
        }
        if unique_parallelism.len() != 1 {
            panic!(
                "Pipelined strategy requires identical parallelism across execution graph, got {:?}",
                unique_parallelism
            );
        }

        let graph_parallelism = unique_parallelism[0];
        if graph_parallelism <= 0 {
            panic!(
                "Pipelined strategy requires positive parallelism, got {}",
                graph_parallelism
            );
        }
        let graph_parallelism = graph_parallelism as usize;

        let total_slots = nodes.len() * self.slots_per_node;
        if total_slots < graph_parallelism {
            panic!(
                "Not enough worker slots ({}) for graph parallelism ({})",
                total_slots, graph_parallelism
            );
        }

        for (vertex_id, vertex) in execution_graph.get_vertices() {
            if vertex.parallelism as usize != graph_parallelism {
                panic!(
                    "Pipelined strategy requires vertex {} to have parallelism {}, got {}",
                    vertex_id, graph_parallelism, vertex.parallelism
                );
            }
            if vertex.task_index < 0 {
                panic!(
                    "Pipelined strategy requires non-negative task_index, got {} for vertex {}",
                    vertex.task_index, vertex_id
                );
            }
            let task_index = vertex.task_index as usize;
            if task_index >= graph_parallelism {
                panic!(
                    "Pipelined strategy requires task_index < parallelism ({}), got {} for vertex {}",
                    graph_parallelism, task_index, vertex_id
                );
            }

            let node_index = task_index / self.slots_per_node;
            let worker_node = nodes
                .get(node_index)
                .expect("worker node should exist for computed slot");
            mapping.insert(vertex_id.as_ref().to_string(), worker_node.clone());
        }

        mapping
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::orchestrator::mock_worker_nodes;
    use crate::runtime::execution_graph::ExecutionGraph;
    use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};

    fn create_test_execution_graph(parallelism: usize) -> ExecutionGraph {
        use crate::api::planner::{Planner, PlanningContext};
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::execution::context::SessionContext;
        use std::sync::Arc;

        let ctx = SessionContext::new();
        let mut planner = Planner::new(PlanningContext::new(ctx).with_parallelism(parallelism));

        // Register test table
        planner.register_source(
            "test_table".to_string(),
            SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])),
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Float64, false),
            ])),
        );

        // Create logical graph from SQL
        let sql = "SELECT id FROM test_table WHERE value > 3.0";
        let logical_graph = planner.sql_to_graph(sql).unwrap();

        // Convert to execution graph
        logical_graph.to_execution_graph()
    }

    #[tokio::test]
    async fn test_operator_per_worker_strategy() {
        let execution_graph = create_test_execution_graph(1);

        // Group vertices by operator_id
        let mut operator_to_vertices: HashMap<String, Vec<String>> = HashMap::new();
        for (vertex_id, vertex) in execution_graph.get_vertices() {
            operator_to_vertices
                .entry(vertex.operator_id.clone())
                .or_insert_with(Vec::new)
                .push(vertex_id.as_ref().to_string());
        }

        let num_operators = operator_to_vertices.len();

        let nodes = mock_worker_nodes(num_operators);

        let strategy = OperatorPerWorkerStrategy;
        let mapping = strategy.assign_tasks(&execution_graph, &nodes);

        // Verify that all vertices are mapped
        assert_eq!(mapping.len(), execution_graph.get_vertices().len());

        // Group vertices by assigned node
        let mut node_to_vertices: HashMap<String, Vec<String>> = HashMap::new();
        for (vertex_id, node) in &mapping {
            node_to_vertices
                .entry(node.worker_id.clone())
                .or_insert_with(Vec::new)
                .push(vertex_id.clone());
        }

        // Verify that each node contains vertices with the same operator_id
        for (node_id, vertex_ids) in &node_to_vertices {
            let mut operator_ids = std::collections::HashSet::new();
            for vertex_id in vertex_ids {
                let vertex = execution_graph
                    .get_vertices()
                    .get(vertex_id.as_str())
                    .unwrap();
                operator_ids.insert(vertex.operator_id.clone());
            }
            // All vertices on the same node should have the same operator_id
            assert_eq!(
                operator_ids.len(),
                1,
                "Node {} contains vertices with different operator_ids: {:?}",
                node_id,
                operator_ids
            );
        }

        // Verify that each operator_id maps to exactly one node
        let mut operator_to_worker: HashMap<String, String> = HashMap::new();
        for (vertex_id, node) in &mapping {
            let vertex = execution_graph
                .get_vertices()
                .get(vertex_id.as_str())
                .unwrap();
            let operator_id = &vertex.operator_id;

            if let Some(existing_node) = operator_to_worker.get(operator_id) {
                assert_eq!(
                    existing_node, &node.worker_id,
                    "Operator {} is assigned to multiple nodes: {} and {}",
                    operator_id, existing_node, node.worker_id
                );
            } else {
                operator_to_worker.insert(operator_id.clone(), node.worker_id.clone());
            }
        }

        // Verify that all operator_ids from the execution graph are present
        let expected_operator_ids: std::collections::HashSet<_> = execution_graph
            .get_vertices()
            .values()
            .map(|v| v.operator_id.clone())
            .collect();
        let mapped_operator_ids: std::collections::HashSet<_> =
            operator_to_worker.keys().cloned().collect();
        assert_eq!(expected_operator_ids, mapped_operator_ids);
    }

    #[tokio::test]
    async fn test_pipelined_strategy_assigns_slices_to_slots() {
        let parallelism = 4usize;
        let execution_graph = create_test_execution_graph(parallelism);
        let nodes = mock_worker_nodes(2);
        let slots_per_node = 2usize;
        let strategy = PipelinedStrategy { slots_per_node };

        let mapping = strategy.assign_tasks(&execution_graph, &nodes);
        assert_eq!(mapping.len(), execution_graph.get_vertices().len());

        // Every task_index slice (across operators) must land on one worker.
        let mut task_index_to_worker: HashMap<i32, String> = HashMap::new();
        for (vertex_id, node) in &mapping {
            let vertex = execution_graph
                .get_vertices()
                .get(vertex_id.as_str())
                .expect("vertex should exist");
            if let Some(existing_worker) = task_index_to_worker.get(&vertex.task_index) {
                assert_eq!(
                    existing_worker, &node.worker_id,
                    "task_index {} assigned to multiple workers",
                    vertex.task_index
                );
            } else {
                task_index_to_worker.insert(vertex.task_index, node.worker_id.clone());
            }

            let expected_worker = &nodes[(vertex.task_index as usize) / slots_per_node].worker_id;
            assert_eq!(
                &node.worker_id, expected_worker,
                "task_index {} expected on worker {}",
                vertex.task_index, expected_worker
            );
        }
        assert_eq!(
            task_index_to_worker.len(),
            parallelism,
            "all parallel slices should be assigned"
        );
    }

}
