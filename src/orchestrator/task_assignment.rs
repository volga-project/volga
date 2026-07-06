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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::orchestrator::mock_worker_nodes;
    use crate::runtime::execution_graph::ExecutionGraph;
    use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};

    fn create_test_execution_graph() -> ExecutionGraph {
        use crate::api::planner::{Planner, PlanningContext};
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::execution::context::SessionContext;
        use std::sync::Arc;

        let ctx = SessionContext::new();
        let mut planner = Planner::new(PlanningContext::new(ctx));

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
        let execution_graph = create_test_execution_graph();

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
}
