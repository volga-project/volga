use std::collections::HashMap;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::cluster::cluster_provider::ClusterNode;

/// Mapping from execution vertex ID to cluster node
pub type ExecutionVertexNodeMapping = HashMap<String, ClusterNode>;

/// Strategy for assigning execution vertices to cluster nodes
pub trait NodeAssignStrategy {
    /// Assign execution vertices to cluster nodes
    fn assign_nodes(&self, execution_graph: &ExecutionGraph, cluster_nodes: &[ClusterNode]) -> ExecutionVertexNodeMapping;
}

/// Strategy that places all vertices with the same operator_id on a single node
pub struct OperatorPerNodeStrategy;

impl NodeAssignStrategy for OperatorPerNodeStrategy {
    fn assign_nodes(&self, execution_graph: &ExecutionGraph, cluster_nodes: &[ClusterNode]) -> ExecutionVertexNodeMapping {
        let mut mapping = ExecutionVertexNodeMapping::new();
        
        if cluster_nodes.is_empty() {
            return mapping;
        }

        // Group vertices by operator_id
        let mut operator_to_vertices: HashMap<String, Vec<String>> = HashMap::new();
        
        for vertex_id in execution_graph.get_vertices().keys() {
            let vertex = execution_graph.get_vertices().get(vertex_id).expect("vertex should exist");
            let operator_id = vertex.operator_id.clone();
            operator_to_vertices.entry(operator_id).or_insert_with(Vec::new).push(vertex_id.clone());
        }

        // Assign each operator group to a cluster node
        let mut node_index = 0;
        for (operator_id, vertex_ids) in operator_to_vertices {
            let cluster_node = &cluster_nodes[node_index % cluster_nodes.len()];
            
            // Assign all vertices of this operator to the same node
            for vertex_id in vertex_ids {
                mapping.insert(vertex_id, cluster_node.clone());
            }
            
            node_index += 1;
        }

        mapping
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::cluster_provider::create_test_cluster_nodes;
    use crate::runtime::operators::operator::OperatorConfig;
    use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
    use crate::runtime::functions::map::MapFunction;
    use crate::runtime::execution_graph::{ExecutionGraph, ExecutionVertex, ExecutionEdge};
    use crate::runtime::partition::PartitionType;

    async fn create_test_execution_graph() -> ExecutionGraph {
        use crate::api::planner::{Planner, PlanningContext};
        use datafusion::execution::context::SessionContext;
        use arrow::datatypes::{Schema, Field, DataType};
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
            ]))
        );
        
        // Create logical graph from SQL
        let sql = "SELECT id FROM test_table WHERE value > 3.0";
        let logical_graph = planner.sql_to_graph(sql).await.unwrap();
        
        // Convert to execution graph
        logical_graph.to_execution_graph()
    }

    #[tokio::test]
    async fn test_operator_per_node_strategy() {
        let execution_graph = create_test_execution_graph().await;

        // Group vertices by operator_id
        let mut operator_to_vertices: HashMap<String, Vec<String>> = HashMap::new();
        for (vertex_id, vertex) in execution_graph.get_vertices() {
            operator_to_vertices
                .entry(vertex.operator_id.clone())
                .or_insert_with(Vec::new)
                .push(vertex_id.clone());
        }

        let num_operators = operator_to_vertices.len();

        let cluster_nodes = create_test_cluster_nodes(num_operators);

        let strategy = OperatorPerNodeStrategy;
        let mapping = strategy.assign_nodes(&execution_graph, &cluster_nodes);

        // Verify that all vertices are mapped
        assert_eq!(mapping.len(), execution_graph.get_vertices().len());

        // Group vertices by assigned node
        let mut node_to_vertices: HashMap<String, Vec<String>> = HashMap::new();
        for (vertex_id, cluster_node) in &mapping {
            node_to_vertices
                .entry(cluster_node.node_id.clone())
                .or_insert_with(Vec::new)
                .push(vertex_id.clone());
        }

        // Verify that each node contains vertices with the same operator_id
        for (node_id, vertex_ids) in &node_to_vertices {
            let mut operator_ids = std::collections::HashSet::new();
            for vertex_id in vertex_ids {
                let vertex = execution_graph.get_vertices().get(vertex_id).unwrap();
                operator_ids.insert(vertex.operator_id.clone());
            }
            // All vertices on the same node should have the same operator_id
            assert_eq!(operator_ids.len(), 1, "Node {} contains vertices with different operator_ids: {:?}", node_id, operator_ids);
        }

        // Verify that each operator_id maps to exactly one node
        let mut operator_to_node: HashMap<String, String> = HashMap::new();
        for (vertex_id, cluster_node) in &mapping {
            let vertex = execution_graph.get_vertices().get(vertex_id).unwrap();
            let operator_id = &vertex.operator_id;
            
            if let Some(existing_node) = operator_to_node.get(operator_id) {
                assert_eq!(existing_node, &cluster_node.node_id, 
                    "Operator {} is assigned to multiple nodes: {} and {}", 
                    operator_id, existing_node, cluster_node.node_id);
            } else {
                operator_to_node.insert(operator_id.clone(), cluster_node.node_id.clone());
            }
        }

        // Verify that all operator_ids from the execution graph are present
        let expected_operator_ids: std::collections::HashSet<_> = execution_graph.get_vertices()
            .values()
            .map(|v| v.operator_id.clone())
            .collect();
        let mapped_operator_ids: std::collections::HashSet<_> = operator_to_node.keys().cloned().collect();
        assert_eq!(expected_operator_ids, mapped_operator_ids);
    }
} 