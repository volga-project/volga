use std::collections::HashMap;
use std::sync::Arc;
use std::fmt;
use arrow::datatypes::Schema as ArrowSchema;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::prelude::EdgeRef;
use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::execution_graph::{ExecutionGraph, ExecutionVertex, ExecutionEdge};
use crate::runtime::partition::PartitionType;
use crate::transport::channel::Channel;
use crate::cluster::cluster_provider::ClusterNode;

#[derive(Debug, Clone)]
pub struct LogicalNode {
    pub operator_id: String,
    pub operator_config: OperatorConfig,
    pub parallelism: usize,
    pub in_schema: Option<Arc<ArrowSchema>>,
    pub out_schema: Option<Arc<ArrowSchema>>,
}

impl LogicalNode {
    pub fn new(operator_config: OperatorConfig, parallelism: usize, in_schema: Option<Arc<ArrowSchema>>, out_schema: Option<Arc<ArrowSchema>>) -> Self {
        Self {
            operator_id: String::new(), // Will be set by add_node
            operator_config,
            parallelism,
            in_schema,
            out_schema,
        }
    }
} 
#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

/// Connector configuration for sources and sinks
#[derive(Debug, Clone)]
pub struct ConnectorConfig {
    pub connector_type: String,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum EdgeType {
    Forward,
    Shuffle,
    Broadcast,
}

#[derive(Debug, Clone)]
pub struct LogicalEdge {
    pub edge_type: EdgeType,
}

#[derive(Debug, Clone)]
pub struct LogicalGraph {
    graph: DiGraph<LogicalNode, LogicalEdge>,
    operator_type_counters: HashMap<String, u32>,
}

impl LogicalGraph {
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            operator_type_counters: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, mut node: LogicalNode) -> NodeIndex {
        // Generate unique operator_id based on operator type name
        let operator_type_name = format!("{}", node.operator_config);
        let counter = self.operator_type_counters.entry(operator_type_name.clone()).or_insert(0);
        *counter += 1;
        node.operator_id = format!("{}_{}", operator_type_name, counter);
        
        let node_index = self.graph.add_node(node);
        node_index
    }

    pub fn add_edge(&mut self, source: NodeIndex, target: NodeIndex, edge_type: EdgeType) -> petgraph::graph::EdgeIndex {
        let edge = LogicalEdge {
            edge_type,
        };
        
        self.graph.add_edge(source, target, edge)
    }

    pub fn get_node(&self, operator_id: String) -> Option<&LogicalNode> {
        self.graph.node_weights().find(|node| node.operator_id == operator_id)
    }

    pub fn get_nodes(&self) -> impl Iterator<Item = &LogicalNode> {
        self.graph.node_weights()
    }

    pub fn get_edges(&self) -> impl Iterator<Item = (NodeIndex, NodeIndex, &LogicalEdge)> {
        self.graph.edge_references().map(|edge| (edge.source(), edge.target(), edge.weight()))
    }

    /// Convert logical graph to execution graph using parallelism from each logical node
    pub fn to_execution_graph(&self) -> ExecutionGraph {
        let mut execution_graph = ExecutionGraph::new();
        let mut logical_to_execution_mapping: HashMap<NodeIndex, Vec<String>> = HashMap::new();

        // Create execution vertices for each logical node
        for logical_node in self.graph.node_weights() {
            let logical_node_index = self.graph.node_indices()
                .find(|&idx| self.graph[idx].operator_id == logical_node.operator_id)
                .unwrap();

            let mut execution_vertex_ids = Vec::new();
            let parallelism = logical_node.parallelism;

            // Create parallel execution vertices for this logical node
            for i in 0..parallelism {
                let execution_vertex_id = format!("{}_{}", logical_node.operator_id, i);

                let execution_vertex = ExecutionVertex::new(
                    execution_vertex_id.clone(),
                    logical_node.operator_id.clone(),
                    logical_node.operator_config.clone(),
                    parallelism as i32,
                    i as i32,
                );

                execution_graph.add_vertex(execution_vertex);
                execution_vertex_ids.push(execution_vertex_id);
            }

            logical_to_execution_mapping.insert(logical_node_index, execution_vertex_ids);
        }

        // Create execution edges for each logical edge (without channels)
        for edge in self.graph.edge_references() {
            let source_logical_node_index = edge.source();
            let target_logical_node_index = edge.target();
            let logical_edge = edge.weight();

            let source_execution_vertices = &logical_to_execution_mapping[&source_logical_node_index];
            let target_execution_vertices = &logical_to_execution_mapping[&target_logical_node_index];

            // Determine partition type based on logical edge type
            // TODO introduce forward partition and use it if parallelism of sequential operators is equal
            let partition_type = match logical_edge.edge_type {
                EdgeType::Forward => PartitionType::RoundRobin,
                EdgeType::Shuffle => PartitionType::Hash,
                EdgeType::Broadcast => PartitionType::Broadcast,
            };

            // Connect each source execution vertex to each target execution vertex
            for source_execution_vertex_id in source_execution_vertices {
                for target_execution_vertex_id in target_execution_vertices {
                    let execution_edge = ExecutionEdge::new(
                        source_execution_vertex_id.clone(),
                        target_execution_vertex_id.clone(),
                        self.graph[target_logical_node_index].operator_id.clone(),
                        partition_type.clone(),
                        None, // No channel initially
                    );

                    execution_graph.add_edge(execution_edge);
                }
            }
        }

        execution_graph
    }




    /// Generate DOT format string
    pub fn to_dot(&self) -> String {
        let mut dot_string = String::from("digraph LogicalGraph {\n");
        
        // Add nodes with labels
        for node in self.graph.node_weights() {
            dot_string.push_str(&format!("  {} [label=\"{}: {}\"];\n", node.operator_id, node.operator_id, node.operator_config));
        }
        
        // Add edges with labels
        for edge in self.graph.edge_references() {
            let source_id = self.graph[edge.source()].operator_id.clone();
            let target_id = self.graph[edge.target()].operator_id.clone();
            let edge_type = match edge.weight().edge_type {
                EdgeType::Forward => "Forward",
                EdgeType::Shuffle => "Shuffle",
                EdgeType::Broadcast => "Broadcast",
            };
            dot_string.push_str(&format!("  {} -> {} [label=\"{}\"];\n", source_id, target_id, edge_type));
        }
        
        dot_string.push_str("}\n");
        dot_string
    }
}

impl fmt::Display for LogicalGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_dot())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::cluster_provider::create_test_cluster_nodes;
    use crate::cluster::node_assignment::{NodeAssignStrategy, OperatorPerNodeStrategy};
    use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
    use crate::runtime::functions::map::{MapFunction, ProjectionFunction};
    use crate::runtime::functions::map::filter_function::FilterFunction;
    use datafusion::common::{DFSchema, DFSchemaRef};
    use datafusion::execution::context::SessionContext;
    use arrow::datatypes::{Schema, Field, DataType};
    use datafusion::prelude::{col, lit};
    use std::sync::Arc;

    #[test]
    fn test_logical_to_execution_graph_conversion() {
        let mut logical_graph = LogicalGraph::new();

        // Create test schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create logical nodes: source -> filter -> projection -> sink
        let source_config = SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![]));
        let source_node = LogicalNode::new(
            OperatorConfig::SourceConfig(source_config),
            2, // source parallelism
            None,
            None,
        );
        let source_index = logical_graph.add_node(source_node);

        let filter_function = FilterFunction::new(
            DFSchemaRef::from(DFSchema::try_from(schema.clone()).unwrap()),
            col("id").gt(lit(0)),
            SessionContext::new(),
        );
        let filter_node = LogicalNode::new(
            OperatorConfig::MapConfig(MapFunction::Filter(filter_function)),
            3, // filter parallelism
            None,
            None,
        );
        let filter_index = logical_graph.add_node(filter_node);

        let projection_function = ProjectionFunction::new(
            DFSchemaRef::from(DFSchema::try_from(schema.clone()).unwrap()),
            DFSchemaRef::from(DFSchema::try_from(schema.clone()).unwrap()),
            vec![col("id"), col("name")],
            SessionContext::new(),
        );
        let projection_node = LogicalNode::new(
            OperatorConfig::MapConfig(MapFunction::Projection(projection_function)),
            1, // projection parallelism
            None,
            None,
        );
        let projection_index = logical_graph.add_node(projection_node);

        // Add sink node
        let sink_config = crate::runtime::operators::sink::sink_operator::SinkConfig::InMemoryStorageGrpcSinkConfig("http://127.0.0.1:8080".to_string());
        let sink_node = LogicalNode::new(
            OperatorConfig::SinkConfig(sink_config),
            1, // sink parallelism
            None,
            None,
        );
        let sink_index = logical_graph.add_node(sink_node);

        // Add edges
        logical_graph.add_edge(source_index, filter_index, EdgeType::Forward);
        logical_graph.add_edge(filter_index, projection_index, EdgeType::Forward);
        logical_graph.add_edge(projection_index, sink_index, EdgeType::Forward);

        // Convert to execution graph
        let execution_graph = logical_graph.to_execution_graph();

        // Verify execution vertices
        let vertices = execution_graph.get_vertices();
        assert_eq!(vertices.len(), 7); // 2 + 3 + 1 + 1 = 7 vertices total

        // Verify execution edges
        let edges = execution_graph.get_edges();
        assert_eq!(edges.len(), 10); // 2 source * 3 filter + 3 filter * 1 projection + 1 projection * 1 sink = 10 edges

        // Verify partition types
        for edge in edges.values() {
            assert!(matches!(edge.partition_type, PartitionType::RoundRobin),
                "Edge {} -> {} should use RoundRobin partitioning", edge.source_vertex_id, edge.target_vertex_id);
            // Verify channels are not set initially
            assert!(edge.channel.is_none(), "Edge {} -> {} should not have channel set initially", edge.source_vertex_id, edge.target_vertex_id);
        }
    }

    #[test]
    fn test_logical_to_execution_graph_with_cluster() {
        let mut logical_graph = LogicalGraph::new();
        let parallelism = 2;

        // Create a simple logical graph: source -> filter
        let source_config = SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![]));
        let source_node = LogicalNode::new(
            OperatorConfig::SourceConfig(source_config),
            parallelism,
            None,
            None,
        );
        let source_index = logical_graph.add_node(source_node);

        let filter_function = FilterFunction::new(
            DFSchemaRef::from(DFSchema::try_from(
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
            ).unwrap()),
            col("id").gt(lit(0)),
            SessionContext::new(),
        );
        let filter_node = LogicalNode::new(
            OperatorConfig::MapConfig(MapFunction::Filter(filter_function)),
            parallelism,
            None,
            None,
        );
        let filter_index = logical_graph.add_node(filter_node);

        logical_graph.add_edge(source_index, filter_index, EdgeType::Forward);

        let num_operators = logical_graph.get_nodes().count();

        // Convert to execution graph
        let mut execution_graph = logical_graph.to_execution_graph();
        
        // Create cluster nodes
        let cluster_nodes = create_test_cluster_nodes(num_operators);

        // Use OperatorPerNodeStrategy to assign vertices to nodes
        let vertex_to_node = OperatorPerNodeStrategy.assign_nodes(&execution_graph, &cluster_nodes);

        // Update channels based on cluster mapping
        execution_graph.update_channels_with_node_mapping(Some(&vertex_to_node));

        // Verify execution vertices
        let vertices = execution_graph.get_vertices();
        assert_eq!(vertices.len(), num_operators * parallelism); // 2 logical nodes * 2 parallelism

        // Verify execution edges
        let edges = execution_graph.get_edges();
        assert_eq!(edges.len(), num_operators * num_operators); // 2 source * 2 target

        // Verify that edges between different nodes use remote channels
        for edge in edges.values() {
            // Get source and target nodes from the mapping
            let source_node = vertex_to_node.get(&edge.source_vertex_id).expect("Source vertex should be mapped");
            let target_node = vertex_to_node.get(&edge.target_vertex_id).expect("Target vertex should be mapped");
            
            // Check if vertices are on different nodes
            if source_node.node_id != target_node.node_id {
                // Vertices are on different nodes, should use remote channel
                match &edge.channel {
                    Some(Channel::Remote { source_node_ip, target_node_ip, target_port, .. }) => {
                        assert_eq!(source_node_ip, &source_node.node_ip);
                        assert_eq!(target_node_ip, &target_node.node_ip);
                        assert_eq!(*target_port, target_node.node_port as i32);
                    }
                    Some(Channel::Local { .. }) => {
                        panic!("Expected remote channel for cross-node edge {} -> {}", 
                               edge.source_vertex_id, edge.target_vertex_id);
                    }
                    None => {
                        panic!("Expected channel to be set for edge {} -> {}", 
                               edge.source_vertex_id, edge.target_vertex_id);
                    }
                }
            } else {
               panic!("Expected different nodes for edge {}", edge.edge_id);
            }
        }
    }
}