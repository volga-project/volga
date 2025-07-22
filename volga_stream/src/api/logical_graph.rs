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
    pub node_id: u32,
    pub operator_config: OperatorConfig,
    pub parallelism: usize,
    pub in_schema: Option<Arc<ArrowSchema>>,
    pub out_schema: Option<Arc<ArrowSchema>>,
}

impl LogicalNode {
    pub fn new(operator_config: OperatorConfig, parallelism: usize, in_schema: Option<Arc<ArrowSchema>>, out_schema: Option<Arc<ArrowSchema>>) -> Self {
        Self {
            node_id: 0, // Will be set by add_node
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
    node_counter: u32,
}

impl LogicalGraph {
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            node_counter: 0,
        }
    }

    pub fn add_node(&mut self, mut node: LogicalNode) -> NodeIndex {
        node.node_id = self.node_counter;
        let node_index = self.graph.add_node(node);
        self.node_counter += 1;
        node_index
    }

    pub fn add_edge(&mut self, source: NodeIndex, target: NodeIndex, edge_type: EdgeType) -> petgraph::graph::EdgeIndex {
        let edge = LogicalEdge {
            edge_type,
        };
        
        self.graph.add_edge(source, target, edge)
    }

    pub fn get_node(&self, node_id: u32) -> Option<&LogicalNode> {
        self.graph.node_weights().find(|node| node.node_id == node_id)
    }

    pub fn get_nodes(&self) -> impl Iterator<Item = &LogicalNode> {
        self.graph.node_weights()
    }

    pub fn get_edges(&self) -> impl Iterator<Item = (NodeIndex, NodeIndex, &LogicalEdge)> {
        self.graph.edge_references().map(|edge| (edge.source(), edge.target(), edge.weight()))
    }

    /// TODO parallelism should come from logical graph, not as a param
    /// Convert logical graph to execution graph with parallelism
    pub fn to_execution_graph(
        &self, 
        parallelism: usize,
    ) -> ExecutionGraph {
        let mut execution_graph = ExecutionGraph::new();
        let mut logical_to_execution_mapping: HashMap<NodeIndex, Vec<String>> = HashMap::new();

        // Create execution vertices for each logical node
        for logical_node in self.graph.node_weights() {
            let logical_node_index = self.graph.node_indices()
                .find(|&idx| self.graph[idx].node_id == logical_node.node_id)
                .unwrap();

            let mut execution_vertex_ids = Vec::new();

            // Create parallel execution vertices for this logical node
            for i in 0..parallelism {
                let execution_vertex_id = if parallelism == 1 {
                    format!("{}", logical_node.node_id)
                } else {
                    format!("{}_{}", logical_node.node_id, i)
                };

                let execution_vertex = ExecutionVertex::new(
                    execution_vertex_id.clone(),
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
            let source_logical_node = edge.source();
            let target_logical_node = edge.target();
            let logical_edge = edge.weight();

            let source_execution_vertices = &logical_to_execution_mapping[&source_logical_node];
            let target_execution_vertices = &logical_to_execution_mapping[&target_logical_node];

            // Determine partition type based on logical edge type
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
                        target_execution_vertex_id.clone(), // target_operator_id
                        partition_type.clone(),
                        None, // No channel initially
                    );

                    execution_graph.add_edge(execution_edge);
                }
            }
        }

        execution_graph
    }

    /// Create channels for execution graph based on cluster mapping
    pub fn create_channels_for_execution_graph(
        execution_graph: &ExecutionGraph,
        execution_vertex_to_cluster_node: Option<&HashMap<String, ClusterNode>>,
    ) -> HashMap<String, Channel> {
        let mut edge_channels = HashMap::new();

        for edge in execution_graph.get_edges().values() {
            let channel = if let Some(vertex_to_node) = execution_vertex_to_cluster_node {
                // Check if vertices are on different nodes
                let source_node = vertex_to_node.get(&edge.source_vertex_id).expect(&format!("Node with id {} expected", edge.source_vertex_id));
                let target_node = vertex_to_node.get(&edge.target_vertex_id).expect(&format!("Node with id {} expected", edge.target_vertex_id));
                
                if source_node.node_id != target_node.node_id {
                    // Vertices are on different nodes, create remote channel
                    Channel::Remote {
                        channel_id: format!("{}_to_{}", edge.source_vertex_id, edge.target_vertex_id),
                        source_node_ip: source_node.node_ip.clone(),
                        source_node_id: source_node.node_id.clone(),
                        target_node_ip: target_node.node_ip.clone(),
                        target_node_id: target_node.node_id.clone(),
                        target_port: target_node.node_port as i32,
                    }
                } else {
                    // Vertices are on same node, use local channel
                    Channel::Local {
                        channel_id: format!("{}_to_{}", edge.source_vertex_id, edge.target_vertex_id),
                    }
                }
            } else {
                // No cluster mapping provided, use local channels
                Channel::Local {
                    channel_id: format!("{}_to_{}", edge.source_vertex_id, edge.target_vertex_id),
                }
            };

            edge_channels.insert(edge.edge_id.clone(), channel);
        }

        edge_channels
    }


    /// Generate DOT format string
    pub fn to_dot(&self) -> String {
        let mut dot_string = String::from("digraph LogicalGraph {\n");
        
        // Add nodes with labels
        for node in self.graph.node_weights() {
            dot_string.push_str(&format!("  {} [label=\"{}: {}\"];\n", node.node_id, node.node_id, node.operator_config));
        }
        
        // Add edges with labels
        for edge in self.graph.edge_references() {
            let source_id = self.graph[edge.source()].node_id;
            let target_id = self.graph[edge.target()].node_id;
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
    use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
    use crate::runtime::functions::map::MapFunction;
    use crate::runtime::functions::map::filter_function::FilterFunction;
    use datafusion::execution::context::SessionContext;
    use crate::common::message::Message;
    use arrow::datatypes::{Schema, Field, DataType};
    use std::sync::Arc;

    #[test]
    fn test_logical_to_execution_graph_conversion() {
        let mut logical_graph = LogicalGraph::new();

        // Create test schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create logical nodes: source -> filter -> projection
        let source_config = SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![]));
        let source_node = LogicalNode::new(
            OperatorConfig::SourceConfig(source_config),
            1,
            None,
            None,
        );
        let source_index = logical_graph.add_node(source_node);

        let filter_function = FilterFunction::new(
            datafusion::common::DFSchemaRef::from(datafusion::common::DFSchema::try_from(schema.clone()).unwrap()),
            datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(0)),
            SessionContext::new(),
        );
        let filter_node = LogicalNode::new(
            OperatorConfig::MapConfig(MapFunction::Filter(filter_function)),
            1,
            None,
            None,
        );
        let filter_index = logical_graph.add_node(filter_node);

        let projection_function = crate::runtime::functions::map::projection_function::ProjectionFunction::new(
            datafusion::common::DFSchemaRef::from(datafusion::common::DFSchema::try_from(schema.clone()).unwrap()),
            datafusion::common::DFSchemaRef::from(datafusion::common::DFSchema::try_from(schema.clone()).unwrap()),
            vec![datafusion::logical_expr::col("id"), datafusion::logical_expr::col("name")],
            SessionContext::new(),
        );
        let projection_node = LogicalNode::new(
            OperatorConfig::MapConfig(MapFunction::Projection(projection_function)),
            1,
            None,
            None,
        );
        let projection_index = logical_graph.add_node(projection_node);

        // Add edges
        logical_graph.add_edge(source_index, filter_index, EdgeType::Forward);
        logical_graph.add_edge(filter_index, projection_index, EdgeType::Forward);

        // Convert to execution graph with parallelism 2
        let execution_graph = logical_graph.to_execution_graph(2);

        // Verify execution vertices
        let vertices = execution_graph.get_vertices();
        assert_eq!(vertices.len(), 6); // 3 logical nodes * 2 parallelism

        // Verify execution edges
        let edges = execution_graph.get_edges();
        assert_eq!(edges.len(), 8); // 2 logical edges * 2 source * 2 target

        // Verify partition types
        for edge in edges.values() {
            assert!(matches!(edge.partition_type, PartitionType::RoundRobin),
                "Edge {} -> {} should use RoundRobin partitioning", edge.source_vertex_id, edge.target_vertex_id);
            // Verify channels are not set initially
            assert!(edge.channel.is_none(), "Edge {} -> {} should not have channel set initially", edge.source_vertex_id, edge.target_vertex_id);
        }

        // Verify vertex naming
        assert!(vertices.contains_key("0_0"));
        assert!(vertices.contains_key("0_1"));
        assert!(vertices.contains_key("1_0"));
        assert!(vertices.contains_key("1_1"));
        assert!(vertices.contains_key("2_0"));
        assert!(vertices.contains_key("2_1"));
    }

    #[test]
    fn test_logical_to_execution_graph_with_cluster() {
        let mut logical_graph = LogicalGraph::new();

        // Create a simple logical graph: source -> filter
        let source_config = SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![]));
        let source_node = LogicalNode::new(
            OperatorConfig::SourceConfig(source_config),
            1,
            None,
            None,
        );
        let source_index = logical_graph.add_node(source_node);

        let filter_function = FilterFunction::new(
            datafusion::common::DFSchemaRef::from(datafusion::common::DFSchema::try_from(
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
            ).unwrap()),
            datafusion::logical_expr::col("id").gt(datafusion::logical_expr::lit(0)),
            SessionContext::new(),
        );
        let filter_node = LogicalNode::new(
            OperatorConfig::MapConfig(MapFunction::Filter(filter_function)),
            1,
            None,
            None,
        );
        let filter_index = logical_graph.add_node(filter_node);

        logical_graph.add_edge(source_index, filter_index, EdgeType::Forward);

        // Create cluster mapping: source vertices on node1, filter vertices on node2
        let mut vertex_to_node = HashMap::new();
        vertex_to_node.insert("0_0".to_string(), ClusterNode::new("node1".to_string(), "192.168.1.10".to_string(), 8080));
        vertex_to_node.insert("0_1".to_string(), ClusterNode::new("node1".to_string(), "192.168.1.10".to_string(), 8080));
        vertex_to_node.insert("1_0".to_string(), ClusterNode::new("node2".to_string(), "192.168.1.11".to_string(), 8081));
        vertex_to_node.insert("1_1".to_string(), ClusterNode::new("node2".to_string(), "192.168.1.11".to_string(), 8081));

        // Convert to execution graph with cluster mapping
        let mut execution_graph = logical_graph.to_execution_graph(2);
        
        // Create channels based on cluster mapping
        let edge_channels = LogicalGraph::create_channels_for_execution_graph(&execution_graph, Some(&vertex_to_node));
        
        // Update execution graph with channels
        execution_graph.update_channels(edge_channels);

        // Verify execution vertices
        let vertices = execution_graph.get_vertices();
        assert_eq!(vertices.len(), 4); // 2 logical nodes * 2 parallelism

        // Verify execution edges
        let edges = execution_graph.get_edges();
        assert_eq!(edges.len(), 4); // 2 source * 2 target

        // Verify that edges between different nodes use remote channels
        for edge in edges.values() {
            if edge.source_vertex_id.starts_with("0") && edge.target_vertex_id.starts_with("1") {
                // Source to filter edges should be remote
                match &edge.channel {
                    Some(Channel::Remote { source_node_ip, target_node_ip, target_port, .. }) => {
                        assert_eq!(source_node_ip, "192.168.1.10");
                        assert_eq!(target_node_ip, "192.168.1.11");
                        assert_eq!(*target_port, 8081);
                    }
                    Some(Channel::Local { .. }) => {
                        panic!("Expected remote channel for cross-node edge");
                    }
                    None => {
                        panic!("Expected channel to be set");
                    }
                }
            }
        }
    }
}