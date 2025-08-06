use std::collections::HashMap;
use std::sync::Arc;
use std::fmt;
use arrow::datatypes::Schema as ArrowSchema;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::prelude::EdgeRef;
use crate::runtime::operators::chained::chained_operator::group_operators_for_chaining;
use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::execution_graph::{ExecutionGraph, ExecutionVertex, ExecutionEdge};
use crate::runtime::partition::PartitionType;
use crate::transport::channel::Channel;
use crate::cluster::cluster_provider::ClusterNode;
use crate::runtime::functions::map::MapFunction;



#[derive(Debug, Clone)]
pub struct LogicalNode {
    pub node_id: String,
    pub operator_config: OperatorConfig,
    pub parallelism: usize,
    pub in_schema: Option<Arc<ArrowSchema>>,
    pub out_schema: Option<Arc<ArrowSchema>>,
}

impl LogicalNode {
    pub fn new(operator_config: OperatorConfig, parallelism: usize, in_schema: Option<Arc<ArrowSchema>>, out_schema: Option<Arc<ArrowSchema>>) -> Self {
        Self {
            node_id: String::new(), // Will be set by add_node
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
pub struct LogicalEdge {
    pub source_node_id: String,
    pub target_node_id: String,
    pub partition_type: PartitionType,
}

#[derive(Debug, Clone)]
pub struct LogicalGraph {
    graph: DiGraph<LogicalNode, LogicalEdge>,
    // TODO use node_type
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
        node.node_id = format!("{}_{}", operator_type_name, counter);
        
        let node_index = self.graph.add_node(node);
        node_index
    }

    pub fn add_edge(&mut self, source: NodeIndex, target: NodeIndex, edge_type: PartitionType) -> petgraph::graph::EdgeIndex {
        let source_node = &self.graph[source];
        let target_node = &self.graph[target];
        let edge = LogicalEdge {
            source_node_id: source_node.node_id.clone(),
            target_node_id: target_node.node_id.clone(),
            partition_type: edge_type,
        };
        
        self.graph.add_edge(source, target, edge)
    }

    pub fn get_node(&self, operator_id: String) -> Option<&LogicalNode> {
        self.graph.node_weights().find(|node| node.node_id == operator_id)
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
                .find(|&idx| self.graph[idx].node_id == logical_node.node_id)
                .unwrap();

            let mut execution_vertex_ids = Vec::new();
            let parallelism = logical_node.parallelism;

            // Create parallel execution vertices for this logical node
            for i in 0..parallelism {
                let execution_vertex_id = format!("{}_{}", logical_node.node_id, i);

                let execution_vertex = ExecutionVertex::new(
                    execution_vertex_id.clone(),
                    logical_node.node_id.clone(),
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

            let partition_type = logical_edge.partition_type.clone();

            // Connect each source execution vertex to each target execution vertex
            for source_execution_vertex_id in source_execution_vertices {
                for target_execution_vertex_id in target_execution_vertices {
                    let execution_edge = ExecutionEdge::new(
                        source_execution_vertex_id.clone(),
                        target_execution_vertex_id.clone(),
                        self.graph[target_logical_node_index].node_id.clone(),
                        partition_type.clone(),
                        None, // No channel initially
                    );

                    execution_graph.add_edge(execution_edge);
                }
            }
        }

        execution_graph
    }

    pub fn from_linear_operators(operator_list: Vec<OperatorConfig>, parallelism: usize, chained: bool) -> Self {
        // Validate configuration
        validate_linear_operator_list(&operator_list);

        // Group operators based on chaining configuration
        let grouped_operators = if chained {
            group_operators_for_chaining(&operator_list)
        } else {
            // If no chaining, each operator becomes its own group
            operator_list.clone()
        };

        // Create a linear logical graph
        let mut logical_graph = LogicalGraph::new();
        let mut node_indices = Vec::new();
        
        // Add nodes for each operator
        for op_config in &grouped_operators {
            let node = LogicalNode::new(
                op_config.clone(),
                parallelism,
                None, // in_schema
                None, // out_schema
            );
            let node_idx = logical_graph.add_node(node);
            node_indices.push(node_idx);
        }

        // Add edges between operators
        for i in 0..grouped_operators.len() - 1 {
            let source_config = &grouped_operators[i];
            let target_config = &grouped_operators[i + 1];
            
            // Determine partition type based on operator types
            let partition_type = determine_partition_type(source_config, target_config);
            
            logical_graph.add_edge(
                node_indices[i],
                node_indices[i + 1],
                partition_type,
            );
        }

        logical_graph
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
            let source_id = self.graph[edge.source()].node_id.clone();
            let target_id = self.graph[edge.target()].node_id.clone();
            let partition_type = match edge.weight().partition_type {
                PartitionType::Forward => "Forward",
                PartitionType::Hash => "Hash",
                PartitionType::Broadcast => "Broadcast",
                PartitionType::RoundRobin => "RoundRobin",
            };
            dot_string.push_str(&format!("  {} -> {} [label=\"{}\"];\n", source_id, target_id, partition_type));
        }
        
        dot_string.push_str("}\n");
        dot_string
    }
}

fn validate_linear_operator_list(operators: &[OperatorConfig]) {
    for (i, op_config) in operators.iter().enumerate() {
        match op_config {
            OperatorConfig::ReduceConfig(_, _) => {
                // Check if there's a KeyBy operator right before this reduce
                if i == 0 {
                    panic!("Reduce operator '{}' requires a KeyBy operator before it", op_config);
                }
                
                let prev_config = &operators[i - 1];
                match prev_config {
                    OperatorConfig::KeyByConfig(_) => {
                        // This is valid - reduce has keyby right before it
                    }
                    _ => {
                        panic!("Reduce operator '{}' requires a KeyBy operator immediately before it", op_config);
                    }
                }
            }
            _ => {}
        }
    }
}

/// Determines the appropriate partition type between two operators
pub fn determine_partition_type(source_config: &OperatorConfig, target_config: &OperatorConfig) -> PartitionType {
    match (source_config, target_config) {
        // Hash partitioning when source is KeyBy
        (OperatorConfig::KeyByConfig(_), _) => PartitionType::Hash,
        // Hash partitioning when source is ChainedConfig and the last operator in the chain is KeyBy
        (OperatorConfig::ChainedConfig(configs), _) => {
            let mut has_key_by = false;
            for config in configs {
                match config {
                    OperatorConfig::KeyByConfig(_) => {
                        has_key_by = true;
                        break;
                    }
                    _ => {}
                }
            }
            if has_key_by {
                PartitionType::Hash
            } else {
                PartitionType::RoundRobin
            }
        }
        // All other cases use round-robin
        _ => PartitionType::RoundRobin,
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
    use crate::common::test_utils::IdentityMapFunction;
    use crate::runtime::functions::key_by::KeyByFunction;
    use crate::runtime::operators::sink::sink_operator::SinkConfig;
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
        logical_graph.add_edge(source_index, filter_index, PartitionType::RoundRobin);
        logical_graph.add_edge(filter_index, projection_index, PartitionType::RoundRobin);
        logical_graph.add_edge(projection_index, sink_index, PartitionType::RoundRobin);

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

        logical_graph.add_edge(source_index, filter_index, PartitionType::Forward);

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

    #[tokio::test]
    async fn test_linear_logical_to_execution_graph_chained() {
        // Define operator list: source -> map1 -> keyby -> map2 -> sink
        let operators = vec![
            OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![]))),
            OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
            OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["value".to_string()])),
            OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
            OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig("http://127.0.0.1:8080".to_string())),
        ];

        let logical_graph = LogicalGraph::from_linear_operators(operators, 2, true);
        let graph = logical_graph.to_execution_graph();

        // Verify vertices - KeyBy should break the chain
        // source -> map1 -> keyby -> map2 -> sink becomes: chain_source->map1->keyby -> chain_map2->sink
        assert_eq!(graph.get_vertices().len(), 4); // 2 groups * 2 parallelism
        // Verify chained configs in vertices
        let vertices = graph.get_vertices().values();
        
        // Count vertices with source->map->keyby chain
        let source_chains = vertices.clone()
            .filter(|v| {
                if let OperatorConfig::ChainedConfig(chain) = &v.operator_config {
                    chain.len() == 3 && 
                    matches!(chain[0], OperatorConfig::SourceConfig(_)) &&
                    matches!(chain[1], OperatorConfig::MapConfig(_)) &&
                    matches!(chain[2], OperatorConfig::KeyByConfig(_))
                } else {
                    false
                }
            })
            .count();
        assert_eq!(source_chains, 2, "Should have 2 source->map->keyby chains");

        // Count vertices with map->sink chain
        let sink_chains = vertices
            .filter(|v| {
                if let OperatorConfig::ChainedConfig(chain) = &v.operator_config {
                    chain.len() == 2 &&
                    matches!(chain[0], OperatorConfig::MapConfig(_)) &&
                    matches!(chain[1], OperatorConfig::SinkConfig(_))
                } else {
                    false
                }
            })
            .count();
        assert_eq!(sink_chains, 2, "Should have 2 map->sink chains");

        // Verify edges between groups
        assert_eq!(graph.get_edges().len(), 4); // 1 connection * 4 edges

        // Verify partition types for edges
        for edge in graph.get_edges().values() {
            // chain_source->map1->keyby -> chain_map2->sink should use Hash partitioning
            // because keyby is the last operator in the source chain
            assert!(matches!(edge.partition_type, crate::runtime::partition::PartitionType::Hash),
                "Edge {} -> {} should use Hash partitioning (keyby -> map2)", edge.source_vertex_id, edge.target_vertex_id);
        }
    }

    #[tokio::test]
    async fn test_linear_logical_to_execution_graph() {
        // Define operator chain: source -> keyby -> map -> sink
        let operators = vec![
            OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![]))),
            OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["value".to_string()])),
            OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
            OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig("http://127.0.0.1:8080".to_string())),
        ];

        let logical_graph = LogicalGraph::from_linear_operators(operators, 2, false);
        let graph = logical_graph.to_execution_graph();

        // Verify vertices
        assert_eq!(graph.get_vertices().len(), 8); // 4 operators * 2 parallelism

        // Verify edges - keyby should use hash partitioning
        assert_eq!(graph.get_edges().len(), 12); // 2 source -> 2 keyby + 2 keyby -> 2 map + 2 map -> 2 sink

        // Check partition types for all edges
        for edge in graph.get_edges().values() {
            let source_vertex = graph.get_vertex(&edge.source_vertex_id).unwrap();
            let target_vertex = graph.get_vertex(&edge.target_vertex_id).unwrap();

            match (&source_vertex.operator_config, &target_vertex.operator_config) {
                (OperatorConfig::SourceConfig(_), OperatorConfig::KeyByConfig(_)) => {
                    // source -> keyby should use RoundRobin
                    assert!(matches!(edge.partition_type, crate::runtime::partition::PartitionType::RoundRobin),
                        "Edge {} -> {} should use RoundRobin partitioning", edge.source_vertex_id, edge.target_vertex_id);
                }
                (OperatorConfig::KeyByConfig(_), OperatorConfig::MapConfig(_)) => {
                    // keyby -> map should use Hash partitioning
                    assert!(matches!(edge.partition_type, crate::runtime::partition::PartitionType::Hash),
                        "Edge {} -> {} should use Hash partitioning", edge.source_vertex_id, edge.target_vertex_id);
                }
                (OperatorConfig::MapConfig(_), OperatorConfig::SinkConfig(_)) => {
                    // map -> sink should use RoundRobin
                    assert!(matches!(edge.partition_type, crate::runtime::partition::PartitionType::RoundRobin),
                        "Edge {} -> {} should use RoundRobin partitioning", edge.source_vertex_id, edge.target_vertex_id);
                }
                _ => panic!("Unexpected edge: {} -> {}", edge.source_vertex_id, edge.target_vertex_id)
            }
        }
    }
}