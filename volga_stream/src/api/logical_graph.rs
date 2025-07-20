use std::collections::HashMap;
use std::sync::Arc;
use arrow::datatypes::Schema as ArrowSchema;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::prelude::EdgeRef;
use crate::runtime::operators::operator::OperatorConfig;

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
    node_mapping: HashMap<String, NodeIndex>,
}

impl LogicalGraph {
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            node_counter: 0,
            node_mapping: HashMap::new(),
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
}