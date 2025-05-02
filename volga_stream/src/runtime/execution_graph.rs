use std::collections::HashMap;
use anyhow::Result;
use crate::runtime::partition::Partition;
use crate::transport::channel::Channel;
use std::fmt;
use crate::runtime::operator::Operator;

pub struct ExecutionEdge {
    pub source_vertex_id: String,
    pub target_vertex_id: String,
    pub edge_id: String,
    pub partition: Box<dyn Partition>,
    pub channel: Channel,
}

impl fmt::Debug for ExecutionEdge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionEdge")
            .field("source_vertex_id", &self.source_vertex_id)
            .field("target_vertex_id", &self.target_vertex_id)
            .field("edge_id", &self.edge_id)
            .field("partition", &"<dyn Partition>")
            .field("channel", &self.channel)
            .finish()
    }
}

impl ExecutionEdge {
    pub fn new(
        source_vertex_id: String,
        target_vertex_id: String,
        partition: Box<dyn Partition>,
        channel: Channel,
    ) -> Self {
        Self {
            source_vertex_id: source_vertex_id.clone(),
            target_vertex_id: target_vertex_id.clone(),
            edge_id: format!("{}-{}", source_vertex_id, target_vertex_id),
            partition,
            channel,
        }
    }
}

pub enum OperatorOrConfig {
    Operator(Box<dyn Operator>),
    Config(HashMap<String, String>),
}

impl fmt::Debug for OperatorOrConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OperatorOrConfig::Operator(_) => f.debug_tuple("Operator").field(&"<dyn Operator>").finish(),
            OperatorOrConfig::Config(config) => f.debug_tuple("Config").field(config).finish(),
        }
    }
}

impl Clone for OperatorOrConfig {
    fn clone(&self) -> Self {
        match self {
            OperatorOrConfig::Operator(_) => panic!("Cannot clone Operator, use Config instead"),
            OperatorOrConfig::Config(config) => OperatorOrConfig::Config(config.clone()),
        }
    }
}

pub enum OperatorConfig {
    MapConfig(HashMap<String, String>),
    JoinConfig(HashMap<String, String>),
    SinkConfig(HashMap<String, String>),
    SourceConfig(HashMap<String, String>),
}

impl fmt::Debug for OperatorConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OperatorConfig::MapConfig(config) => f.debug_tuple("MapConfig").field(config).finish(),
            OperatorConfig::JoinConfig(config) => f.debug_tuple("JoinConfig").field(config).finish(),
            OperatorConfig::SinkConfig(config) => f.debug_tuple("SinkConfig").field(config).finish(),
            OperatorConfig::SourceConfig(config) => f.debug_tuple("SourceConfig").field(config).finish(),
        }
    }
}

impl Clone for OperatorConfig {
    fn clone(&self) -> Self {
        match self {
            OperatorConfig::MapConfig(config) => OperatorConfig::MapConfig(config.clone()),
            OperatorConfig::JoinConfig(config) => OperatorConfig::JoinConfig(config.clone()),
            OperatorConfig::SinkConfig(config) => OperatorConfig::SinkConfig(config.clone()),
            OperatorConfig::SourceConfig(config) => OperatorConfig::SourceConfig(config.clone()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionVertex {
    pub vertex_id: String,
    pub operator_config: OperatorConfig,
    pub input_edges: Vec<String>,
    pub output_edges: Vec<String>,
}

impl ExecutionVertex {
    pub fn new(
        vertex_id: String,
        operator_config: OperatorConfig,
    ) -> Self {
        Self {
            vertex_id,
            operator_config,
            input_edges: Vec::new(),
            output_edges: Vec::new(),
        }
    }

    pub fn add_input_edge(&mut self, edge_id: String) {
        self.input_edges.push(edge_id);
    }

    pub fn add_output_edge(&mut self, edge_id: String) {
        self.output_edges.push(edge_id);
    }
}

#[derive(Debug, Default)]
pub struct ExecutionGraph {
    vertices: HashMap<String, ExecutionVertex>,
    edges: HashMap<String, ExecutionEdge>,
}

impl ExecutionGraph {
    pub fn new() -> Self {
        Self {
            vertices: HashMap::new(),
            edges: HashMap::new(),
        }
    }

    pub fn add_vertex(&mut self, vertex: ExecutionVertex) {
        self.vertices.insert(vertex.vertex_id.clone(), vertex);
    }

    pub fn add_edge(&mut self, edge: ExecutionEdge) -> Result<()> {
        let source_id = edge.source_vertex_id.clone();
        let target_id = edge.target_vertex_id.clone();
        let edge_id = edge.edge_id.clone();

        // Verify both vertices exist
        if !self.vertices.contains_key(&source_id) {
            anyhow::bail!("Source vertex {} does not exist", source_id);
        }
        if !self.vertices.contains_key(&target_id) {
            anyhow::bail!("Target vertex {} does not exist", target_id);
        }

        // Add edge to the graph
        self.edges.insert(edge_id.clone(), edge);

        // Update vertex connections
        if let Some(source_vertex) = self.vertices.get_mut(&source_id) {
            source_vertex.add_output_edge(edge_id.clone());
        }
        if let Some(target_vertex) = self.vertices.get_mut(&target_id) {
            target_vertex.add_input_edge(edge_id);
        }

        Ok(())
    }

    pub fn get_vertex(&self, vertex_id: &str) -> Option<&ExecutionVertex> {
        self.vertices.get(vertex_id)
    }

    pub fn get_edge(&self, edge_id: &str) -> Option<&ExecutionEdge> {
        self.edges.get(edge_id)
    }

    pub fn get_vertices(&self) -> &HashMap<String, ExecutionVertex> {
        &self.vertices
    }

    pub fn get_edges(&self) -> &HashMap<String, ExecutionEdge> {
        &self.edges
    }

    pub fn get_edges_for_vertex(&self, vertex_id: &str) -> Option<(Vec<&ExecutionEdge>, Vec<&ExecutionEdge>)> {
        let vertex = self.vertices.get(vertex_id)?;
        
        let input_edges: Vec<&ExecutionEdge> = vertex.input_edges
            .iter()
            .map(|edge_id| self.edges.get(edge_id).expect("Edge should exist"))
            .collect();
            
        let output_edges: Vec<&ExecutionEdge> = vertex.output_edges
            .iter()
            .map(|edge_id| self.edges.get(edge_id).expect("Edge should exist"))
            .collect();
            
        Some((input_edges, output_edges))
    }
} 