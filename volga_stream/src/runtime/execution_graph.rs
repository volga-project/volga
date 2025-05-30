use std::collections::HashMap;
use anyhow::Result;
use crate::runtime::partition::PartitionType;
use crate::transport::channel::Channel;
use std::fmt;
use crate::common::data_batch::DataBatch;
use crate::runtime::storage::in_memory_storage_actor::InMemoryStorageActor;
use kameo::prelude::ActorRef;
use crate::runtime::functions::{
    map::MapFunction,
    key_by::KeyByFunction,
    reduce::{ReduceFunction, AggregationResultExtractor},
};

use super::functions::source::word_count_source::BatchingMode;

pub struct ExecutionEdge {
    pub source_vertex_id: String,
    pub target_vertex_id: String,
    pub edge_id: String,
    pub job_edge_id: String,
    pub partition_type: PartitionType,
    pub channel: Channel,
}

impl fmt::Debug for ExecutionEdge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionEdge")
            .field("source_vertex_id", &self.source_vertex_id)
            .field("target_vertex_id", &self.target_vertex_id)
            .field("edge_id", &self.edge_id)
            .field("job_edge_id", &self.job_edge_id)
            .field("partition_type", &self.partition_type)
            .field("channel", &self.channel)
            .finish()
    }
}
 
impl ExecutionEdge {
    pub fn new(
        source_vertex_id: String,
        target_vertex_id: String,
        partition_type: PartitionType,
        channel: Channel,
    ) -> Self {
        Self {
            source_vertex_id: source_vertex_id.clone(),
            target_vertex_id: target_vertex_id.clone(),
            edge_id: format!("{}-{}", source_vertex_id, target_vertex_id),
            job_edge_id: format!("{}-{}", source_vertex_id, target_vertex_id),
            partition_type,
            channel,
        }
    }
}

#[derive(Debug, Clone)]
pub enum SourceConfig {
    VectorSourceConfig(Vec<DataBatch>),
    WordCountSourceConfig {
        word_length: usize,
        num_words: usize,        // Total pool of words to generate
        num_to_send_per_word: Option<usize>, // Optional: how many copies of each word to send
        run_for_s: Option<u64>,
        batch_size: usize,
        batching_mode: BatchingMode, // Controls how words are batched together
    },
}

#[derive(Clone, Debug)]
pub enum SinkConfig {
    InMemoryStorageActorSinkConfig(ActorRef<InMemoryStorageActor>),
}

pub enum OperatorConfig {
    MapConfig(MapFunction),
    JoinConfig(HashMap<String, String>),
    SinkConfig(SinkConfig),
    SourceConfig(SourceConfig),
    KeyByConfig(KeyByFunction),
    ReduceConfig(ReduceFunction, Option<AggregationResultExtractor>),
}

impl fmt::Debug for OperatorConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OperatorConfig::MapConfig(config) => f.debug_tuple("MapConfig").field(config).finish(),
            OperatorConfig::JoinConfig(config) => f.debug_tuple("JoinConfig").field(config).finish(),
            OperatorConfig::SinkConfig(config) => f.debug_tuple("SinkConfig").field(config).finish(),
            OperatorConfig::SourceConfig(config) => f.debug_tuple("SourceConfig").field(config).finish(),
            OperatorConfig::KeyByConfig(config) => f.debug_tuple("KeyByConfig").field(config).finish(),
            OperatorConfig::ReduceConfig(reduce_fn, extractor) => {
                let mut debug = f.debug_tuple("ReduceConfig");
                debug.field(reduce_fn);
                if let Some(ext) = extractor {
                    debug.field(ext);
                }
                debug.finish()
            },
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
            OperatorConfig::KeyByConfig(config) => OperatorConfig::KeyByConfig(config.clone()),
            OperatorConfig::ReduceConfig(reduce_fn, extractor) => {
                OperatorConfig::ReduceConfig(reduce_fn.clone(), extractor.clone())
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionVertex {
    pub vertex_id: String,
    pub operator_config: OperatorConfig,
    pub input_edges: Vec<String>,
    pub output_edges: Vec<String>,
    pub parallelism: i32,
    pub task_index: i32,
}

impl ExecutionVertex {
    pub fn new(
        vertex_id: String,
        operator_config: OperatorConfig,
        parallelism: i32,
        task_index: i32,
    ) -> Self {
        Self {
            vertex_id,
            operator_config,
            input_edges: Vec::new(),
            output_edges: Vec::new(),
            parallelism,
            task_index,
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