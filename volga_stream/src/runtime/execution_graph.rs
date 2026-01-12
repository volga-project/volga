use std::collections::HashMap;
use std::sync::Arc;
use crate::cluster::node_assignment::ExecutionVertexNodeMapping;
use crate::runtime::operators::operator::{get_operator_type_from_config, OperatorConfig};
use crate::runtime::partition::PartitionType;
use crate::transport::channel::Channel;
use crate::transport::transport_spec::TransportSpec;
use crate::runtime::operators::operator::OperatorType;
use crate::runtime::VertexId;
use crate::runtime::watermark::WatermarkAssignConfig;

#[derive(Debug, Clone)]
pub struct ExecutionEdge {
    pub source_vertex_id: VertexId,
    pub target_vertex_id: VertexId,
    pub edge_id: String,
    pub target_operator_id: String,
    pub partition_type: PartitionType,
    pub channel: Option<Channel>,
}
 
impl ExecutionEdge {
    pub fn new(
        source_vertex_id: String,
        target_vertex_id: String,
        target_operator_id: String,
        partition_type: PartitionType,
        channel: Option<Channel>,
    ) -> Self {
        Self {
            source_vertex_id: Arc::<str>::from(source_vertex_id.as_str()),
            target_vertex_id: Arc::<str>::from(target_vertex_id.as_str()),
            edge_id: gen_edge_id(&source_vertex_id, &target_vertex_id),
            target_operator_id: target_operator_id.clone(),
            partition_type,
            channel,
        }
    }

    pub fn get_channel(&self) -> Channel {
        self.channel.as_ref().expect("channel should be present").clone()
    }
}

pub fn gen_edge_id(source_vertex_id: &str, target_vertex_id: &str) -> String {
    format!("{}-{}", source_vertex_id, target_vertex_id)
}

#[derive(Debug, Clone)]
pub struct ExecutionVertex {
    pub vertex_id: VertexId,
    pub operator_id: String,
    pub operator_config: OperatorConfig,
    pub input_edges: Vec<String>,
    pub output_edges: Vec<String>,
    pub parallelism: i32,
    pub task_index: i32,
    pub watermark_assign: Option<WatermarkAssignConfig>,
}

impl ExecutionVertex {
    pub fn new(
        vertex_id: String,
        operator_id: String,
        operator_config: OperatorConfig,
        parallelism: i32,
        task_index: i32,
    ) -> Self {
        Self {
            vertex_id: Arc::<str>::from(vertex_id),
            operator_id,
            operator_config,
            input_edges: Vec::new(),
            output_edges: Vec::new(),
            parallelism,
            task_index,
            watermark_assign: None,
        }
    }

    pub fn add_input_edge(&mut self, edge_id: String) {
        self.input_edges.push(edge_id);
    }

    pub fn add_output_edge(&mut self, edge_id: String) {
        self.output_edges.push(edge_id);
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionGraph {
    vertices: HashMap<VertexId, ExecutionVertex>,
    edges: HashMap<String, ExecutionEdge>,
    // Optional pipeline execution mode. Used for runtime invariants (e.g. disabling watermark assigners in Batch).
    execution_mode: Option<String>,
}

impl ExecutionGraph {
    pub fn new() -> Self {
        Self {
            vertices: HashMap::new(),
            edges: HashMap::new(),
            execution_mode: None,
        }
    }

    pub fn set_execution_mode(&mut self, mode: impl Into<String>) {
        self.execution_mode = Some(mode.into());
    }

    pub fn execution_mode(&self) -> Option<&str> {
        self.execution_mode.as_deref()
    }

    pub fn add_vertex(&mut self, vertex: ExecutionVertex) {
        self.vertices.insert(vertex.vertex_id.clone(), vertex);
    }

    pub fn add_edge(&mut self, edge: ExecutionEdge) {
        let source_id = edge.source_vertex_id.clone();
        let target_id = edge.target_vertex_id.clone();
        let edge_id = edge.edge_id.clone();

        // Verify both vertices exist
        if !self.vertices.contains_key(&source_id) {
            panic!("Source vertex {} does not exist", source_id);
        }
        if !self.vertices.contains_key(&target_id) {
            panic!("Target vertex {} does not exist", target_id);
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
    }

    pub fn get_vertex(&self, vertex_id: &str) -> Option<&ExecutionVertex> {
        self.vertices.get(vertex_id)
    }

    pub fn get_vertex_mut(&mut self, vertex_id: &str) -> Option<&mut ExecutionVertex> {
        self.vertices.get_mut(vertex_id)
    }

    pub fn get_edge(&self, edge_id: &str) -> Option<&ExecutionEdge> {
        self.edges.get(edge_id)
    }

    pub fn get_vertices(&self) -> &HashMap<VertexId, ExecutionVertex> {
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

    pub fn get_source_vertices(&self) -> Vec<ExecutionVertex> {
        self.vertices.values()
            .filter(|v| matches!(v.operator_config, OperatorConfig::SourceConfig(_)))
            .cloned()
            .collect()
    }

    pub fn get_vertex_type(&self, vertex_id: &str) -> OperatorType {
        let vertex = self.vertices.get(vertex_id).expect("vertex should exist");
        get_operator_type_from_config(&vertex.operator_config)
    }

    pub fn get_sink_vertices(&self) -> Vec<String> {
        self.vertices.iter()
            .filter_map(|(id, vertex)| {
                match &vertex.operator_config {
                    OperatorConfig::SinkConfig(_) => Some(id.as_ref().to_string()),
                    _ => None,
                }
            })
            .collect()
    }

    // TODO we should add topology verification for request mode (window+window_request and reques source+sink tasks on same nodes)

    // TODO we should separate execution graph and channel configurations - 
    // execution graph should be static and compiled by planner,
    // channel mapping depends on cluster configuration and is defined at runtime
    pub fn update_channels_with_node_mapping(
        &mut self,
        execution_vertex_to_cluster_node: Option<&ExecutionVertexNodeMapping>,
    ) {
        self.update_channels_with_node_mapping_and_transport(
            execution_vertex_to_cluster_node,
            &TransportSpec::default(),
            &HashMap::new(),
        );
    }

    pub fn update_channels_with_node_mapping_and_transport(
        &mut self,
        execution_vertex_to_cluster_node: Option<&ExecutionVertexNodeMapping>,
        transport: &TransportSpec,
        per_operator_queue_records: &HashMap<String, u32>,
    ) {
        for edge in self.edges.values_mut() {
            let source_operator_id = self
                .vertices
                .get(&edge.source_vertex_id)
                .expect("source vertex should exist")
                .operator_id
                .clone();
            let target_operator_id = self
                .vertices
                .get(&edge.target_vertex_id)
                .expect("target vertex should exist")
                .operator_id
                .clone();
            let mut queue_size_records = transport.default_queue_records.max(1);
            if let Some(v) = per_operator_queue_records.get(&source_operator_id) {
                queue_size_records = queue_size_records.max((*v).max(1));
            }
            if let Some(v) = per_operator_queue_records.get(&target_operator_id) {
                queue_size_records = queue_size_records.max((*v).max(1));
            }

            let channel = if let Some(vertex_to_node) = execution_vertex_to_cluster_node {
                // Check if vertices are on different nodes
                let source_node = vertex_to_node
                    .get(edge.source_vertex_id.as_ref())
                    .expect(&format!("Node with id {} expected", edge.source_vertex_id));
                let target_node = vertex_to_node
                    .get(edge.target_vertex_id.as_ref())
                    .expect(&format!("Node with id {} expected", edge.target_vertex_id));
                
                if source_node.node_id != target_node.node_id {
                    // Vertices are on different nodes, create remote channel
                    Channel::new_remote_with_queue(
                        edge.source_vertex_id.as_ref().to_string(),
                        edge.target_vertex_id.as_ref().to_string(),
                        source_node.node_ip.clone(), 
                        source_node.node_id.clone(), 
                        target_node.node_ip.clone(), 
                        target_node.node_id.clone(), 
                        target_node.node_port as i32,
                        queue_size_records,
                    )
                } else {
                    // Vertices are on same node, use local channel
                    Channel::new_local_with_queue(
                        edge.source_vertex_id.as_ref().to_string(),
                        edge.target_vertex_id.as_ref().to_string(),
                        queue_size_records,
                    )
                }
            } else {
                // No cluster mapping provided, use local channels
                Channel::new_local_with_queue(
                    edge.source_vertex_id.as_ref().to_string(),
                    edge.target_vertex_id.as_ref().to_string(),
                    queue_size_records,
                )
            };

            edge.channel = Some(channel);
        }
    }
}
