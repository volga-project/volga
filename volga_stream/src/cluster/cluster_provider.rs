use std::collections::HashMap;

/// Represents a node in the cluster
#[derive(Debug, Clone)]
pub struct ClusterNode {
    pub node_id: String,
    pub node_ip: String,
    pub node_port: u16,
}

impl ClusterNode {
    pub fn new(node_id: String, node_ip: String, node_port: u16) -> Self {
        Self {
            node_id,
            node_ip,
            node_port,
        }
    }
}

/// Provider for cluster node information
pub trait ClusterProvider {
    fn get_node(&self, node_id: &str) -> Option<&ClusterNode>;
    fn get_all_nodes(&self) -> &HashMap<String, ClusterNode>;
}

/// Simple in-memory cluster provider
#[derive(Debug, Clone)]
pub struct InMemoryClusterProvider {
    nodes: HashMap<String, ClusterNode>,
}

impl InMemoryClusterProvider {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, node: ClusterNode) {
        self.nodes.insert(node.node_id.clone(), node);
    }

    pub fn add_nodes(&mut self, nodes: Vec<ClusterNode>) {
        for node in nodes {
            self.add_node(node);
        }
    }
}

impl ClusterProvider for InMemoryClusterProvider {
    fn get_node(&self, node_id: &str) -> Option<&ClusterNode> {
        self.nodes.get(node_id)
    }

    fn get_all_nodes(&self) -> &HashMap<String, ClusterNode> {
        &self.nodes
    }
}

impl Default for InMemoryClusterProvider {
    fn default() -> Self {
        Self::new()
    }
} 