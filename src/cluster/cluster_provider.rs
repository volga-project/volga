use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Represents a node in the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
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
pub trait ClusterProvider: Send + Sync {
    fn get_node(&self, node_id: &str) -> Option<&ClusterNode>;
    fn get_all_nodes(&self) -> &HashMap<String, ClusterNode>;
}

// TODO this is not used, we can delete
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

pub fn create_test_cluster_nodes(num_nodes: usize) -> Vec<ClusterNode> {
    (0..num_nodes)
        .map(|i| ClusterNode::new(
            format!("node{}", i + 1),
            format!("192.168.1.{}", 10 + i),
            8080 + i as u16,
        ))
        .collect()
}

/// Local-only cluster provider: represents the local machine as one (or a few) logical nodes.
#[derive(Debug, Clone)]
pub struct LocalMachineClusterProvider {
    nodes: HashMap<String, ClusterNode>,
}

impl LocalMachineClusterProvider {
    pub fn single_node() -> Self {
        let mut nodes = HashMap::new();
        nodes.insert(
            "local".to_string(),
            ClusterNode::new("local".to_string(), "127.0.0.1".to_string(), 0),
        );
        Self { nodes }
    }

    /// Two logical nodes on the same machine (useful for “master on one node, worker on another” scenarios).
    pub fn master_worker_nodes() -> Self {
        let mut nodes = HashMap::new();
        nodes.insert(
            "master".to_string(),
            ClusterNode::new("master".to_string(), "127.0.0.1".to_string(), 0),
        );
        nodes.insert(
            "worker".to_string(),
            ClusterNode::new("worker".to_string(), "127.0.0.1".to_string(), 0),
        );
        Self { nodes }
    }
}

impl ClusterProvider for LocalMachineClusterProvider {
    fn get_node(&self, node_id: &str) -> Option<&ClusterNode> {
        self.nodes.get(node_id)
    }

    fn get_all_nodes(&self) -> &HashMap<String, ClusterNode> {
        &self.nodes
    }
}