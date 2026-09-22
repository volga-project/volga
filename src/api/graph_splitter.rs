//! Splits a compiled logical graph into a streaming graph and a request graph.
//!
//! Streaming keeps the write path (window operator in `StateOnly`, no outgoing edge).
//! The request graph is `keyby → window request → followers` at parallelism 1.

use std::collections::{HashMap, HashSet, VecDeque};

use arrow::datatypes::SchemaRef;
use petgraph::graph::NodeIndex;
use petgraph::prelude::EdgeRef;
use petgraph::Direction;

use crate::api::logical_graph::{LogicalGraph, LogicalNode};
use crate::api::spec::pipeline::RequestSpec;
use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::operators::window::operator::WindowOutputMode;
use crate::runtime::operators::window::request::WindowRequestOperatorConfig;

/// Request-mode read path. HTTP decode and encode stay outside this graph.
#[derive(Debug, Clone)]
pub struct RequestGraph {
    pub graph: LogicalGraph,
    pub max_pending_requests: usize,
    pub request_timeout_ms: u64,
    pub schema: SchemaRef,
}

pub struct GraphSplitter;

impl GraphSplitter {
    /// `streaming` keeps the write path. Returns the request graph.
    pub fn split(
        streaming: &mut LogicalGraph,
        request: &RequestSpec,
    ) -> Result<RequestGraph, String> {
        let mut window_nodes = Vec::new();
        for node_idx in streaming.graph.node_indices() {
            if matches!(
                &streaming.graph[node_idx].operator_config,
                OperatorConfig::WindowConfig(_)
            ) {
                window_nodes.push(node_idx);
            }
        }
        if window_nodes.is_empty() {
            return Err("No window operators found in graph".to_string());
        }

        let root_node = streaming
            .root_node_index
            .ok_or_else(|| "Root node not set in graph".to_string())?;

        let top_window_node = {
            let mut min_distance = usize::MAX;
            let mut top_window = window_nodes[0];
            for &window_idx in &window_nodes {
                let distance = distance(&streaming.graph, root_node, window_idx);
                if distance < min_distance {
                    min_distance = distance;
                    top_window = window_idx;
                }
            }
            top_window
        };

        if let Some(node) = streaming.graph.node_weight_mut(top_window_node) {
            if let OperatorConfig::WindowConfig(ref mut config) = node.operator_config {
                config.output_mode = WindowOutputMode::StateOnly;
            }
        }

        let incoming: Vec<NodeIndex> = streaming
            .graph
            .neighbors_directed(top_window_node, Direction::Incoming)
            .collect();
        assert_eq!(
            incoming.len(),
            1,
            "Window operator should have exactly one preceding node"
        );
        let keyby_node = incoming[0];
        assert!(
            matches!(
                &streaming.graph[keyby_node].operator_config,
                OperatorConfig::KeyByConfig(_)
            ),
            "Preceding node of window operator must be a KeyBy operator"
        );

        let keyby_config = streaming.graph[keyby_node].operator_config.clone();
        let window_config = match &streaming.graph[top_window_node].operator_config {
            OperatorConfig::WindowConfig(config) => config.clone(),
            _ => return Err("Expected WindowConfig".to_string()),
        };
        let schema = window_config.window_exec.input().schema();

        let keyby_idx_new = streaming.add_node(LogicalNode::new(keyby_config, 1, None, None));

        let mut window_request_config =
            WindowRequestOperatorConfig::from_window_operator_config(window_config);
        window_request_config.state_owner_operator_id =
            Some(streaming.graph[top_window_node].operator_id.clone());
        let window_request_idx = streaming.add_node(LogicalNode::new(
            OperatorConfig::WindowRequestConfig(window_request_config),
            1,
            None,
            None,
        ));
        streaming.add_edge(keyby_idx_new, window_request_idx);

        let outgoing: Vec<NodeIndex> = streaming
            .graph
            .neighbors_directed(top_window_node, Direction::Outgoing)
            .collect();
        assert_eq!(
            outgoing.len(),
            1,
            "Window operator should have exactly one outgoing edge"
        );
        let target_node = outgoing[0];
        let edge_idx = streaming
            .graph
            .find_edge(top_window_node, target_node)
            .expect("Window operator should have exactly one outgoing edge");
        streaming.graph.remove_edge(edge_idx);
        streaming.add_edge(window_request_idx, target_node);

        let graph = split_off_request_component(streaming, keyby_idx_new);
        Ok(RequestGraph {
            graph,
            max_pending_requests: request.max_pending_requests,
            request_timeout_ms: request.request_timeout_ms,
            schema,
        })
    }
}

/// Directed closure from `start`, copied at parallelism 1, then removed from `streaming`.
fn split_off_request_component(streaming: &mut LogicalGraph, start: NodeIndex) -> LogicalGraph {
    let mut request_idx = HashSet::new();
    let mut stack = vec![start];
    while let Some(idx) = stack.pop() {
        if !request_idx.insert(idx) {
            continue;
        }
        stack.extend(streaming.graph.neighbors_directed(idx, Direction::Outgoing));
    }

    let mixed = streaming.clone();

    let mut request = LogicalGraph::new();
    request.watermarks_enabled = mixed.watermarks_enabled;
    request.event_time = mixed.event_time.clone();
    request.emit_interval = mixed.emit_interval;
    request.max_parallelism = mixed.max_parallelism;

    let mut id_to_new = HashMap::new();
    for idx in mixed.graph.node_indices() {
        if !request_idx.contains(&idx) {
            continue;
        }
        let mut node = mixed.graph[idx].clone();
        node.parallelism = 1;
        let new_idx = request.graph.add_node(node);
        id_to_new.insert(request.graph[new_idx].operator_id.clone(), new_idx);
    }
    for edge in mixed.graph.edge_references() {
        if !request_idx.contains(&edge.source()) || !request_idx.contains(&edge.target()) {
            continue;
        }
        let src = mixed.graph[edge.source()].operator_id.clone();
        let tgt = mixed.graph[edge.target()].operator_id.clone();
        request.add_edge(id_to_new[&src], id_to_new[&tgt]);
    }
    if let Some(root) = mixed.root_node_index {
        if request_idx.contains(&root) {
            let id = mixed.graph[root].operator_id.clone();
            request.root_node_index = Some(id_to_new[&id]);
        }
    }

    let mut kept = LogicalGraph::new();
    kept.watermarks_enabled = mixed.watermarks_enabled;
    kept.event_time = mixed.event_time.clone();
    kept.emit_interval = mixed.emit_interval;
    kept.max_parallelism = mixed.max_parallelism;
    kept.operator_type_counters = mixed.operator_type_counters.clone();

    let mut streaming_ids = HashMap::new();
    for idx in mixed.graph.node_indices() {
        if request_idx.contains(&idx) {
            continue;
        }
        let node = mixed.graph[idx].clone();
        let new_idx = kept.graph.add_node(node);
        streaming_ids.insert(kept.graph[new_idx].operator_id.clone(), new_idx);
    }
    for edge in mixed.graph.edge_references() {
        if request_idx.contains(&edge.source()) || request_idx.contains(&edge.target()) {
            continue;
        }
        let src = mixed.graph[edge.source()].operator_id.clone();
        let tgt = mixed.graph[edge.target()].operator_id.clone();
        kept.add_edge(streaming_ids[&src], streaming_ids[&tgt]);
    }
    if let Some(window) = kept.graph.node_indices().find(|&idx| {
        matches!(
            kept.graph[idx].operator_config,
            OperatorConfig::WindowConfig(_)
        )
    }) {
        kept.root_node_index = Some(window);
    }

    *streaming = kept;
    request
}

fn distance(
    graph: &petgraph::graph::DiGraph<LogicalNode, crate::api::logical_graph::LogicalEdge>,
    source: NodeIndex,
    target: NodeIndex,
) -> usize {
    let mut queue = VecDeque::new();
    let mut visited = HashSet::new();
    queue.push_back((source, 0));
    visited.insert(source);
    while let Some((node, distance)) = queue.pop_front() {
        if node == target {
            return distance;
        }
        for neighbor in graph.neighbors_directed(node, Direction::Outgoing) {
            if visited.insert(neighbor) {
                queue.push_back((neighbor, distance + 1));
            }
        }
    }
    usize::MAX
}
