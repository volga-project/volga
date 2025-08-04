use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::partition::PartitionType;

use crate::api::{streaming_context::StreamingContext, logical_graph::{LogicalGraph, LogicalNode, EdgeType}};
use crate::runtime::operators::source::source_operator::SourceConfig;
use crate::runtime::operators::sink::sink_operator::SinkConfig;
use crate::cluster::node_assignment::{ExecutionVertexNodeMapping, OperatorPerNodeStrategy};
use crate::cluster::cluster_provider::create_test_cluster_nodes;

use arrow::datatypes::Schema;
use std::sync::Arc;
use std::collections::HashMap;

/// Creates a test execution graph from SQL query using streaming context
pub async fn create_sql_test_execution_graph(
    sql: &str,
    sources: HashMap<String, (SourceConfig, Arc<Schema>)>,
    sink_config: Option<SinkConfig>,
    parallelism: usize,
    num_cluster_nodes: usize,
) -> (ExecutionGraph, Option<ExecutionVertexNodeMapping>) {
    // Create streaming context
    let mut context = StreamingContext::new()
        .with_parallelism(parallelism);

    // Register sources
    for (table_name, (source_config, schema)) in sources {
        context = context.with_source(table_name, source_config, schema);
    }

    // Register sink if provided
    if let Some(sink_config) = sink_config {
        context = context.with_sink(sink_config);
    }

    // Set SQL query and build execution graph
    let context = context.sql(sql);

    // Build execution graph with appropriate clustering
    let cluster_nodes = if num_cluster_nodes > 1 {
        Some(create_test_cluster_nodes(num_cluster_nodes))
    } else {
        None
    };

    let strategy = OperatorPerNodeStrategy;
    context.build_execution_graph(
        cluster_nodes.as_deref(),
        Some(&strategy),
    ).await
}


/// Configuration for generating test execution graphs
#[derive(Debug, Clone)]
pub struct TestLinearGraphConfig {
    /// List of (vertex_name, operator_config) tuples defining the operator chain
    pub operators: Vec<OperatorConfig>,
    /// Parallelism level for each operator
    pub parallelism: usize,
    /// Whether to enable chaining
    pub chained: bool,
    /// Whether to simulate remote connections
    pub is_remote: bool,
    /// For remote case
    pub num_workers_per_operator: Option<usize>
}

/// Creates a test execution graph using node assignment strategy
pub async fn create_linear_test_execution_graph(config: TestLinearGraphConfig) -> (ExecutionGraph, Option<ExecutionVertexNodeMapping>) {
    let logical_graph = LogicalGraph::from_operator_list(config.operators, config.parallelism, config.chained);

    let num_operators = logical_graph.get_nodes().count();

    // Create streaming context with the logical graph
    let context = StreamingContext::new()
        .with_parallelism(config.parallelism)
        .with_logical_graph(logical_graph);

    // Build execution graph with appropriate clustering
    let cluster_nodes = if config.is_remote {
        let num_workers = num_operators * config.num_workers_per_operator.unwrap();
        Some(create_test_cluster_nodes(num_workers))
    } else {
        None
    };

    // Use node assignment strategy to build execution graph
    let strategy = OperatorPerNodeStrategy;
    context.build_execution_graph(
        cluster_nodes.as_deref(),
        Some(&strategy),
    ).await
}

/// Validates operator configurations

// /// Helper function to create a worker distribution where each worker handles one operator type
// pub fn create_operator_based_worker_distribution(
//     num_workers_per_operator: usize,
//     operators: &[(String, OperatorConfig)],
//     parallelism_per_worker: usize,
// ) -> HashMap<String, Vec<String>> {
//     let mut distribution = HashMap::new();
//     let mut worker_id = 0;
    
//     for (op_name, _) in operators {
//         for worker_idx in 0..num_workers_per_operator {
//             let worker_id_str = format!("worker_{}", worker_id);
            
//             // Assign vertices for this worker (only vertices of the specific operator type)
//             let mut vertex_ids = Vec::new();
//             for vertex_idx in 0..parallelism_per_worker {
//                 let global_vertex_idx = worker_idx * parallelism_per_worker + vertex_idx;
//                 let vertex_id = if parallelism_per_worker == 1 {
//                     op_name.clone()
//                 } else {
//                     format!("{}_{}", op_name, global_vertex_idx)
//                 };
//                 vertex_ids.push(vertex_id);
//             }
            
//             distribution.insert(worker_id_str, vertex_ids);
//             worker_id += 1;
//         }
//     }
    
//     distribution
// }

// TODO these tests are mostly for creating execution graphs from SQL queries - move them to logical graph
#[cfg(test)]
mod sql_tests {
    use super::*;
    use crate::runtime::operators::source::source_operator::VectorSourceConfig;
    use crate::transport::channel::Channel;
    use arrow::datatypes::{Field, DataType};

    #[tokio::test]
    async fn test_simple_sql_execution_graph() {
        // Create schema for test table
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Utf8, false),
        ]));

        // Create sources map
        let mut sources = HashMap::new();
        sources.insert(
            "test_table".to_string(),
            (SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])), schema)
        );

        // Create execution graph (local execution by default)
        let (graph, node_mapping) = create_sql_test_execution_graph(
            "SELECT value FROM test_table",
            sources,
            None, // No sink
            2,    // Parallelism
            1,    // Cluster nodes (local)
        ).await;

        // Verify execution vertices were created
        assert!(!graph.get_vertices().is_empty(), "Should have created execution vertices");
        
        // Verify node mapping is None for local execution (num_cluster_nodes = 1)
        assert!(node_mapping.is_none(), "Node mapping should be None for local execution");

        // Verify edges have local channels (configured automatically)
        for edge in graph.get_edges().values() {
            assert!(edge.channel.is_some(), "Edge should have a channel");
            if let Some(Channel::Local { .. }) = edge.channel {
                // This is expected for local execution
            } else {
                panic!("Expected local channel for local execution");
            }
        }
    }

    #[tokio::test]
    async fn test_sql_execution_graph_with_aggregation() {
        // Create schema for test table
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Utf8, false),
        ]));

        // Create sources map
        let mut sources = HashMap::new();
        sources.insert(
            "test_table".to_string(),
            (SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])), schema)
        );

        // Create execution graph with aggregation
        let (graph, _) = create_sql_test_execution_graph(
            "SELECT value, COUNT(*) as count FROM test_table GROUP BY value",
            sources,
            None, // No sink
            2,    // Parallelism
            1,    // Cluster nodes (local)
        ).await;

        // Verify that the graph contains aggregation operators
        let vertices: Vec<_> = graph.get_vertices().values().collect();
        
        // Should have source, key_by, aggregate, and projection operators
        let mut has_source = false;
        let mut has_key_by = false;
        let mut has_aggregate = false;
        let mut has_projection = false;

        for vertex in vertices {
            match &vertex.operator_config {
                OperatorConfig::SourceConfig(_) => has_source = true,
                OperatorConfig::KeyByConfig(_) => has_key_by = true,
                OperatorConfig::AggregateConfig(_) => has_aggregate = true,
                OperatorConfig::MapConfig(_) => has_projection = true,
                _ => {}
            }
        }

        assert!(has_source, "Should have source operators");
        assert!(has_key_by, "Should have key_by operators for GROUP BY");
        assert!(has_aggregate, "Should have aggregate operators for COUNT");
        assert!(has_projection, "Should have projection operators");
    }

    #[tokio::test]
    async fn test_sql_execution_graph_remote() {
        // Create schema for test table
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Utf8, false),
        ]));

        // Create sources map
        let mut sources = HashMap::new();
        sources.insert(
            "test_table".to_string(),
            (SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])), schema)
        );

        // Create execution graph with remote execution (2 cluster nodes)
        let (graph, node_mapping) = create_sql_test_execution_graph(
            "SELECT value FROM test_table",
            sources,
            None, // No sink
            2,    // Parallelism
            2,    // Cluster nodes (remote)
        ).await;

        // Verify node mapping exists for remote execution (num_cluster_nodes > 1)
        assert!(node_mapping.is_some(), "Node mapping should exist for remote execution");
        
        let mapping = node_mapping.as_ref().unwrap();
        assert!(!mapping.is_empty(), "Node mapping should not be empty");

        // All edges should have channels (configured automatically, local or remote depending on node assignment)
        for edge in graph.get_edges().values() {
            assert!(edge.channel.is_some(), "Edge should have a channel");
        }
    }
} 