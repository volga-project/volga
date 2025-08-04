use crate::runtime::operators::source::source_operator::VectorSourceConfig;
use crate::runtime::tests::graph_test_utils::{
    create_linear_test_execution_graph, TestLinearGraphConfig, 
    // create_operator_based_worker_distribution
};
use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::functions::{
    map::{MapFunction, MapFunctionTrait},
    key_by::KeyByFunction,
    reduce::{ReduceFunction, AggregationResultExtractor},
};
use crate::runtime::operators::{
    source::source_operator::SourceConfig,
    sink::sink_operator::SinkConfig,
};
use crate::common::test_utils::create_test_string_batch;
use crate::common::message::Message;
use std::collections::HashMap;
use async_trait::async_trait;

#[derive(Debug, Clone)]
struct IdentityMapFunction;

#[async_trait]
impl MapFunctionTrait for IdentityMapFunction {
    fn map(&self, message: Message) -> anyhow::Result<Message> {
        Ok(message)
    }
}

// TODO these tests are mostly for creating execution graphs from operator configs - move them to logical graph tests

#[tokio::test]
async fn test_create_simple_graph() {
    // Create test messages
    let test_messages = vec![
        Message::new(None, create_test_string_batch(vec!["test1".to_string()]), None),
        Message::new(None, create_test_string_batch(vec!["test2".to_string()]), None),
    ];

    // Define operator chain: source -> map -> sink
    let operators = vec![
        OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(VectorSourceConfig::new(test_messages))),
        OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
        OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig("http://127.0.0.1:8080".to_string())),
    ];

    let config = TestLinearGraphConfig {
        operators,
        parallelism: 2,
        chained: false,
        is_remote: false,
        num_workers_per_operator: None,
    };

    let (graph, _) = create_linear_test_execution_graph(config).await;

    // Verify vertices
    assert_eq!(graph.get_vertices().len(), 6); // 3 operators * 2 parallelism

    // Verify vertex types
    let mut source_count = 0;
    let mut map_count = 0;
    let mut sink_count = 0;

    for vertex in graph.get_vertices().values() {
        match &vertex.operator_config {
            OperatorConfig::SourceConfig(_) => source_count += 1,
            OperatorConfig::MapConfig(_) => map_count += 1,
            OperatorConfig::SinkConfig(_) => sink_count += 1,
            _ => {}
        }
    }

    assert_eq!(source_count, 2, "Expected 2 source vertices");
    assert_eq!(map_count, 2, "Expected 2 map vertices"); 
    assert_eq!(sink_count, 2, "Expected 2 sink vertices");

    // Verify edges
    assert_eq!(graph.get_edges().len(), 8); // 2 source -> 2 map + 2 map -> 2 sink

    // Verify partition types for edges
    for edge in graph.get_edges().values() {
        // source -> map and map -> sink should use RoundRobin partitioning
        assert!(matches!(edge.partition_type, crate::runtime::partition::PartitionType::RoundRobin),
            "Edge {} -> {} should use RoundRobin partitioning", edge.source_vertex_id, edge.target_vertex_id);
    }
}

#[tokio::test]
async fn test_create_chained_graph() {
    // Create test messages
    let test_messages = vec![
        Message::new(None, create_test_string_batch(vec!["test1".to_string()]), None),
        Message::new(None, create_test_string_batch(vec!["test2".to_string()]), None),
    ];

    // Define operator chain: source -> map1 -> map2 -> sink
    let operators = vec![
        OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(VectorSourceConfig::new(test_messages))),
        OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
        OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
        OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig("http://127.0.0.1:8080".to_string())),
    ];

    let config = TestLinearGraphConfig {
        operators,
        parallelism: 2,
        chained: true,
        is_remote: false,
        num_workers_per_operator: None,
    };

    let (graph, _) = create_linear_test_execution_graph(config).await;

    // Verify vertices - should be chained into a single chain operator (containing source, map1, map2, sink)
    assert_eq!(graph.get_vertices().len(), 2); // 1 group * 2 parallelism

    // Verify chained configs in vertices
    let vertices = graph.get_vertices().values();
    let chain_configs = vertices.filter(|v| {
        if let OperatorConfig::ChainedConfig(chain) = &v.operator_config {
            chain.len() == 4 && 
            matches!(chain[0], OperatorConfig::SourceConfig(_)) &&
            matches!(chain[1], OperatorConfig::MapConfig(_)) &&
            matches!(chain[2], OperatorConfig::MapConfig(_)) &&
            matches!(chain[3], OperatorConfig::SinkConfig(_))
        } else {
            false
        }
    }).count();
    assert_eq!(chain_configs, 2); // 2 parallel instances of the chain

    // Verify no edges since everything is chained together
    assert_eq!(graph.get_edges().len(), 0);
}

#[tokio::test]
async fn test_create_chained_graph_with_keyby() {
    // Create test messages
    let test_messages = vec![
        Message::new(None, create_test_string_batch(vec!["test1".to_string()]), None),
        Message::new(None, create_test_string_batch(vec!["test2".to_string()]), None),
    ];

    // Define operator chain: source -> map1 -> keyby -> map2 -> sink
    let operators = vec![
        OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(VectorSourceConfig::new(test_messages))),
        OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
        OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["value".to_string()])),
        OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
        OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig("http://127.0.0.1:8080".to_string())),
    ];

    let config = TestLinearGraphConfig {
        operators,
        parallelism: 2,
        chained: true,
        is_remote: false,
        num_workers_per_operator: None,
    };

    let (graph, _) = create_linear_test_execution_graph(config).await;

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
async fn test_create_graph_with_keyby() {
    // Create test messages
    let test_messages = vec![
        Message::new(None, create_test_string_batch(vec!["test1".to_string()]), None),
        Message::new(None, create_test_string_batch(vec!["test2".to_string()]), None),
    ];

    // Define operator chain: source -> keyby -> map -> sink
    let operators = vec![
        OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(VectorSourceConfig::new(test_messages))),
        OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["value".to_string()])),
        OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
        OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig("http://127.0.0.1:8080".to_string())),
    ];

    let config = TestLinearGraphConfig {
        operators,
        parallelism: 2,
        chained: false,
        is_remote: false,
        num_workers_per_operator: None,
    };

    let (graph, _) = create_linear_test_execution_graph(config).await;

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

#[tokio::test]
async fn test_create_remote_graph() {
    // Create test messages
    let test_messages = vec![
        Message::new(None, create_test_string_batch(vec!["test1".to_string()]), None),
        Message::new(None, create_test_string_batch(vec!["test2".to_string()]), None),
    ];

    // Define operator chain: source -> map -> sink
    let operators = vec![
        OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(VectorSourceConfig::new(test_messages))),
        OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
        OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig("http://127.0.0.1:8080".to_string())),
    ];

    let num_operators = operators.len();

    let num_workers_per_operator = 2;
    let parallelism_per_worker = 2;
    let total_parallelism = num_workers_per_operator * parallelism_per_worker;

    let config = TestLinearGraphConfig {
        operators,
        parallelism: total_parallelism,
        chained: false,
        is_remote: true,
        num_workers_per_operator: Some(num_workers_per_operator),
    };

    let (graph, _) = create_linear_test_execution_graph(config).await;

    // Verify vertices
    assert_eq!(graph.get_vertices().len(), total_parallelism * num_operators);

    // Verify edges use remote channels
    for edge in graph.get_edges().values() {
        let channel = edge.channel.as_ref().expect("channel should be present");
        match channel {
            crate::transport::channel::Channel::Remote { .. } => {
                // This is expected for remote channels
            }
            crate::transport::channel::Channel::Local { .. } => {
                panic!("Expected remote channels but got local");
            }
        }
        
        // Verify partition types - all should use RoundRobin for source -> map -> sink
        assert!(matches!(edge.partition_type, crate::runtime::partition::PartitionType::RoundRobin),
            "Edge {} -> {} should use RoundRobin partitioning", edge.source_vertex_id, edge.target_vertex_id);
    }
}

#[tokio::test]
async fn test_create_graph_with_reduce_chained() {
    // Create test messages
    let test_messages = vec![
        Message::new(None, create_test_string_batch(vec!["test1".to_string()]), None),
        Message::new(None, create_test_string_batch(vec!["test2".to_string()]), None),
    ];

    // Define operator chain: source -> keyby -> reduce -> sink
    let operators = vec![
        OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(VectorSourceConfig::new(test_messages))),
        OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["value".to_string()])),
        OperatorConfig::ReduceConfig(ReduceFunction::new_arrow_reduce("value".to_string()), None),
        OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig("http://127.0.0.1:8080".to_string())),
    ];

    let config = TestLinearGraphConfig {
        operators,
        parallelism: 2,
        chained: true,
        is_remote: false,
        num_workers_per_operator: None,
    };

    let (graph, _) = create_linear_test_execution_graph(config).await;

    // Verify vertices
    assert_eq!(graph.get_vertices().len(), 4); // 2 chained operators * 2 parallelism

    assert_eq!(graph.get_edges().len(), 4);

    // Verify partition types for edges
    for edge in graph.get_edges().values() {
        assert!(matches!(edge.partition_type, crate::runtime::partition::PartitionType::Hash),
            "Edge {} -> {} should use Hash partitioning, but got {:?}", edge.source_vertex_id, edge.target_vertex_id, edge.partition_type);
    }
}