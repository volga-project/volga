use crate::runtime::tests::graph_test_utils::{
    create_test_execution_graph, TestGraphConfig, 
    create_operator_based_worker_distribution
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

#[test]
fn test_create_simple_graph() {
    // Create test messages
    let test_messages = vec![
        Message::new(None, create_test_string_batch(vec!["test1".to_string()]), None),
        Message::new(None, create_test_string_batch(vec!["test2".to_string()]), None),
    ];

    // Define operator chain: source -> map -> sink
    let operators = vec![
        ("source".to_string(), OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(test_messages))),
        ("map".to_string(), OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction))),
        ("sink".to_string(), OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig("http://127.0.0.1:8080".to_string()))),
    ];

    let config = TestGraphConfig {
        operators,
        parallelism: 2,
        chained: false,
        is_remote: false,
        num_workers_per_operator: None,
    };

    let (graph, _) = create_test_execution_graph(config);

    // Verify vertices
    assert_eq!(graph.get_vertices().len(), 6); // 3 operators * 2 parallelism
    assert!(graph.get_vertex("source_0").is_some());
    assert!(graph.get_vertex("source_1").is_some());
    assert!(graph.get_vertex("map_0").is_some());
    assert!(graph.get_vertex("map_1").is_some());
    assert!(graph.get_vertex("sink_0").is_some());
    assert!(graph.get_vertex("sink_1").is_some());

    // Verify edges
    assert_eq!(graph.get_edges().len(), 8); // 2 source -> 2 map + 2 map -> 2 sink

    // Verify partition types for edges
    for edge in graph.get_edges().values() {
        // source -> map and map -> sink should use RoundRobin partitioning
        assert!(matches!(edge.partition_type, crate::runtime::partition::PartitionType::RoundRobin),
            "Edge {} -> {} should use RoundRobin partitioning", edge.source_vertex_id, edge.target_vertex_id);
    }
}

#[test]
fn test_create_chained_graph() {
    // Create test messages
    let test_messages = vec![
        Message::new(None, create_test_string_batch(vec!["test1".to_string()]), None),
        Message::new(None, create_test_string_batch(vec!["test2".to_string()]), None),
    ];

    // Define operator chain: source -> map1 -> map2 -> sink
    let operators = vec![
        ("source".to_string(), OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(test_messages))),
        ("map1".to_string(), OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction))),
        ("map2".to_string(), OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction))),
        ("sink".to_string(), OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig("http://127.0.0.1:8080".to_string()))),
    ];

    let config = TestGraphConfig {
        operators,
        parallelism: 2,
        chained: true,
        is_remote: false,
        num_workers_per_operator: None,
    };

    let (graph, _) = create_test_execution_graph(config);

    // Verify vertices - should be chained into a single chain operator (containing source, map1, map2, sink)
    assert_eq!(graph.get_vertices().len(), 2); // 1 group * 2 parallelism
    assert!(graph.get_vertex("chain_source->map1->map2->sink_0").is_some());
    assert!(graph.get_vertex("chain_source->map1->map2->sink_1").is_some());

    // Verify no edges since everything is chained together
    assert_eq!(graph.get_edges().len(), 0);
}

#[test]
fn test_create_chained_graph_with_keyby() {
    // Create test messages
    let test_messages = vec![
        Message::new(None, create_test_string_batch(vec!["test1".to_string()]), None),
        Message::new(None, create_test_string_batch(vec!["test2".to_string()]), None),
    ];

    // Define operator chain: source -> map1 -> keyby -> map2 -> sink
    let operators = vec![
        ("source".to_string(), OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(test_messages))),
        ("map1".to_string(), OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction))),
        ("keyby".to_string(), OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["value".to_string()]))),
        ("map2".to_string(), OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction))),
        ("sink".to_string(), OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig("http://127.0.0.1:8080".to_string()))),
    ];

    let config = TestGraphConfig {
        operators,
        parallelism: 2,
        chained: true,
        is_remote: false,
        num_workers_per_operator: None,
    };

    let (graph, _) = create_test_execution_graph(config);

    // Verify vertices - KeyBy should break the chain
    // source -> map1 -> keyby -> map2 -> sink becomes: chain_source->map1->keyby -> chain_map2->sink
    assert_eq!(graph.get_vertices().len(), 4); // 2 groups * 2 parallelism
    assert!(graph.get_vertex("chain_source->map1->keyby_0").is_some());
    assert!(graph.get_vertex("chain_source->map1->keyby_1").is_some());
    assert!(graph.get_vertex("chain_map2->sink_0").is_some());
    assert!(graph.get_vertex("chain_map2->sink_1").is_some());

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

#[test]
fn test_create_graph_with_keyby() {
    // Create test messages
    let test_messages = vec![
        Message::new(None, create_test_string_batch(vec!["test1".to_string()]), None),
        Message::new(None, create_test_string_batch(vec!["test2".to_string()]), None),
    ];

    // Define operator chain: source -> keyby -> map -> sink
    let operators = vec![
        ("source".to_string(), OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(test_messages))),
        ("keyby".to_string(), OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["value".to_string()]))),
        ("map".to_string(), OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction))),
        ("sink".to_string(), OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig("http://127.0.0.1:8080".to_string()))),
    ];

    let config = TestGraphConfig {
        operators,
        parallelism: 2,
        chained: false,
        is_remote: false,
        num_workers_per_operator: None,
    };

    let (graph, _) = create_test_execution_graph(config);

    // Verify vertices
    assert_eq!(graph.get_vertices().len(), 8); // 4 operators * 2 parallelism

    // Verify edges - keyby should use hash partitioning
    assert_eq!(graph.get_edges().len(), 12); // 2 source -> 2 keyby + 2 keyby -> 2 map + 2 map -> 2 sink

    // Check partition types for all edges
    for edge in graph.get_edges().values() {
        if edge.source_vertex_id.starts_with("source") && edge.target_vertex_id.starts_with("keyby") {
            // source -> keyby should use RoundRobin
            assert!(matches!(edge.partition_type, crate::runtime::partition::PartitionType::RoundRobin),
                "Edge {} -> {} should use RoundRobin partitioning", edge.source_vertex_id, edge.target_vertex_id);
        } else if edge.source_vertex_id.starts_with("keyby") && edge.target_vertex_id.starts_with("map") {
            // keyby -> map should use Hash partitioning
            assert!(matches!(edge.partition_type, crate::runtime::partition::PartitionType::Hash),
                "Edge {} -> {} should use Hash partitioning", edge.source_vertex_id, edge.target_vertex_id);
        } else if edge.source_vertex_id.starts_with("map") && edge.target_vertex_id.starts_with("sink") {
            // map -> sink should use RoundRobin
            assert!(matches!(edge.partition_type, crate::runtime::partition::PartitionType::RoundRobin),
                "Edge {} -> {} should use RoundRobin partitioning", edge.source_vertex_id, edge.target_vertex_id);
        } else {
            panic!("Unexpected edge: {} -> {}", edge.source_vertex_id, edge.target_vertex_id);
        }
    }
}

#[test]
fn test_create_remote_graph() {
    // Create test messages
    let test_messages = vec![
        Message::new(None, create_test_string_batch(vec!["test1".to_string()]), None),
        Message::new(None, create_test_string_batch(vec!["test2".to_string()]), None),
    ];

    // Define operator chain: source -> map -> sink
    let operators = vec![
        ("source".to_string(), OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(test_messages))),
        ("map".to_string(), OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction))),
        ("sink".to_string(), OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig("http://127.0.0.1:8080".to_string()))),
    ];

    let num_operators = operators.len();

    let num_workers_per_operator = 2;
    let parallelism_per_worker = 2;
    let total_parallelism = num_workers_per_operator * parallelism_per_worker;

    let config = TestGraphConfig {
        operators,
        parallelism: total_parallelism,
        chained: false,
        is_remote: true,
        num_workers_per_operator: Some(num_workers_per_operator),
    };

    let (graph, _) = create_test_execution_graph(config);

    // Verify vertices
    assert_eq!(graph.get_vertices().len(), total_parallelism * num_operators);

    // Verify edges use remote channels
    for edge in graph.get_edges().values() {
        match &edge.channel {
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

#[test]
fn test_create_operator_based_worker_distribution() {
    // Create test messages
    let test_messages = vec![
        Message::new(None, create_test_string_batch(vec!["test1".to_string()]), None),
        Message::new(None, create_test_string_batch(vec!["test2".to_string()]), None),
    ];

    // Define operator chain: source -> map -> sink
    let operators = vec![
        ("source".to_string(), OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(test_messages))),
        ("map".to_string(), OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction))),
        ("sink".to_string(), OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig("http://127.0.0.1:8080".to_string()))),
    ];

    let worker_distribution = create_operator_based_worker_distribution(2, &operators, 2);

    // Verify distribution
    assert_eq!(worker_distribution.len(), 6); // 3 operators * 2 workers per operator

    // Check that each worker has vertices of the same operator type
    for (worker_id, vertex_ids) in &worker_distribution {
        if worker_id.starts_with("worker_0") || worker_id.starts_with("worker_1") {
            // Source workers
            for vertex_id in vertex_ids {
                assert!(vertex_id.starts_with("source"));
            }
        } else if worker_id.starts_with("worker_2") || worker_id.starts_with("worker_3") {
            // Map workers
            for vertex_id in vertex_ids {
                assert!(vertex_id.starts_with("map"));
            }
        } else if worker_id.starts_with("worker_4") || worker_id.starts_with("worker_5") {
            // Sink workers
            for vertex_id in vertex_ids {
                assert!(vertex_id.starts_with("sink"));
            }
        }
    }
}

#[test]
fn test_create_graph_with_reduce_chained() {
    // Create test messages
    let test_messages = vec![
        Message::new(None, create_test_string_batch(vec!["test1".to_string()]), None),
        Message::new(None, create_test_string_batch(vec!["test2".to_string()]), None),
    ];

    // Define operator chain: source -> keyby -> reduce -> sink
    let operators = vec![
        ("source".to_string(), OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(test_messages))),
        ("keyby".to_string(), OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["value".to_string()]))),
        ("reduce".to_string(), OperatorConfig::ReduceConfig(ReduceFunction::new_arrow_reduce("value".to_string()), None)),
        ("sink".to_string(), OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig("http://127.0.0.1:8080".to_string()))),
    ];

    let config = TestGraphConfig {
        operators,
        parallelism: 2,
        chained: true,
        is_remote: false,
        num_workers_per_operator: None,
    };

    let (graph, _) = create_test_execution_graph(config);

    // Verify vertices
    assert_eq!(graph.get_vertices().len(), 4); // 2 chained operators * 2 parallelism

    assert_eq!(graph.get_edges().len(), 4);

    // Verify partition types for edges
    for edge in graph.get_edges().values() {
        assert!(matches!(edge.partition_type, crate::runtime::partition::PartitionType::Hash),
            "Edge {} -> {} should use Hash partitioning, but got {:?}", edge.source_vertex_id, edge.target_vertex_id, edge.partition_type);
    }
}