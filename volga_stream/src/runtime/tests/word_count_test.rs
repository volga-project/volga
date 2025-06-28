use crate::{common::test_utils::gen_unique_grpc_port, runtime::{
    execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionVertex}, functions::{
        key_by::KeyByFunction,
        reduce::{AggregationResultExtractor, AggregationType, ReduceFunction},
        source::word_count_source::{BatchingMode, WordCountSourceFunction},
    }, operators::{operator::OperatorConfig, sink::sink_operator::SinkConfig, source::source_operator::SourceConfig}, partition::PartitionType, storage::{InMemoryStorageClient, InMemoryStorageServer}, worker::{Worker, WorkerConfig}
}, transport::transport_backend_actor::TransportBackendType};
use crate::common::message::{Message, KeyedMessage};
use crate::common::Key;
use crate::runtime::tests::graph_test_utils::{create_test_execution_graph, TestGraphConfig};
use anyhow::Result;
use std::collections::HashMap;
use tokio::runtime::Runtime;
use kameo::{Actor, spawn};
use crate::transport::channel::Channel;
use async_trait::async_trait;
use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;

// TODO: we may have colliding words since each source worker generates it's own
#[test]
fn test_parallel_word_count() -> Result<()> {
    // Create runtime for async operations
    let runtime = Runtime::new()?;

    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());
    let parallelism = 2;
    let word_length = 10;
    let num_words = 2; // Number of unique words
    let num_to_send_per_word = 10; // Number of copies of each word to send
    let batch_size = 1;

    // Define operator chain: source -> keyby -> reduce -> sink
    let operators = vec![
        ("source".to_string(), OperatorConfig::SourceConfig(SourceConfig::WordCountSourceConfig {
            word_length: word_length,
            num_words,
            num_to_send_per_word: Some(num_to_send_per_word),
            run_for_s: None,     // No time limit
            batch_size: batch_size,
            batching_mode: BatchingMode::SameWord,
        })),
        ("keyby".to_string(), OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["word".to_string()]))),
        ("reduce".to_string(), OperatorConfig::ReduceConfig(
            ReduceFunction::new_arrow_reduce("word".to_string()),
            Some(AggregationResultExtractor::single_aggregation(
                AggregationType::Count,
                "count".to_string(),
            )),
        )),
        ("sink".to_string(), OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig(format!("http://{}", storage_server_addr)))),
    ];

    let config = TestGraphConfig {
        operators,
        parallelism,
        chained: false,
        is_remote: false,
        worker_vertex_distribution: None,
    };

    let graph = create_test_execution_graph(config);

    // Create and start worker
    let worker_config = WorkerConfig::new(
        graph,
        (0..parallelism).flat_map(|i| {
            vec![
                format!("source_{}", i),
                format!("keyby_{}", i),
                format!("reduce_{}", i),
                format!("sink_{}", i),
            ]
        }).collect(),
        1,
        TransportBackendType::InMemory,
    );
    let mut worker = Worker::new(worker_config);

    let result_map = runtime.block_on(async {
        let mut storage_server = InMemoryStorageServer::new();
        storage_server.start(&storage_server_addr).await.unwrap();
        worker.execute_worker_lifecycle_for_testing().await;
        let mut client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await.unwrap();
        let result_map = client.get_map().await.unwrap();
        storage_server.stop().await;
        result_map
    });
    println!("Result map len: {:?}", result_map.len());
    // Count occurrences of each word
    let mut word_counts = HashMap::new();
    for (_, batch) in result_map {
        // Get the keyed batch and extract the word from its key
        let keyed_batch = match batch {
            Message::Keyed(kb) => kb,
            _ => panic!("Expected KeyedBatch"),
        };
        
        // Extract word from the key's record batch
        let key_batch = keyed_batch.key().record_batch();
        let word_array = key_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let word = word_array.value(0).to_string();
        // Get count from the single column in the batch
        let count_array = keyed_batch.base.record_batch.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        let count = count_array.value(0);
        
        word_counts.insert(word, count);
    }
    
    assert_eq!(word_counts.len(), num_words * parallelism as usize, "Should have exactly num_words * parallelism unique words");
    
    for (word, count) in &word_counts {
        assert_eq!(*count, num_to_send_per_word as f64, 
            "Word '{}' should appear exactly {} times", word, num_to_send_per_word);
    }

    Ok(())
} 