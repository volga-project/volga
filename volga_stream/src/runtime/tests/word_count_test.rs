use crate::{common::test_utils::{gen_unique_grpc_port, print_worker_metrics}, runtime::{
    execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionVertex}, functions::{
        key_by::KeyByFunction,
        reduce::{AggregationResultExtractor, AggregationType, ReduceFunction},
        source::word_count_source::{BatchingMode, WordCountSourceFunction},
    }, operators::{operator::OperatorConfig, sink::sink_operator::SinkConfig, source::source_operator::{SourceConfig, WordCountSourceConfig}}, partition::PartitionType, worker::{Worker, WorkerConfig}
}, transport::transport_backend_actor::TransportBackendType, storage::{InMemoryStorageClient, InMemoryStorageServer}};
use crate::common::message::{Message, KeyedMessage};
use crate::common::Key;
use crate::runtime::tests::graph_test_utils::{create_linear_test_execution_graph, TestLinearGraphConfig};
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
fn test_word_count() -> Result<()> {
    // Create runtime for async operations
    let runtime = Runtime::new()?;

    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());
    let parallelism = 2;
    let word_length = 10;
    let dictionary_size_per_source = 10; // Number of unique words per source vertex
    let num_to_send_per_word = 1000; // Number of copies of each word to send
    let batch_size = 10;

    // Define operator chain: source -> keyby -> reduce -> sink
    let operators = vec![
        OperatorConfig::SourceConfig(SourceConfig::WordCountSourceConfig(WordCountSourceConfig::new(
            word_length,
            dictionary_size_per_source,
            Some(num_to_send_per_word),
            None, // No time limit
            batch_size,
            BatchingMode::SameWord,
        ))),
        OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["word".to_string()])),
        OperatorConfig::ReduceConfig(
            ReduceFunction::new_arrow_reduce("word".to_string()),
            Some(AggregationResultExtractor::single_aggregation(
                AggregationType::Count,
                "count".to_string(),
            )),
        ),
        OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig(format!("http://{}", storage_server_addr))),
    ];

    let (graph, _) = runtime.block_on(create_linear_test_execution_graph(TestLinearGraphConfig {
        operators,
        parallelism,
        chained: true,
        is_remote: false,
        num_workers_per_operator: None,
    }));

    let vertex_ids = graph.get_vertices().keys().cloned().collect();
    // Create and start worker
    let worker_config = WorkerConfig::new(
        graph,
        vertex_ids,
        1,
        TransportBackendType::InMemory,
    );
    let mut worker = Worker::new(worker_config);

    let (result_map, worker_state) = runtime.block_on(async {
        let mut storage_server = InMemoryStorageServer::new();
        storage_server.start(&storage_server_addr).await.unwrap();
        worker.execute_worker_lifecycle_for_testing().await;
        let worker_state = worker.get_state().await;
        let mut client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await.unwrap();
        let result_map = client.get_map().await.unwrap();
        storage_server.stop().await;
        (result_map, worker_state)
    });

    print_worker_metrics(&worker_state);

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
    
    assert_eq!(word_counts.len(), dictionary_size_per_source * parallelism as usize, "Should have exactly num_words * parallelism unique words");
    
    let mut failed_words = Vec::new();
    for (word, count) in &word_counts {
        if (*count - num_to_send_per_word as f64).abs() > f64::EPSILON {
            failed_words.push((word.clone(), *count));
        }
    }
    
    if !failed_words.is_empty() {
        println!("Words that don't match expected count of {}:", num_to_send_per_word);
        for (word, count) in &failed_words {
            println!("  Word: '{}', Expected: {}, Actual: {}", word, num_to_send_per_word, count);
        }
        panic!("Found {} words with incorrect counts", failed_words.len());
    }

    Ok(())
} 