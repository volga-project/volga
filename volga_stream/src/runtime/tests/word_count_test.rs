use crate::{common::test_utils::gen_unique_grpc_port, runtime::{
    execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionVertex, OperatorConfig, SinkConfig, SourceConfig}, functions::{
        key_by::KeyByFunction,
        reduce::{AggregationResultExtractor, AggregationType, ReduceFunction},
        source::word_count_source::{BatchingMode, WordCountSourceFunction},
    }, partition::PartitionType, storage::{InMemoryStorageClient, InMemoryStorageServer}, worker::Worker
}};
use crate::common::message::{Message, KeyedMessage};
use crate::common::Key;
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

    // Create execution graph
    let mut graph = ExecutionGraph::new();
    let parallelism = 2;
    let word_length = 10;
    let num_words = 2; // Number of unique words
    let num_to_send_per_word = 10; // Number of copies of each word to send
    let batch_size = 1;

    // Create vertices for each parallel task
    for i in 0..parallelism {
        let task_id = i.to_string();
        
        // Create source vertex with word count source
        let source_vertex = ExecutionVertex::new(
            format!("source_{}", task_id),
            OperatorConfig::SourceConfig(SourceConfig::WordCountSourceConfig {
                word_length: word_length,
                num_words,
                num_to_send_per_word: Some(num_to_send_per_word),
                run_for_s: None,     // No time limit
                batch_size: batch_size,
                batching_mode: BatchingMode::SameWord,
            }),
            parallelism,
            i,
        );
        graph.add_vertex(source_vertex);

        // Create key-by vertex using ArrowKeyByFunction
        let key_by_vertex = ExecutionVertex::new(
            format!("key_by_{}", task_id),
            OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["word".to_string()])),
            parallelism,
            i,
        );
        graph.add_vertex(key_by_vertex);

        // Create reduce vertex using ArrowReduceFunction
        let reduce_vertex = ExecutionVertex::new(
            format!("reduce_{}", task_id),
            OperatorConfig::ReduceConfig(
                ReduceFunction::new_arrow_reduce("word".to_string()),
                Some(AggregationResultExtractor::single_aggregation(
                    AggregationType::Count,
                    "count".to_string(),
                )),
            ),
            parallelism,
            i,
        );
        graph.add_vertex(reduce_vertex);

        // Create sink vertex
        let sink_vertex = ExecutionVertex::new(
            format!("sink_{}", task_id),
            OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig(format!("http://{}", storage_server_addr))),
            parallelism,
            i,
        );
        graph.add_vertex(sink_vertex);
    }

    // Add edges connecting vertices across parallel tasks
    for i in 0..parallelism {
        let source_id = format!("source_{}", i);
        
        // Connect each source to all key-by tasks
        for j in 0..parallelism {
            let key_by_id = format!("key_by_{}", j);
            let source_to_key_by = ExecutionEdge::new(
                source_id.clone(),
                key_by_id,
                "key_by".to_string(),
                PartitionType::RoundRobin,
                Channel::Local {
                    channel_id: format!("source_{}_to_key_by_{}", i, j),
                },
            );
            graph.add_edge(source_to_key_by);
        }

        let key_by_id = format!("key_by_{}", i);
        // Connect each key-by to all reduce tasks
        for j in 0..parallelism {
            let reduce_id = format!("reduce_{}", j);
            let key_by_to_reduce = ExecutionEdge::new(
                key_by_id.clone(),
                reduce_id,
                "reduce".to_string(),
                PartitionType::Hash,
                Channel::Local {
                    channel_id: format!("key_by_{}_to_reduce_{}", i, j),
                },
            );
            graph.add_edge(key_by_to_reduce);
        }

        let reduce_id = format!("reduce_{}", i);

        // TODO having round robin here makes same keys end up in different sinks - leads to write 
        // races and inconsistent resulst - figure out how to fix this
        // Connect each reduce to all sink tasks
        for j in 0..parallelism {
            let sink_id = format!("sink_{}", j);
            let reduce_to_sink = ExecutionEdge::new(
                reduce_id.clone(),
                sink_id,
                "sink".to_string(),
                PartitionType::RoundRobin,
                Channel::Local {
                    channel_id: format!("reduce_{}_to_sink_{}", i, j),
                },
            );
            graph.add_edge(reduce_to_sink);
        }
    }

    // Create and start worker
    let mut worker = Worker::new(
        graph,
        (0..parallelism).flat_map(|i| {
            vec![
                format!("source_{}", i),
                format!("key_by_{}", i),
                format!("reduce_{}", i),
                format!("sink_{}", i),
            ]
        }).collect(),
        1,
    );

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