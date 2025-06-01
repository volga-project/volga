use crate::runtime::{
    execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionVertex, OperatorConfig, SourceConfig, SinkConfig}, 
    partition::PartitionType, 
    worker::Worker,
    functions::{
        key_by::KeyByFunction,
    },
    storage::in_memory_storage_actor::{InMemoryStorageActor, InMemoryStorageMessage, InMemoryStorageReply},
};
use crate::common::message::{Message, WatermarkMessage, KeyedMessage};
use crate::common::{test_utils::create_test_string_batch, MAX_WATERMARK_VALUE};
use anyhow::Result;
use std::collections::HashMap;
use tokio::runtime::Runtime;
use kameo::{Actor, spawn};
use crate::transport::channel::Channel;
use arrow::array::StringArray;

#[test]
fn test_worker_shutdown_with_watermarks() -> Result<()> {
    // Create runtime for async operations
    let runtime = Runtime::new()?;

    // Create storage actor
    let storage_actor = InMemoryStorageActor::new();
    let storage_ref = runtime.block_on(async {
        spawn(storage_actor)
    });

    // Create execution graph
    let mut graph = ExecutionGraph::new();
    let parallelism = 2i32; // Number of parallel tasks
    let num_messages_per_source = 5; // Number of regular messages per source

    // Create test data for each source
    let mut source_messages = Vec::new();
    for i in 0..parallelism as usize {
        let mut messages = Vec::new();
        // Add regular messages
        for j in 0..num_messages_per_source {
            messages.push(Message::new(
                Some(format!("source_{}", i)),
                create_test_string_batch(vec![format!("key_{}", j % 3)])? // Use modulo to create 3 unique keys
            ));
        }
        // Add max watermark as the last message
        messages.push(Message::Watermark(WatermarkMessage {
            source_vertex_id: format!("source_{}", i),
            watermark_value: MAX_WATERMARK_VALUE,
        }));
        source_messages.push(messages);
    }

    // Create vertices for each parallel task
    for i in 0..parallelism as usize {
        let task_id = i.to_string();
        
        // Create source vertex with vector source
        let source_vertex = ExecutionVertex::new(
            format!("source_{}", task_id),
            OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(source_messages[i].clone())),
            parallelism,
            i as i32,
        );
        graph.add_vertex(source_vertex);

        // Create key-by vertex
        let key_by_vertex = ExecutionVertex::new(
            format!("key_by_{}", task_id),
            OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["value".to_string()])),
            parallelism,
            i as i32,
        );
        graph.add_vertex(key_by_vertex);

        // Create sink vertex
        let sink_vertex = ExecutionVertex::new(
            format!("sink_{}", task_id),
            OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageActorSinkConfig(storage_ref.clone())),
            parallelism,
            i as i32,
        );
        graph.add_vertex(sink_vertex);

        // Add edges
        let source_to_key_by = ExecutionEdge::new(
            format!("source_{}", task_id),
            format!("key_by_{}", task_id),
            PartitionType::Forward,
            Channel::Local {
                channel_id: format!("source_to_key_by_{}", task_id),
            },
        );
        graph.add_edge(source_to_key_by)?;

        let key_by_to_sink = ExecutionEdge::new(
            format!("key_by_{}", task_id),
            format!("sink_{}", task_id),
            PartitionType::Hash,
            Channel::Local {
                channel_id: format!("key_by_to_sink_{}", task_id),
            },
        );
        graph.add_edge(key_by_to_sink)?;
    }

    // Create and start worker
    let mut worker = Worker::new(
        graph,
        (0..parallelism).flat_map(|i| {
            vec![
                format!("source_{}", i),
                format!("key_by_{}", i),
                format!("sink_{}", i),
            ]
        }).collect(),
        1,
    );

    println!("Starting worker...");
    worker.execute()?;
    println!("Worker completed");

    // Verify results by reading from storage actor
    let result = runtime.block_on(async {
        storage_ref.ask(InMemoryStorageMessage::GetMap).await
    })?;
    
    match result {
        InMemoryStorageReply::Map(result_map) => {
            // Verify we received all messages
            let total_expected_messages = parallelism as usize * num_messages_per_source;
            assert_eq!(result_map.len(), total_expected_messages, 
                "Expected {} messages, got {}", total_expected_messages, result_map.len());

            // Group messages by key to verify key-by partitioning
            let mut key_counts: HashMap<String, usize> = HashMap::new();
            for (_, batch) in result_map {
                let keyed_batch = match batch {
                    Message::Keyed(kb) => kb,
                    _ => panic!("Expected KeyedMessage"),
                };
                
                // Get the key value
                let key_batch = keyed_batch.key().record_batch();
                let key_array = key_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                let key = key_array.value(0).to_string();
                
                // Increment count for this key
                *key_counts.entry(key).or_insert(0) += 1;
            }

            // Verify each key appears the expected number of times
            // Each key should appear (num_messages_per_source * parallelism) / 3 times
            // because we used modulo 3 to create the keys
            let expected_count_per_key = (num_messages_per_source * parallelism as usize) / 3;
            for (key, count) in key_counts {
                assert_eq!(count, expected_count_per_key,
                    "Key '{}' should appear {} times, got {}", key, expected_count_per_key, count);
            }
        }
        _ => panic!("Expected Map reply from storage actor"),
    }

    Ok(())
} 