use crate::{common::test_utils::gen_unique_grpc_port, runtime::{
    execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionVertex}, functions::{
        key_by::KeyByFunction,
        map::{MapFunction, MapFunctionTrait},
    }, operators::{operator::OperatorConfig, sink::sink_operator::SinkConfig, source::source_operator::SourceConfig}, partition::PartitionType, storage::{InMemoryStorageClient, InMemoryStorageServer}, worker::{Worker, WorkerConfig}
}, transport::transport_backend_actor::TransportBackendType};
use crate::common::message::{Message, WatermarkMessage, KeyedMessage};
use crate::common::{test_utils::create_test_string_batch, MAX_WATERMARK_VALUE};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use tokio::runtime::Runtime;
use kameo::spawn;
use crate::transport::channel::Channel;
use arrow::array::StringArray;
use async_trait::async_trait;

#[derive(Debug, Clone)]
struct KeyedToRegularMapFunction;

#[async_trait]
impl MapFunctionTrait for KeyedToRegularMapFunction {
    fn map(&self, message: Message) -> Result<Message> {
        let value = message.record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0);
        println!("map rcvd, value {:?}", value);
        let upstream_vertex_id = message.upstream_vertex_id();
        let ingest_ts = message.ingest_timestamp();
        match message {
            Message::Keyed(keyed_message) => {
                // Create a new regular message with the same record batch
                Ok(Message::new(upstream_vertex_id, keyed_message.base.record_batch, ingest_ts))
            }
            _ => Ok(message), // Pass through non-keyed messages (like watermarks)
        }
    }
}

#[test]
fn test_worker_shutdown_with_watermarks() -> Result<()> {
    // Create runtime for async operations
    let runtime = Runtime::new()?;

    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());

    // Create execution graph
    let mut graph = ExecutionGraph::new();
    let parallelism = 2; // Number of parallel tasks
    let num_messages_per_source = 4; // Number of regular messages per source

    // Create test data for each source
    let mut source_messages = Vec::new();
    let mut msg_id = 0;
    for i in 0..parallelism as usize {
        let mut messages = Vec::new();
        // Add regular messages
        for _ in 0..num_messages_per_source {
            messages.push(Message::new(
                Some(format!("source_{}", i)),
                create_test_string_batch(vec![format!("value_{}", msg_id)]),
                None
            ));
            msg_id += 1;
        }
        // Add max watermark as the last message
        messages.push(Message::Watermark(WatermarkMessage::new(
            format!("source_{}", i),
            MAX_WATERMARK_VALUE,
            None,
        )));
        source_messages.push(messages);
    }

    // Create vertices for each parallel task
    for i in 0..parallelism {
        let task_id = i.to_string();
        
        // Create source vertex with vector source
        let source_vertex = ExecutionVertex::new(
            format!("source_{}", task_id),
            OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(source_messages[i as usize].clone())),
            parallelism,
            i,
        );
        graph.add_vertex(source_vertex);

        // Create key-by vertex using ArrowKeyByFunction
        let key_by_vertex = ExecutionVertex::new(
            format!("key_by_{}", task_id),
            OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["value".to_string()])),
            parallelism,
            i,
        );
        graph.add_vertex(key_by_vertex);

        // Create map vertex to transform keyed messages back to regular
        let map_vertex = ExecutionVertex::new(
            format!("map_{}", task_id),
            OperatorConfig::MapConfig(MapFunction::new_custom(KeyedToRegularMapFunction)),
            parallelism,
            i,
        );
        graph.add_vertex(map_vertex);

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
        // Connect each key-by to all map tasks
        for j in 0..parallelism {
            let map_id = format!("map_{}", j);
            let key_by_to_map = ExecutionEdge::new(
                key_by_id.clone(),
                map_id,
                "map".to_string(),
                PartitionType::Hash,
                Channel::Local {
                    channel_id: format!("key_by_{}_to_map_{}", i, j),
                },
            );
            graph.add_edge(key_by_to_map);
        }

        let map_id = format!("map_{}", i);
        // Connect each map to all sink tasks
        for j in 0..parallelism {
            let sink_id = format!("sink_{}", j);
            let map_to_sink = ExecutionEdge::new(
                map_id.clone(),
                sink_id,
                "sink".to_string(),
                PartitionType::RoundRobin,
                Channel::Local {
                    channel_id: format!("map_{}_to_sink_{}", i, j),
                },
            );
            graph.add_edge(map_to_sink);
        }
    }

    // Create and start worker
    let worker_config = WorkerConfig::new(
        graph,
        (0..parallelism).flat_map(|i| {
            vec![
                format!("source_{}", i),
                format!("key_by_{}", i),
                format!("map_{}", i),
                format!("sink_{}", i),
            ]
        }).collect(),
        1,
        TransportBackendType::InMemory,
    );
    let mut worker = Worker::new(worker_config);

    println!("Starting worker...");
    let (worker_state, result_messages) = runtime.block_on(async {
        let mut storage_server = InMemoryStorageServer::new();
        storage_server.start(&storage_server_addr).await.unwrap();
        worker.execute_worker_lifecycle_for_testing().await;
        let worker_state = worker.get_state().await;
        let mut client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await.unwrap();
        let result_messages = client.get_vector().await.unwrap();
        storage_server.stop().await;
        (worker_state, result_messages)
    });
    println!("Worker completed");

    // print metrics
    println!("\n=== Worker Metrics ===");
    println!("Task Statuses:");
    for (vertex_id, status) in &worker_state.task_statuses {
        println!("  {}: {:?}", vertex_id, status);
    }
    
    println!("\nTask Metrics:");
    for (vertex_id, metrics) in &worker_state.task_metrics {
        println!("  {}:", vertex_id);
        println!("    Messages: {}", metrics.num_messages);
        println!("    Records: {}", metrics.num_records);
        println!("    Latency Histogram: {:?}", metrics.latency_histogram);
    }
    
    println!("\nAggregated Metrics:");
    println!("  Total Messages: {}", worker_state.aggregated_metrics.total_messages);
    println!("  Total Records: {}", worker_state.aggregated_metrics.total_records);
    println!("  Latency Histogram: {:?}", worker_state.aggregated_metrics.latency_histogram);
    println!("===================\n");

    // Verify we received all messages except watermarks
    let result_len = result_messages.len();
    let mut values = Vec::new();
    for message in result_messages {
        let value = message.record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0);
        values.push(value.to_string());
    }

    println!("Result values: {:?}", values);
    let total_expected_messages = parallelism as usize * num_messages_per_source;
    assert_eq!(result_len, total_expected_messages, 
        "Expected {} messages, got {}", total_expected_messages, values.len());

    for msg_id in 0..(num_messages_per_source * parallelism as usize) {
        let value = format!("value_{}", msg_id);
        assert!(values.contains(&value));
    }

    Ok(())
} 