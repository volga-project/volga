use crate::{common::test_utils::gen_unique_grpc_port, runtime::{
    execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionVertex}, functions::{
        key_by::KeyByFunction,
        map::{MapFunction, MapFunctionTrait},
    }, operators::{operator::OperatorConfig, sink::sink_operator::SinkConfig, source::source_operator::SourceConfig}, partition::PartitionType, storage::{InMemoryStorageClient, InMemoryStorageServer}, worker::{Worker, WorkerConfig}
}, transport::transport_backend_actor::TransportBackendType};
use crate::common::message::{Message, WatermarkMessage, KeyedMessage};
use crate::common::{test_utils::create_test_string_batch, MAX_WATERMARK_VALUE};
use crate::runtime::tests::graph_test_utils::{create_test_execution_graph, TestGraphConfig};
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
    let parallelism = 2; // Number of parallel tasks
    let num_messages = 4; // Number of regular messages

    // Create single test data vector
    let mut source_messages = Vec::new();
    
    // Add regular messages
    for i in 0..num_messages {
        source_messages.push(Message::new(
            None,
            create_test_string_batch(vec![format!("value_{}", i)]),
            None
        ));
    }
    
    // Add max watermark as the last message
    source_messages.push(Message::Watermark(WatermarkMessage::new(
        "source".to_string(),
        MAX_WATERMARK_VALUE,
        None,
    )));

    // Define operator chain: source -> keyby -> map -> sink
    let operators = vec![
        ("source".to_string(), OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(source_messages))),
        ("keyby".to_string(), OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["value".to_string()]))),
        ("map".to_string(), OperatorConfig::MapConfig(MapFunction::new_custom(KeyedToRegularMapFunction))),
        ("sink".to_string(), OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig(format!("http://{}", storage_server_addr)))),
    ];

    let config = TestGraphConfig {
        operators,
        parallelism,
        chained: false,
        is_remote: false,
        num_workers_per_operator: None,
    };

    let (graph, _) = create_test_execution_graph(config);

    // Create and start worker
    let worker_config = WorkerConfig::new(
        graph,
        (0..parallelism).flat_map(|i| {
            vec![
                format!("source_{}", i),
                format!("keyby_{}", i),
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
    let expected_total_messages = num_messages * parallelism as usize;
    assert_eq!(result_len, expected_total_messages, 
        "Expected {} messages ({} unique values × {} parallelism), got {}", 
        expected_total_messages, num_messages, parallelism, values.len());

    // Verify all expected values are present, each appearing parallelism times
    for i in 0..num_messages {
        let value = format!("value_{}", i);
        let count = values.iter().filter(|&v| v == &value).count();
        assert_eq!(count, parallelism as usize, 
            "Expected value '{}' to appear {} times (parallelism), but found {} times", 
            value, parallelism, count);
    }

    Ok(())
} 