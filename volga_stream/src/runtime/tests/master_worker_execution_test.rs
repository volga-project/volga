use crate::{cluster::node_assignment::node_to_vertex_ids, common::test_utils::{create_test_string_batch, gen_unique_grpc_port}, runtime::{
    execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionVertex}, functions::{
        key_by::KeyByFunction,
        map::{MapFunction, MapFunctionTrait},
    }, master::Master, operators::{operator::OperatorConfig, sink::sink_operator::SinkConfig, source::source_operator::{SourceConfig, VectorSourceConfig}}, partition::PartitionType, storage::{InMemoryStorageClient, InMemoryStorageServer}, worker::WorkerConfig, worker_server::WorkerServer
}, transport::transport_backend_actor::TransportBackendType};
use crate::common::message::{Message, WatermarkMessage};
use crate::common::MAX_WATERMARK_VALUE;
use crate::runtime::tests::graph_test_utils::{create_linear_test_execution_graph, TestLinearGraphConfig};
use anyhow::Result;
use std::collections::HashMap;
use tokio::runtime::Runtime;
use crate::transport::channel::Channel;
use arrow::array::StringArray;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

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

/// Starts worker servers and returns their addresses
async fn start_worker_servers(
    num_workers_per_operator: usize,
    parallelism_per_worker: usize,
    num_messages_per_source: usize,
    storage_server_addr: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut worker_servers = Vec::new();
    let mut worker_addresses = Vec::new();

    // Create single test data vector
    let mut source_messages = Vec::new();
    
    // Add regular messages
    for i in 0..num_messages_per_source {
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

    let operators = vec![
        OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(VectorSourceConfig::new(source_messages))),
        OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["value".to_string()])),
        OperatorConfig::MapConfig(MapFunction::new_custom(KeyedToRegularMapFunction)),
        OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig(format!("http://{}", storage_server_addr))),
    ];

    // Create execution graph
    let (graph, vertex_to_node) = create_linear_test_execution_graph(TestLinearGraphConfig {
        operators,
        parallelism: num_workers_per_operator * parallelism_per_worker,
        chained: false,
        is_remote: true,
        num_workers_per_operator: Some(num_workers_per_operator),
    }).await;

    let vertex_to_node = vertex_to_node.unwrap();
    let node_to_vertex_ids = node_to_vertex_ids(&vertex_to_node);
    
    // Start worker servers
    for node_id in node_to_vertex_ids.keys() {
        let port = gen_unique_grpc_port();
        let addr = format!("127.0.0.1:{}", port);
        
        let vertex_ids = node_to_vertex_ids.get(node_id).unwrap().clone();

        // Create and start worker server
        let worker_config = WorkerConfig::new(
            graph.clone(),
            vertex_ids,
            1,
            TransportBackendType::Grpc,
        );
        let mut worker_server = WorkerServer::new(worker_config);
        worker_server.start(&addr).await?;
        
        worker_servers.push(worker_server);
        worker_addresses.push(addr.clone());
        
        println!("[TEST] Started worker server for node {} on {}", node_id, addr);
    }

    // Store worker servers in a static to prevent them from being dropped
    lazy_static::lazy_static! {
        static ref WORKER_SERVERS: Arc<Mutex<Vec<WorkerServer>>> = Arc::new(Mutex::new(Vec::new()));
    }
    
    {
        let mut servers = WORKER_SERVERS.lock().await;
        *servers = worker_servers;
    }

    Ok(worker_addresses)
}

#[test]
fn test_master_worker_execution() -> Result<()> {
    let num_workers_per_operator = 2;
    let parallelism_per_worker = 2;
    let num_messages_per_source = 10; // TODO large number fails, debug why
    
    // Create runtime for async operations
    let runtime = Runtime::new()?;

    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());

    let (_, result_messages) = runtime.block_on(async {
        // Start storage server
        let mut storage_server = InMemoryStorageServer::new();
        storage_server.start(&storage_server_addr).await.unwrap();
        println!("[TEST] Started storage server on {}", storage_server_addr);

        // Start worker servers
        let worker_addresses = start_worker_servers(num_workers_per_operator, parallelism_per_worker, num_messages_per_source, &storage_server_addr).await.unwrap();
        println!("[TEST] Started {} worker servers", worker_addresses.len());

        // Wait a bit for servers to start
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Create and execute master
        let mut master = Master::new();
        println!("[TEST] Starting master execution");
        
        match master.execute(worker_addresses.clone()).await {
            Ok(_) => println!("[TEST] Master execution completed successfully"),
            Err(e) => {
                println!("[TEST] Master execution failed: {}", e);
                // Continue to get results even if master fails
            }
        }

        // Get results from storage
        let mut client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await.unwrap();
        let result_messages = client.get_vector().await.unwrap();
        
        // Stop storage server
        storage_server.stop().await;
        
        (worker_addresses, result_messages)
    });

    println!("[TEST] Test completed");

    // Verify results
    let result_len = result_messages.len();
    let mut values = Vec::new();
    for message in result_messages {
        let value = message.record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0);
        values.push(value.to_string());
    }

    println!("[TEST] Result values: {:?}", values);
    let total_parallelism = num_workers_per_operator * parallelism_per_worker;
    let expected_total_messages = num_messages_per_source * total_parallelism;
    assert_eq!(result_len, expected_total_messages, 
        "Expected {} messages ({} unique values × {} parallelism), got {}", 
        expected_total_messages, num_messages_per_source, total_parallelism, values.len());

    // Verify all expected values are present, each appearing parallelism times
    for i in 0..num_messages_per_source {
        let value = format!("value_{}", i);
        let count = values.iter().filter(|&v| v == &value).count();
        assert_eq!(count, total_parallelism, 
            "Expected value '{}' to appear {} times (parallelism), but found {} times", 
            value, total_parallelism, count);
    }

    Ok(())
} 