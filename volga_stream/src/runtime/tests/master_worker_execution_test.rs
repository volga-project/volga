use crate::{common::test_utils::{create_test_string_batch, gen_unique_grpc_port}, runtime::{
    execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionVertex, OperatorConfig, SinkConfig, SourceConfig}, functions::{
        key_by::KeyByFunction,
        map::{MapFunction, MapFunctionTrait},
    }, master::Master, partition::PartitionType, storage::{InMemoryStorageClient, InMemoryStorageServer}, worker::WorkerConfig, worker_server::WorkerServer
}, transport::transport_backend_actor::TransportBackendType};
use crate::common::message::{Message, WatermarkMessage};
use crate::common::MAX_WATERMARK_VALUE;
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

/// Creates the execution graph for testing
fn create_test_graph(
    storage_server_addr: &str, 
    parallelism: usize, 
    num_messages_per_source: usize,
    worker_vertex_distribution: &HashMap<String, Vec<String>>,
) -> ExecutionGraph {
    let mut graph = ExecutionGraph::new();
    let mut worker_to_port = HashMap::new();

    // Assign one port per worker
    for worker_id in worker_vertex_distribution.keys() {
        worker_to_port.insert(worker_id.clone(), gen_unique_grpc_port() as i32);
    }

    // Create test data for each source
    let mut source_messages = Vec::new();
    let mut msg_id = 0;
    for i in 0..parallelism {
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
        let source_vertex_id = format!("source_{}", task_id);
        let source_vertex = ExecutionVertex::new(
            source_vertex_id,
            OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(source_messages[i].clone())),
            parallelism as i32,
            i as i32,
        );
        graph.add_vertex(source_vertex);

        // Create key-by vertex using ArrowKeyByFunction
        let key_by_vertex_id = format!("key_by_{}", task_id);
        let key_by_vertex = ExecutionVertex::new(
            key_by_vertex_id,
            OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["value".to_string()])),
            parallelism as i32,
            i as i32,
        );
        graph.add_vertex(key_by_vertex);

        // Create map vertex to transform keyed messages back to regular
        let map_vertex_id = format!("map_{}", task_id);
        let map_vertex = ExecutionVertex::new(
            map_vertex_id,
            OperatorConfig::MapConfig(MapFunction::new_custom(KeyedToRegularMapFunction)),
            parallelism as i32,
            i as i32,
        );
        graph.add_vertex(map_vertex);

        // Create sink vertex
        let sink_vertex_id = format!("sink_{}", task_id);
        let sink_vertex = ExecutionVertex::new(
            sink_vertex_id,
            OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig(format!("http://{}", storage_server_addr))),
            parallelism as i32,
            i as i32,
        );
        graph.add_vertex(sink_vertex);
    }

    // Helper function to find worker ID for a vertex
    fn find_worker_id_for_vertex(vertex_id: &str, worker_vertex_distribution: &HashMap<String, Vec<String>>) -> String {
        for (worker_id, vertex_ids) in worker_vertex_distribution {
            if vertex_ids.contains(&vertex_id.to_string()) {
                return worker_id.clone();
            }
        }
        panic!("No worker found for vertex: {}", vertex_id);
    }

    // Add edges connecting vertices across parallel tasks
    for i in 0..parallelism {
        let source_id = format!("source_{}", i);
        let source_worker_id = find_worker_id_for_vertex(&source_id, worker_vertex_distribution);
        
        // Connect each source to all key-by tasks
        for j in 0..parallelism {
            let key_by_id = format!("key_by_{}", j);
            let key_by_worker_id = find_worker_id_for_vertex(&key_by_id, worker_vertex_distribution);
            let target_port = worker_to_port.get(&key_by_worker_id).unwrap();
            let source_to_key_by = ExecutionEdge::new(
                source_id.clone(),
                key_by_id.clone(),
                "key_by".to_string(),
                PartitionType::RoundRobin,
                Channel::Remote {
                    channel_id: format!("source_{}_to_key_by_{}", i, j),
                    source_node_ip: "127.0.0.1".to_string(),
                    source_node_id: source_worker_id.clone(),
                    target_node_ip: "127.0.0.1".to_string(),
                    target_node_id: key_by_worker_id.clone(),
                    target_port: *target_port,
                },
            );
            graph.add_edge(source_to_key_by);
        }

        let key_by_id = format!("key_by_{}", i);
        let key_by_worker_id = find_worker_id_for_vertex(&key_by_id, worker_vertex_distribution);
        // Connect each key-by to all map tasks
        for j in 0..parallelism {
            let map_id = format!("map_{}", j);
            let map_worker_id = find_worker_id_for_vertex(&map_id, worker_vertex_distribution);
            let target_port = worker_to_port.get(&map_worker_id).unwrap();
            let key_by_to_map = ExecutionEdge::new(
                key_by_id.clone(),
                map_id.clone(),
                "map".to_string(),
                PartitionType::Hash,
                Channel::Remote {
                    channel_id: format!("key_by_{}_to_map_{}", i, j),
                    source_node_ip: "127.0.0.1".to_string(),
                    source_node_id: key_by_worker_id.clone(),
                    target_node_ip: "127.0.0.1".to_string(),
                    target_node_id: map_worker_id.clone(),
                    target_port: *target_port,
                },
            );
            graph.add_edge(key_by_to_map);
        }

        let map_id = format!("map_{}", i);
        let map_worker_id = find_worker_id_for_vertex(&map_id, worker_vertex_distribution);
        // Connect each map to all sink tasks
        for j in 0..parallelism {
            let sink_id = format!("sink_{}", j);
            let sink_worker_id = find_worker_id_for_vertex(&sink_id, worker_vertex_distribution);
            let target_port = worker_to_port.get(&sink_worker_id).unwrap();
            let map_to_sink = ExecutionEdge::new(
                map_id.clone(),
                sink_id.clone(),
                "sink".to_string(),
                PartitionType::RoundRobin,
                Channel::Remote {
                    channel_id: format!("map_{}_to_sink_{}", i, j),
                    source_node_ip: "127.0.0.1".to_string(),
                    source_node_id: map_worker_id.clone(),
                    target_node_ip: "127.0.0.1".to_string(),
                    target_node_id: sink_worker_id.clone(),
                    target_port: *target_port,
                },
            );
            graph.add_edge(map_to_sink);
        }
    }

    graph
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

    // Calculate total parallelism for each operator type
    let total_parallelism = num_workers_per_operator * parallelism_per_worker;
    
    // Create worker-to-vertex distribution
    let mut worker_vertex_distribution: HashMap<String, Vec<String>> = HashMap::new();
    let operator_types = vec!["source", "key_by", "map", "sink"];
    let mut worker_id = 0;

    // distribute vertices to workers - for this test each worker has vertices of one operator type
    for operator_type in &operator_types {
        for worker_idx in 0..num_workers_per_operator {
            let worker_id_str = format!("worker_{}", worker_id);
            
            // Assign vertices for this worker (only vertices of the specific operator type)
            let mut vertex_ids = Vec::new();
            for vertex_idx in 0..parallelism_per_worker {
                let global_vertex_idx = worker_idx * parallelism_per_worker + vertex_idx;
                vertex_ids.push(format!("{}_{}", operator_type, global_vertex_idx));
            }
            
            worker_vertex_distribution.insert(worker_id_str, vertex_ids);
            worker_id += 1;
        }
    }
    
    // Create execution graph
    let graph = create_test_graph(storage_server_addr, total_parallelism, num_messages_per_source, &worker_vertex_distribution);

    // println!("worker_vertex_distribution: {:?}", worker_vertex_distribution);
    // Start worker servers
    for worker_id in worker_vertex_distribution.keys() {
        let port = gen_unique_grpc_port();
        let addr = format!("127.0.0.1:{}", port);
        
        let vertex_ids = worker_vertex_distribution.get(worker_id).unwrap().clone();

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
        
        println!("[TEST] Started worker server {} on {}", worker_id, addr);
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
    
    println!("[TEST] Running master-worker execution test with {} workers per operator, {} parallelism per worker, {} messages per source", 
             num_workers_per_operator, parallelism_per_worker, num_messages_per_source);
    
    // Create runtime for async operations
    let runtime = Runtime::new()?;

    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());

    let (worker_addresses, result_messages) = runtime.block_on(async {
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
    let total_expected_messages = total_parallelism * num_messages_per_source;
    assert_eq!(result_len, total_expected_messages, 
        "Expected {} messages, got {}", total_expected_messages, values.len());

    for msg_id in 0..(num_messages_per_source * total_parallelism) {
        let value = format!("value_{}", msg_id);
        assert!(values.contains(&value), "Expected value {} not found in results", value);
    }

    println!("[TEST] All expected messages found in results");
    Ok(())
} 