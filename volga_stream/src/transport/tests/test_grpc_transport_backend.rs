use crate::runtime::execution_graph::{ExecutionGraph, ExecutionVertex, OperatorConfig, ExecutionEdge};
use crate::runtime::partition::PartitionType;
use crate::runtime::functions::{
    map::MapFunction,
    map::MapFunctionTrait,
};
use crate::transport::transport_backend_actor::{TransportBackendActor, TransportBackendActorMessage};
use crate::transport::channel::Channel;
use crate::common::message::Message;
use crate::common::test_utils::create_test_string_batch;
use crate::transport::test_utils::{TestDataReaderActor, TestDataWriterActor};
use crate::transport::{GrpcTransportBackend, TransportBackend};
use arrow::array::StringArray;
use anyhow::Result;
use kameo::{Actor, spawn};
use std::collections::HashMap;
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone)]
struct IdentityMapFunction;

impl MapFunctionTrait for IdentityMapFunction {
    fn map(&self, message: Message) -> Result<Message> {
        Ok(message)
    }
}

#[tokio::test]
async fn test_grpc_transport_backend() {
    // Configuration
    let num_writer_nodes = 2;
    let num_reader_nodes = 2;
    let num_writers_per_node = 3;
    let num_readers_per_node = 3;
    let messages_per_writer = 5;

    // Create node configurations
    let writer_nodes = (0..num_writer_nodes).map(|i| format!("writer_node_{}", i)).collect::<Vec<_>>();
    let reader_nodes = (0..num_reader_nodes).map(|i| format!("reader_node_{}", i)).collect::<Vec<_>>();

    // Create vertex IDs for each node
    let mut writer_vertex_ids = Vec::new();
    let mut reader_vertex_ids = Vec::new();

    for node_idx in 0..num_writer_nodes {
        for writer_idx in 0..num_writers_per_node {
            writer_vertex_ids.push(format!("writer_node_{}_writer_{}", node_idx, writer_idx));
        }
    }

    for node_idx in 0..num_reader_nodes {
        for reader_idx in 0..num_readers_per_node {
            reader_vertex_ids.push(format!("reader_node_{}_reader_{}", node_idx, reader_idx));
        }
    }

    // Create execution graph
    let mut graph = ExecutionGraph::new();
    
    // Add writer vertices
    for vertex_id in &writer_vertex_ids {
        graph.add_vertex(ExecutionVertex::new(
            vertex_id.clone(),
            OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
            1,
            0
        ));
    }

    // Add reader vertices
    for vertex_id in &reader_vertex_ids {
        graph.add_vertex(ExecutionVertex::new(
            vertex_id.clone(),
            OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
            1,
            0
        ));
    }

    // Create remote channels between all writers and all readers
    // Each writer connects to each reader via a remote channel
    for (writer_idx, writer_vertex_id) in writer_vertex_ids.iter().enumerate() {
        for (reader_idx, reader_vertex_id) in reader_vertex_ids.iter().enumerate() {
            // Use a simple port calculation based on indices
            let port = 50051 + (writer_idx as i32 * 1000 + reader_idx as i32);
            
            let edge = ExecutionEdge::new(
                writer_vertex_id.clone(),
                reader_vertex_id.clone(),
                reader_vertex_id.clone(),
                PartitionType::Forward,
                Channel::Remote { 
                    channel_id: format!("writer_{}_to_reader_{}", writer_idx, reader_idx),
                    source_node_ip: format!("127.0.0.1:{}", port),
                    source_node_id: format!("writer_node_{}", writer_idx / num_writers_per_node),
                    target_node_ip: format!("127.0.0.1:{}", port),
                    target_node_id: format!("reader_node_{}", reader_idx / num_readers_per_node),
                    port 
                }
            );
            graph.add_edge(edge);
        }
    }

    // Create gRPC transport backend
    let mut backend = GrpcTransportBackend::new();
    let all_vertex_ids = writer_vertex_ids.clone().into_iter().chain(reader_vertex_ids.clone()).collect::<Vec<_>>();
    let mut configs = backend.init_channels(&graph, all_vertex_ids);

    // Create transport backend actor
    let backend_actor = TransportBackendActor::new(backend);
    let backend_ref = spawn(backend_actor);

    // Create writer actors
    let mut writer_refs = Vec::new();
    for vertex_id in &writer_vertex_ids {
        let writer_actor = TestDataWriterActor::new(vertex_id.clone(), configs.remove(vertex_id).unwrap());
        let writer_ref = spawn(writer_actor);
        writer_refs.push(writer_ref.clone());
    }

    // Create reader actors
    let mut reader_refs = Vec::new();
    for vertex_id in &reader_vertex_ids {
        let reader_actor = TestDataReaderActor::new(vertex_id.clone(), configs.remove(vertex_id).unwrap());
        let reader_ref = spawn(reader_actor);
        reader_refs.push(reader_ref.clone());
    }

    // Start the backend
    backend_ref.ask(TransportBackendActorMessage::Start).await.unwrap();

    // Give the backend some time to start up
    sleep(Duration::from_millis(1000)).await;

    // Create test data and send from each writer
    for writer_idx in 0..writer_vertex_ids.len() {
        for message_idx in 0..messages_per_writer {
            let message = Message::new(
                Some(format!("writer_{}_stream", writer_idx)),
                create_test_string_batch(vec![format!("writer_{}_batch_{}", writer_idx, message_idx)]),
                None
            );

            // Send message to each reader
            for reader_idx in 0..reader_vertex_ids.len() {
                let channel_id = format!("writer_{}_to_reader_{}", writer_idx, reader_idx);
                writer_refs[writer_idx].ask(crate::transport::test_utils::TestDataWriterMessage::WriteMessage {
                    channel_id,
                    message: message.clone(),
                }).await.unwrap();
            }
        }
    }

    // Give some time for messages to be processed
    sleep(Duration::from_millis(2000)).await;

    // Verify data received by each reader
    for reader_idx in 0..reader_vertex_ids.len() {
        let mut received_messages = Vec::new();
        
        // Read all expected batches
        for _ in 0..(writer_vertex_ids.len() * messages_per_writer) {
            let result = reader_refs[reader_idx].ask(crate::transport::test_utils::TestDataReaderMessage::ReadMessage).await.unwrap();
            if let Some(message) = result {
                received_messages.push(message);
            }
        }

        // Verify we got all expected batches
        assert_eq!(received_messages.len(), writer_vertex_ids.len() * messages_per_writer, 
            "Reader {} did not receive all expected batches", reader_idx);

        // Create a map to track received batches by writer and batch number
        let mut received_map: HashMap<(usize, usize), String> = HashMap::new();
        for message in &received_messages {
            let value = message.record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0);
            
            // Parse writer_idx and batch_num from the value
            let parts: Vec<&str> = value.split("_batch_").collect();
            let writer_part = parts[0].strip_prefix("writer_").unwrap();
            let writer_idx = writer_part.parse::<usize>().unwrap();
            let message_idx = parts[1].parse::<usize>().unwrap();
            
            received_map.insert((writer_idx, message_idx), value.to_string());
        }

        // Verify all expected batches were received
        for writer_idx in 0..writer_vertex_ids.len() {
            for message_idx in 0..messages_per_writer {
                let expected_value = format!("writer_{}_batch_{}", writer_idx, message_idx);
                let actual_value = received_map.get(&(writer_idx, message_idx))
                    .expect(&format!("Missing message writer_{}_batch_{} for reader {}", writer_idx, message_idx, reader_idx));
                assert_eq!(actual_value, &expected_value);
            }
        }

        println!("Reader {} successfully received all {} messages", reader_idx, received_messages.len());
    }

    // Close the backend
    backend_ref.ask(TransportBackendActorMessage::Close).await.unwrap();
    
    println!("gRPC transport backend test completed successfully!");
} 