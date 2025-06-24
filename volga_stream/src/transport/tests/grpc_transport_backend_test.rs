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

    // Create a single execution graph for the entire system
    let mut graph = ExecutionGraph::new();
    
    // Add all writer vertices
    for node_idx in 0..num_writer_nodes {
        for writer_idx in 0..num_writers_per_node {
            let vertex_id = format!("writer_node_{}_writer_{}", node_idx, writer_idx);
            graph.add_vertex(ExecutionVertex::new(
                vertex_id.clone(),
                OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
                1,
                0
            ));
        }
    }

    // Add all reader vertices
    for node_idx in 0..num_reader_nodes {
        for reader_idx in 0..num_readers_per_node {
            let vertex_id = format!("reader_node_{}_reader_{}", node_idx, reader_idx);
            graph.add_vertex(ExecutionVertex::new(
                vertex_id.clone(),
                OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
                1,
                0
            ));
        }
    }

    // Create remote channels between all writers and all readers
    // Each writer connects to each reader via a remote channel
    for (writer_idx, writer_vertex_id) in writer_vertex_ids.iter().enumerate() {
        for (reader_idx, reader_vertex_id) in reader_vertex_ids.iter().enumerate() {
            // Calculate target node ID
            let target_node_id = reader_idx / num_readers_per_node;
            let source_node_id = writer_idx / num_writers_per_node;
            
            // Use port based on target node ID to ensure same node gets same port
            let target_port = 50051 + target_node_id as i32;
            
            let edge = ExecutionEdge::new(
                writer_vertex_id.clone(),
                reader_vertex_id.clone(),
                reader_vertex_id.clone(),
                PartitionType::Forward,
                Channel::Remote { 
                    channel_id: format!("writer_{}_to_reader_{}", writer_idx, reader_idx),
                    source_node_ip: "127.0.0.1".to_string(),
                    source_node_id: format!("writer_node_{}", source_node_id),
                    target_node_ip: "127.0.0.1".to_string(),
                    target_node_id: format!("reader_node_{}", target_node_id),
                    target_port,
                }
            );
            
            graph.add_edge(edge);
        }
    }

    // Create transport backends for each node
    let mut writer_backends = Vec::new();
    let mut reader_backends = Vec::new();

    // Create writer node backends
    for node_idx in 0..num_writer_nodes {
        let mut backend: Box<dyn TransportBackend> = Box::new(GrpcTransportBackend::new());
        let vertex_ids = (0..num_writers_per_node)
            .map(|writer_idx| format!("writer_node_{}_writer_{}", node_idx, writer_idx))
            .collect::<Vec<_>>();
        
        let configs = backend.init_channels(&graph, vertex_ids);
        writer_backends.push((backend, configs));
    }

    // Create reader node backends
    for node_idx in 0..num_reader_nodes {
        let mut backend: Box<dyn TransportBackend> = Box::new(GrpcTransportBackend::new());
        let vertex_ids = (0..num_readers_per_node)
            .map(|reader_idx| format!("reader_node_{}_reader_{}", node_idx, reader_idx))
            .collect::<Vec<_>>();
        
        let configs = backend.init_channels(&graph, vertex_ids);
        reader_backends.push((backend, configs));
    }

    // Create transport backend actors for each node
    let mut writer_backend_refs = Vec::new();
    let mut reader_backend_refs = Vec::new();
    let mut writer_refs = HashMap::new();
    let mut reader_refs = HashMap::new();

    for (backend, configs) in writer_backends {
        let backend_actor = TransportBackendActor::new(backend);
        let backend_ref = spawn(backend_actor);
        writer_backend_refs.push(backend_ref);
        
        // Create writer actors for this backend
        for (vertex_id, config) in configs {
            let writer_actor = TestDataWriterActor::new(vertex_id.clone(), config);
            let writer_ref = spawn(writer_actor);
            writer_refs.insert(vertex_id, writer_ref.clone());
        }
    }

    for (backend, configs) in reader_backends {
        let backend_actor = TransportBackendActor::new(backend);
        let backend_ref = spawn(backend_actor);
        reader_backend_refs.push(backend_ref);
        
        // Create reader actors for this backend
        for (vertex_id, config) in configs {
            let reader_actor = TestDataReaderActor::new(vertex_id.clone(), config);
            let reader_ref = spawn(reader_actor);
            reader_refs.insert(vertex_id, reader_ref.clone());
        }
    }

    // Start all backends
    for backend_ref in &writer_backend_refs {
        backend_ref.ask(TransportBackendActorMessage::Start).await.unwrap();
    }
    for backend_ref in &reader_backend_refs {
        backend_ref.ask(TransportBackendActorMessage::Start).await.unwrap();
    }

    // Give the backends some time to start up
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
                let writer_vertex_id = &writer_vertex_ids[writer_idx];
                let writer_ref = writer_refs.get(writer_vertex_id).unwrap();
                writer_ref.ask(crate::transport::test_utils::TestDataWriterMessage::WriteMessage {
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
        let reader_vertex_id = &reader_vertex_ids[reader_idx];
        let reader_ref = reader_refs.get(reader_vertex_id).unwrap();
        let mut received_messages = Vec::new();
        
        // Read all expected batches
        for _ in 0..(writer_vertex_ids.len() * messages_per_writer) {
            let result = reader_ref.ask(crate::transport::test_utils::TestDataReaderMessage::ReadMessage).await.unwrap();
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

    // Close the backends
    for backend_ref in &writer_backend_refs {
        backend_ref.ask(TransportBackendActorMessage::Close).await.unwrap();
    }
    for backend_ref in &reader_backend_refs {
        backend_ref.ask(TransportBackendActorMessage::Close).await.unwrap();
    }
    
    println!("gRPC transport backend test completed successfully!");
} 