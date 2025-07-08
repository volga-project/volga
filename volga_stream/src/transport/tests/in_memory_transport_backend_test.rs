use crate::runtime::execution_graph::{ExecutionGraph, ExecutionVertex, ExecutionEdge};
use crate::runtime::operators::operator::OperatorConfig;
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
use crate::transport::{InMemoryTransportBackend, TransportBackend};
use arrow::array::StringArray;
use anyhow::Result;
use kameo::{Actor, spawn};
use std::collections::HashMap;

#[derive(Debug, Clone)]
struct IdentityMapFunction;

impl MapFunctionTrait for IdentityMapFunction {
    fn map(&self, message: Message) -> Result<Message> {
        Ok(message)
    }
}

// TODO test ordered delivery
#[tokio::test]
async fn test_actor_transport() {
    // Initialize console subscriber for tracing
    // console_subscriber::init();

    // Configuration
    let num_writers = 10;
    let num_readers = 10;
    let messages_per_writer = 10;

    let writer_vertex_ids = (0..num_writers).map(|i| format!("writer{}", i)).collect::<Vec<_>>();
    let reader_vertex_ids = (0..num_readers).map(|i| format!("reader{}", i)).collect::<Vec<_>>();

    let mut graph = ExecutionGraph::new();
    for i in 0..num_writers {
        graph.add_vertex(ExecutionVertex::new(
            writer_vertex_ids[i].clone(),
            OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
            1,
            0
        ));
    }
    for i in 0..num_readers {
        graph.add_vertex(ExecutionVertex::new(
            reader_vertex_ids[i].clone(),
            OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
            1,
            0
        ));
    }

    for i in 0..num_writers {
        for j in 0..num_readers {
            let edge = ExecutionEdge::new(
                writer_vertex_ids[i].clone(),
                reader_vertex_ids[j].clone(),
                reader_vertex_ids[j].clone(),
                PartitionType::Forward,
                Channel::Local { channel_id: format!("writer{}_to_reader{}", i, j) }
            );
            graph.add_edge(edge);
        }
    }

    let mut backend: Box<dyn TransportBackend> = Box::new(InMemoryTransportBackend::new());
    let all_vertex_ids = writer_vertex_ids.clone().into_iter().chain(reader_vertex_ids.clone()).collect::<Vec<_>>();
    let mut configs = backend.init_channels(&graph, all_vertex_ids);
    

    // Create transport backend actor
    let backend_actor = TransportBackendActor::new(backend);
    let backend_ref = spawn(backend_actor);

    // Create and start writer actors
    let mut writer_refs = Vec::new();
    for vertex_id in writer_vertex_ids {
        let writer_actor = TestDataWriterActor::new(vertex_id.clone(), configs.remove(&vertex_id.clone()).unwrap());
        let writer_ref = spawn(writer_actor);
        writer_refs.push(writer_ref.clone());
    }

    // Create reader actors
    let mut reader_refs = Vec::new();
    for vertex_id in reader_vertex_ids {
        let reader_actor = TestDataReaderActor::new(vertex_id.clone(), configs.remove(&vertex_id.clone()).unwrap());
        let reader_ref = spawn(reader_actor);
        reader_refs.push(reader_ref.clone());
    }

    // Start the backend
    backend_ref.ask(TransportBackendActorMessage::Start).await.unwrap();

    // Create test data and send from each writer
    for writer_idx in 0..num_writers {
        // Start writer
        let writer_ref = &writer_refs[writer_idx];
        writer_ref.ask(crate::transport::test_utils::TestDataWriterMessage::Start).await.unwrap();
        
        // Send message to each reader
        for message_idx in 0..messages_per_writer {
            let message = Message::new(
                Some(format!("writer{}_stream", writer_idx)),
                create_test_string_batch(vec![format!("writer{}_batch{}", writer_idx, message_idx)]),
                Some(100)
            );
            for reader_idx in 0..num_readers {
                let channel_id = format!("writer{}_to_reader{}", writer_idx, reader_idx);
                writer_ref.ask(crate::transport::test_utils::TestDataWriterMessage::WriteMessage {
                    channel_id,
                    message: message.clone(),
                }).await.unwrap();
            }
        }

        // Flush and close writers
        writer_ref.ask(crate::transport::test_utils::TestDataWriterMessage::FlushAndClose).await.unwrap();
    }


    // Verify data received by each reader
    for reader_idx in 0..num_readers {
        let mut received_messages = Vec::new();
        
        // Read all expected batches
        for _ in 0..(num_writers * messages_per_writer) {
            let result = reader_refs[reader_idx].ask(crate::transport::test_utils::TestDataReaderMessage::ReadMessage).await.unwrap();
            if let Some(message) = result {
                received_messages.push(message);
            }
        }

        // Verify we got all expected batches
        assert_eq!(received_messages.len(), num_writers * messages_per_writer, 
            "Reader {} did not receive all expected batches", reader_idx);

        // Create a map to track received batches by writer and batch number
        let mut received_map: HashMap<(usize, usize), String> = HashMap::new();
        for message in received_messages {
            let value = message.record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0);
            
            // Parse writer_idx and batch_num from the value
            let parts: Vec<&str> = value.split("_batch").collect();
            let writer_part = parts[0].strip_prefix("writer").unwrap();
            let writer_idx = writer_part.parse::<usize>().unwrap();
            let message_idx = parts[1].parse::<usize>().unwrap();
            
            received_map.insert((writer_idx, message_idx), value.to_string());
        }

        // Verify all expected batches were received
        for writer_idx in 0..num_writers {
            for message_idx in 0..messages_per_writer {
                let expected_value = format!("writer{}_batch{}", writer_idx, message_idx);
                let actual_value = received_map.get(&(writer_idx, message_idx))
                    .expect(&format!("Missing message writer{}_batch{} for reader {}", writer_idx, message_idx, reader_idx));
                assert_eq!(actual_value, &expected_value);
            }
        }
    }

    // Close the backend
    backend_ref.ask(TransportBackendActorMessage::Close).await.unwrap();
} 