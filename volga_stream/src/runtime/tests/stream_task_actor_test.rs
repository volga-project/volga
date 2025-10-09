use crate::api::LogicalGraph;
use crate::common::{WatermarkMessage, MAX_WATERMARK_VALUE};
use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::partition::PartitionType;
use crate::runtime::stream_task_actor::{StreamTaskActor, StreamTaskMessage};
use crate::runtime::stream_task::StreamTask;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionVertex};
use crate::common::message::Message;
use crate::common::test_utils::{create_test_string_batch, IdentityMapFunction};
use crate::storage::storage::Storage;
use crate::transport::test_utils::{TestDataReaderActor, TestDataWriterActor};
use crate::transport::channel::Channel;
use crate::transport::transport_backend_actor::{TransportBackendActor, TransportBackendActorMessage};
use crate::runtime::functions::{
    map::MapFunction,
    map::MapFunctionTrait,
};
use crate::transport::{InMemoryTransportBackend, TransportBackend};
use anyhow::Result;
use kameo::{Actor, spawn};
use tokio::runtime::Runtime;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;
use async_trait::async_trait;

#[test]
fn test_stream_task_actor() -> Result<()> {
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        // Create test data
        let mut test_messages = vec![
            Message::new(None, create_test_string_batch(vec!["test1".to_string()]), Some(1)),
            Message::new(None, create_test_string_batch(vec!["test2".to_string()]), Some(2)),
            Message::new(None, create_test_string_batch(vec!["test3".to_string()]), Some(3)),
        ];

        let num_messages = test_messages.len();

        test_messages.push(Message::Watermark(WatermarkMessage::new(
            "source".to_string(),
            MAX_WATERMARK_VALUE,
            None,
        )));

        // Define operator chain: input -> task -> output
        let operators = vec![
            OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
            OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
            OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
        ];

        let logical_graph = LogicalGraph::from_linear_operators(operators, 1, false);
        let mut graph = logical_graph.to_execution_graph();
        graph.update_channels_with_node_mapping(None);

        let mut vertex_ids = graph.get_vertices().keys().cloned().collect::<Vec<String>>();
        vertex_ids.sort();

        let mut backend: Box<dyn TransportBackend> = Box::new(InMemoryTransportBackend::new());
        let mut configs = backend.init_channels(&graph, vertex_ids.clone());

        let input_vertex_id = vertex_ids[0].clone();
        let task_vertex_id = vertex_ids[1].clone();
        let output_vertex_id = vertex_ids[2].clone();

        // Create task with MapOperator
        let task = StreamTask::new(
            task_vertex_id.clone(),
            OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
            configs.remove(&task_vertex_id).unwrap(),
            RuntimeContext::new(
                task_vertex_id.clone(),
                0,
                1,
                None,
            ),
            graph.clone(),
            Arc::new(Storage::default()),
        );

        // Create transport backend actor
        let backend_actor = TransportBackendActor::new(backend);
        let backend_ref = spawn(backend_actor);

        // Create external writer and reader actors
        let input_actor = TestDataWriterActor::new(input_vertex_id.clone(), configs.remove(&input_vertex_id).unwrap());
        let output_actor = TestDataReaderActor::new(output_vertex_id.clone(), configs.remove(&output_vertex_id).unwrap());
        let task_actor = StreamTaskActor::new(task);

        let input_ref = spawn(input_actor);
        let output_ref = spawn(output_actor);
        let task_ref = spawn(task_actor);

        // Start the backend
        backend_ref.ask(TransportBackendActorMessage::Start).await?;

        // Run task
        task_ref.ask(StreamTaskMessage::Start).await?;

        // Signal to run
        task_ref.ask(StreamTaskMessage::Run).await?;

        // Write test data using external writer
        for message in &test_messages {
            // let channel_id = gen_channel_id(&input_vertex_id, &task_vertex_id);
            let channel = Channel::new_local(input_vertex_id.clone(), task_vertex_id.clone());
            input_ref.ask(crate::transport::test_utils::TestDataWriterMessage::WriteMessage {
                channel,
                message: message.clone(),
            }).await?;
        }

        // Read and verify output using external reader
        let mut received_messages: Vec<Message> = Vec::new();
        for _ in 0..test_messages.len() {
            let result = output_ref.ask(crate::transport::test_utils::TestDataReaderMessage::ReadMessage).await?;
            if let Some(message) = result {
                received_messages.push(message);
            }
        }

        println!("received_messages {:?}", received_messages);

        // Filter out watermarks from received messages
        received_messages.retain(|msg| !matches!(msg, Message::Watermark(_)));

        // Verify received data
        assert_eq!(received_messages.len(), num_messages);
        for (expected, actual) in test_messages.iter().zip(received_messages.iter()) {
            if matches!(expected, Message::Watermark(_)) {
                continue;
            }
            assert_eq!(actual.record_batch(), expected.record_batch());
        }

        // Close task and backend
        task_ref.ask(StreamTaskMessage::Close).await?;
        backend_ref.ask(TransportBackendActorMessage::Close).await?;

        Ok(())
    })
} 