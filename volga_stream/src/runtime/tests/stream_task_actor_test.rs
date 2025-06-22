use crate::common::{WatermarkMessage, MAX_WATERMARK_VALUE};
use crate::runtime::partition::PartitionType;
use crate::runtime::stream_task_actor::{StreamTaskActor, StreamTaskMessage};
use crate::runtime::stream_task::StreamTask;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionVertex, OperatorConfig};
use crate::common::message::Message;
use crate::common::test_utils::create_test_string_batch;
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

#[derive(Debug, Clone)]
struct IdentityMapFunction;

#[async_trait]
impl MapFunctionTrait for IdentityMapFunction {
    fn map(&self, message: Message) -> Result<Message> {
        Ok(message)
    }
}

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

        let mut graph = ExecutionGraph::new();
        graph.add_vertex(ExecutionVertex::new(
            "input".to_string(),
            OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
            1,
            0
        ));
        graph.add_vertex(ExecutionVertex::new(
            "task".to_string(),
            OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
            1,
            0
        ));
        graph.add_vertex(ExecutionVertex::new(
            "output".to_string(),
            OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
            1,
            0
        ));

        graph.add_edge(ExecutionEdge::new(
            "input".to_string(),
            "task".to_string(),
            "task".to_string(),
            PartitionType::Forward,
            Channel::Local { channel_id: format!("input_to_task") }
        ));

        graph.add_edge(ExecutionEdge::new(
            "task".to_string(),
            "output".to_string(),
            "output".to_string(),
            PartitionType::Forward,
            Channel::Local { channel_id: format!("task_to_output") }
        ));

        let mut backend = InMemoryTransportBackend::new();
        let mut configs = backend.init_channels(&graph, vec!["input".to_string(), "task".to_string(), "output".to_string()]);


        // Create task with MapOperator
        let task = StreamTask::new(
            "task".to_string(),
            OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
            configs.remove("task").unwrap(),
            RuntimeContext::new(
                "task".to_string(),
                0,
                1,
                None,
            ),
            graph.clone(),
        );

        // Create transport backend actor
        let backend_actor = TransportBackendActor::new(backend);
        let backend_ref = spawn(backend_actor);

        // Create external writer and reader actors
        let input_actor = TestDataWriterActor::new("input".to_string(), configs.remove("input").unwrap());
        let output_actor = TestDataReaderActor::new("output".to_string(), configs.remove("output").unwrap());
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
            input_ref.ask(crate::transport::test_utils::TestDataWriterMessage::WriteMessage {
                channel_id: "input_to_task".to_string(),
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