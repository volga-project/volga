use crate::runtime::stream_task_actor::{StreamTaskActor, StreamTaskMessage};
use crate::runtime::stream_task::StreamTask;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::execution_graph::{ExecutionGraph, OperatorConfig};
use crate::common::message::Message;
use crate::common::test_utils::create_test_string_batch;
use crate::transport::test_utils::{TestDataReaderActor, TestDataWriterActor};
use crate::transport::channel::Channel;
use crate::transport::transport_backend_actor::{TransportBackendActor, TransportBackendActorMessage};
use crate::transport::transport_client_actor::TransportClientActorType;
// use crate::runtime::map_function::{MapFunction, MapFunctionTrait};
use crate::runtime::functions::{
    map::MapFunction,
    map::MapFunctionTrait,
};
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
        let test_messages = vec![
            Message::new(None, create_test_string_batch(vec!["test1".to_string()])?),
            Message::new(None, create_test_string_batch(vec!["test2".to_string()])?),
            Message::new(None, create_test_string_batch(vec!["test3".to_string()])?),
        ];

        let dummy_execution_graph = ExecutionGraph::new();

        // Create task with MapOperator
        let task = StreamTask::new(
            "task1".to_string(),
            OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
            RuntimeContext::new(
                "task1".to_string(), // vertex_id
                0, // task_index
                1, // parallelism
                None, // job_config
            ),
            dummy_execution_graph.clone(),
        )?;

        // Create transport backend actor
        let backend_actor = TransportBackendActor::new();
        let backend_ref = spawn(backend_actor);

        // Create external writer and reader actors
        let input_actor = TestDataWriterActor::new("input".to_string());
        let output_actor = TestDataReaderActor::new("output".to_string());
        let task_actor = StreamTaskActor::new(task);

        let input_ref = spawn(input_actor);
        let output_ref = spawn(output_actor);
        let task_ref = spawn(task_actor);

        // Register actors with backend
        let mut registrations = Vec::new();
        
        registrations.push(backend_ref.ask(TransportBackendActorMessage::RegisterActor {
            vertex_id: "input".to_string(),
            actor: TransportClientActorType::TestWriter(input_ref.clone()),
        }));

        registrations.push(backend_ref.ask(TransportBackendActorMessage::RegisterActor {
            vertex_id: "output".to_string(),
            actor: TransportClientActorType::TestReader(output_ref.clone()),
        }));

        registrations.push(backend_ref.ask(TransportBackendActorMessage::RegisterActor {
            vertex_id: "task1".to_string(),
            actor: TransportClientActorType::StreamTask(task_ref.clone()),
        }));

        // Register channels through backend
        let input_channel = Channel::Local {
            channel_id: "input_to_task".to_string(),
        };
        let output_channel = Channel::Local {
            channel_id: "task_to_output".to_string(),
        };

        // Register input channel
        registrations.push(backend_ref.ask(TransportBackendActorMessage::RegisterChannel {
            vertex_id: "input".to_string(),
            channel: input_channel.clone(),
            is_input: false,
        }));

        registrations.push(backend_ref.ask(TransportBackendActorMessage::RegisterChannel {
            vertex_id: "task1".to_string(),
            channel: input_channel,
            is_input: true,
        }));

        // Register output channel
        registrations.push(backend_ref.ask(TransportBackendActorMessage::RegisterChannel {
            vertex_id: "task1".to_string(),
            channel: output_channel.clone(),
            is_input: false,
        }));

        registrations.push(backend_ref.ask(TransportBackendActorMessage::RegisterChannel {
            vertex_id: "output".to_string(),
            channel: output_channel,
            is_input: true,
        }));

        // Wait for all registrations to complete
        for result in registrations {
            result.await?;
        }

        // Start the backend
        backend_ref.ask(TransportBackendActorMessage::Start).await?;

        // Create collector for task to output
        task_ref.ask(StreamTaskMessage::CreateCollector {
            channel_id: "task_to_output".to_string(),
            partition_type: crate::runtime::partition::PartitionType::Forward,
            target_operator_id: "output".to_string(),
        }).await?;

        // Run task
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
        assert_eq!(received_messages.len(), test_messages.len());
        for (expected, actual) in test_messages.iter().zip(received_messages.iter()) {
            assert_eq!(actual.record_batch(), expected.record_batch());
        }

        // Close task and backend
        task_ref.ask(StreamTaskMessage::Close).await?;
        backend_ref.ask(TransportBackendActorMessage::Close).await?;

        Ok(())
    })
} 