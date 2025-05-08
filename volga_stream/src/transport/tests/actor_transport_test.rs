use crate::runtime::actors::{StreamTaskActor, TransportBackendActor, StreamTaskMessage, TransportBackendMessage};
use crate::runtime::task::StreamTask;
use crate::runtime::execution_graph::{OperatorConfig, ExecutionVertex, SourceConfig, SinkConfig};
use crate::runtime::runtime_context::RuntimeContext;
use crate::transport::channel::Channel;
use crate::transport::transport_client::TransportClient;
use crate::common::data_batch::DataBatch;
use anyhow::Result;
use kameo::{Actor, spawn};
use tokio::runtime::Runtime;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_rayon::rayon::ThreadPoolBuilder;
use std::collections::HashMap;
use serde_json::Value;

#[test]
fn test_actor_transport_exchange() -> Result<()> {
    // Configuration
    let num_writers = 10;
    let num_readers = 10;
    let batches_per_writer = 10;

    // Create a runtime for transport operations
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        // Create transport backend actor
        let transport_backend = TransportBackendActor::new();
        let transport_backend_ref = spawn(transport_backend);

        // Create writer actors
        let mut writer_actors = Vec::new();
        for i in 0..num_writers {
            let task_id = i as i64;
            let task_index = i as i32;
            let parallelism = num_writers as i32;
            let operator_id = i as i64;
            let operator_name = format!("writer_{}", i);
            let job_config = None;
            let compute_pool = Some(Arc::new(ThreadPoolBuilder::new().num_threads(1).build()?));

            let context = RuntimeContext::new(
                task_id,
                task_index,
                parallelism,
                operator_id,
                operator_name,
                job_config,
                compute_pool,
            );

            let task = StreamTask::new(
                format!("writer_{}", i),
                OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(Arc::new(Mutex::new(Vec::new())))),
                context,
            )?;
            let actor = StreamTaskActor::new(task);
            let actor_ref = spawn(actor);
            writer_actors.push(actor_ref);
        }

        // Create reader actors
        let mut reader_actors = Vec::new();
        for i in 0..num_readers {
            let task_id = (i + num_writers) as i64;
            let task_index = i as i32;
            let parallelism = num_readers as i32;
            let operator_id = (i + num_writers) as i64;
            let operator_name = format!("reader_{}", i);
            let job_config = None;
            let compute_pool = Some(Arc::new(ThreadPoolBuilder::new().num_threads(1).build()?));

            let context = RuntimeContext::new(
                task_id,
                task_index,
                parallelism,
                operator_id,
                operator_name,
                job_config,
                compute_pool,
            );

            let task = StreamTask::new(
                format!("reader_{}", i),
                OperatorConfig::SinkConfig(SinkConfig::VectorSinkConfig(Arc::new(Mutex::new(Vec::new())))),
                context,
            )?;
            let actor = StreamTaskActor::new(task);
            let actor_ref = spawn(actor);
            reader_actors.push(actor_ref);
        }

        // Register clients and channels
        for writer in &writer_actors {
            let client = TransportClient::new(format!("writer_{}", writer_actors.iter().position(|w| w.id() == writer.id()).unwrap()));
            transport_backend_ref.tell(TransportBackendMessage::RegisterClient { 
                vertex_id: format!("writer_{}", writer_actors.iter().position(|w| w.id() == writer.id()).unwrap()),
                client
            }).await?;
            writer.tell(StreamTaskMessage::Open).await?;
        }

        for reader in &reader_actors {
            let client = TransportClient::new(format!("reader_{}", reader_actors.iter().position(|r| r.id() == reader.id()).unwrap()));
            transport_backend_ref.tell(TransportBackendMessage::RegisterClient {
                vertex_id: format!("reader_{}", reader_actors.iter().position(|r| r.id() == reader.id()).unwrap()),
                client
            }).await?;
            reader.tell(StreamTaskMessage::Open).await?;
        }

        // Create channels between writers and readers
        for (i, writer) in writer_actors.iter().enumerate() {
            for (j, reader) in reader_actors.iter().enumerate() {
                let channel = Channel::Local {
                    channel_id: format!("channel_{}_{}", i, j)
                };
                transport_backend_ref.tell(TransportBackendMessage::RegisterChannel {
                    vertex_id: format!("writer_{}", i),
                    channel: channel.clone(),
                    is_input: false
                }).await?;
                transport_backend_ref.tell(TransportBackendMessage::RegisterChannel {
                    vertex_id: format!("reader_{}", j),
                    channel: channel.clone(),
                    is_input: true
                }).await?;
            }
        }

        // Start transport backend
        transport_backend_ref.tell(TransportBackendMessage::Start).await?;

        // Run tasks
        for writer in &writer_actors {
            writer.tell(StreamTaskMessage::Run).await?;
        }
        for reader in &reader_actors {
            reader.tell(StreamTaskMessage::Run).await?;
        }

        // Close tasks
        for writer in &writer_actors {
            writer.tell(StreamTaskMessage::Close).await?;
        }
        for reader in &reader_actors {
            reader.tell(StreamTaskMessage::Close).await?;
        }

        transport_backend_ref.tell(TransportBackendMessage::Close).await?;

        Ok(())
    })
} 