use crate::runtime::actors::{TransportBackendActor, TransportBackendMessage};
use crate::transport::channel::Channel;
use crate::common::data_batch::DataBatch;
use crate::transport::test_utils::{TestDataReaderActor, TestDataWriterActor};
use crate::transport::transport_actor::TransportActorType;
use anyhow::Result;
use kameo::{Actor, spawn};
use kameo::prelude::ActorRef;
use tokio::sync::mpsc;
use tracing::info;
use tokio::sync::Barrier;

#[tokio::test]
async fn test_actor_transport() -> Result<()> {
    // Initialize console subscriber for tracing
    // console_subscriber::init();

    // Configuration
    let num_writers = 10;
    let num_readers = 10;
    let batches_per_writer = 10;

    info!("Starting actor transport test");

    // Create transport backend actor
    let backend_actor = TransportBackendActor::new();
    let backend_ref = spawn(backend_actor);

    // Create writer actors
    let mut writer_refs = Vec::new();
    for i in 0..num_writers {
        let writer_actor = TestDataWriterActor::new(format!("writer{}", i));
        let writer_ref = spawn(writer_actor);
        writer_refs.push(writer_ref.clone());

        // Register writer with backend
        backend_ref.tell(TransportBackendMessage::RegisterActor {
            vertex_id: format!("writer{}", i),
            actor: TransportActorType::TestWriter(writer_ref),
        }).await?;
        info!("Registered writer {}", i);
    }

    // Create reader actors
    let mut reader_refs = Vec::new();
    for i in 0..num_readers {
        let reader_actor = TestDataReaderActor::new(format!("reader{}", i));
        let reader_ref = spawn(reader_actor);
        reader_refs.push(reader_ref.clone());

        // Register reader with backend
        backend_ref.tell(TransportBackendMessage::RegisterActor {
            vertex_id: format!("reader{}", i),
            actor: TransportActorType::TestReader(reader_ref),
        }).await?;
        info!("Registered reader {}", i);
    }

    // Create and register channels between each writer and reader
    let mut channel_registrations = Vec::new();
    for writer_idx in 0..num_writers {
        for reader_idx in 0..num_readers {
            let channel = Channel::Local {
                channel_id: format!("writer{}_to_reader{}", writer_idx, reader_idx),
            };

            // Register channel for writer (output)
            channel_registrations.push(backend_ref.ask(TransportBackendMessage::RegisterChannel {
                vertex_id: format!("writer{}", writer_idx),
                channel: channel.clone(),
                is_input: false,
            }).await);

            // Register channel for reader (input)
            channel_registrations.push(backend_ref.ask(TransportBackendMessage::RegisterChannel {
                vertex_id: format!("reader{}", reader_idx),
                channel: channel.clone(),
                is_input: true,
            }).await);
            info!("Queued channel registration between writer {} and reader {}", writer_idx, reader_idx);
        }
    }

    // Wait for all channel registrations to complete
    for result in channel_registrations {
        result?;
    }
    info!("All channel registrations completed");

    // Start the backend
    backend_ref.tell(TransportBackendMessage::Start).await?;
    info!("Started transport backend");

    // Create test data and send from each writer
    for writer_idx in 0..num_writers {
        for batch_idx in 0..batches_per_writer {
            let batch = DataBatch::new(
                Some(format!("writer{}_stream", writer_idx)),
                vec![format!("writer{}_batch{}", writer_idx, batch_idx)],
            );

            // Send batch to each reader
            for reader_idx in 0..num_readers {
                let channel_id = format!("writer{}_to_reader{}", writer_idx, reader_idx);
                writer_refs[writer_idx].tell(crate::transport::test_utils::DataWriterMessage::WriteBatch {
                    channel_id,
                    batch: batch.clone(),
                }).await?;
                info!("Writer {} sent batch {} to reader {}", writer_idx, batch_idx, reader_idx);
            }
        }
    }

    // Verify data received by each reader
    for reader_idx in 0..num_readers {
        // Each reader should receive batches_per_writer from each writer
        for _ in 0..(num_writers * batches_per_writer) {
            let batch_result = reader_refs[reader_idx]
                .ask(crate::transport::test_utils::DataReaderMessage::ReadBatch)
                .await?;
            
            if let Some(batch) = batch_result {
                info!("Reader {} received batch: {:?}", reader_idx, batch);
            } else {
                return Err(anyhow::anyhow!("Reader {} did not receive expected batch", reader_idx));
            }
        }
    }

    // Close the backend
    backend_ref.tell(TransportBackendMessage::Close).await?;
    info!("Closed transport backend");

    Ok(())
} 