use crate::transport::transport_backend_actor::{TransportBackendActor, TransportBackendActorMessage};
use crate::transport::channel::Channel;
use crate::common::data_batch::DataBatch;
use crate::common::test_utils::create_test_string_batch;
use crate::transport::test_utils::{TestDataReaderActor, TestDataWriterActor};
use crate::transport::transport_client_actor::TransportClientActorType;
use arrow::array::StringArray;
use anyhow::Result;
use kameo::{Actor, spawn};
use std::collections::HashMap;

// TODO test ordered delivery
#[tokio::test]
async fn test_actor_transport() -> Result<()> {
    // Initialize console subscriber for tracing
    // console_subscriber::init();

    // Configuration
    let num_writers = 10;
    let num_readers = 10;
    let batches_per_writer = 10;

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
        backend_ref.ask(TransportBackendActorMessage::RegisterActor {
            vertex_id: format!("writer{}", i),
            actor: TransportClientActorType::TestWriter(writer_ref),
        }).await?;
    }

    // Create reader actors
    let mut reader_refs = Vec::new();
    for i in 0..num_readers {
        let reader_actor = TestDataReaderActor::new(format!("reader{}", i));
        let reader_ref = spawn(reader_actor);
        reader_refs.push(reader_ref.clone());

        // Register reader with backend
        backend_ref.ask(TransportBackendActorMessage::RegisterActor {
            vertex_id: format!("reader{}", i),
            actor: TransportClientActorType::TestReader(reader_ref),
        }).await?;
    }

    // Create and register channels between each writer and reader
    let mut channel_registrations = Vec::new();
    for writer_idx in 0..num_writers {
        for reader_idx in 0..num_readers {
            let channel = Channel::Local {
                channel_id: format!("writer{}_to_reader{}", writer_idx, reader_idx),
            };

            // Register channel for writer (output)
            channel_registrations.push(backend_ref.ask(TransportBackendActorMessage::RegisterChannel {
                vertex_id: format!("writer{}", writer_idx),
                channel: channel.clone(),
                is_input: false,
            }).await);

            // Register channel for reader (input)
            channel_registrations.push(backend_ref.ask(TransportBackendActorMessage::RegisterChannel {
                vertex_id: format!("reader{}", reader_idx),
                channel: channel.clone(),
                is_input: true,
            }).await);
        }
    }

    // Wait for all channel registrations to complete
    for result in channel_registrations {
        result?;
    }

    // Start the backend
    backend_ref.ask(TransportBackendActorMessage::Start).await?;

    // Create test data and send from each writer
    for writer_idx in 0..num_writers {
        for batch_idx in 0..batches_per_writer {
            let batch = DataBatch::new(
                Some(format!("writer{}_stream", writer_idx)),
                create_test_string_batch(vec![format!("writer{}_batch{}", writer_idx, batch_idx)])?
            );

            // Send batch to each reader
            for reader_idx in 0..num_readers {
                let channel_id = format!("writer{}_to_reader{}", writer_idx, reader_idx);
                writer_refs[writer_idx].ask(crate::transport::test_utils::TestDataWriterMessage::WriteBatch {
                    channel_id,
                    batch: batch.clone(),
                }).await?;
            }
        }
    }

    // Verify data received by each reader
    for reader_idx in 0..num_readers {
        let mut received_batches = Vec::new();
        
        // Read all expected batches
        for _ in 0..(num_writers * batches_per_writer) {
            let result = reader_refs[reader_idx].ask(crate::transport::test_utils::TestDataReaderMessage::ReadBatch).await?;
            if let Some(batch) = result {
                received_batches.push(batch);
            }
        }

        // Verify we got all expected batches
        assert_eq!(received_batches.len(), num_writers * batches_per_writer, 
            "Reader {} did not receive all expected batches", reader_idx);

        // Create a map to track received batches by writer and batch number
        let mut received_map: HashMap<(usize, usize), String> = HashMap::new();
        for batch in received_batches {
            let value = batch.record_batch()
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0);
            
            // Parse writer_idx and batch_num from the value
            let parts: Vec<&str> = value.split("_batch").collect();
            let writer_part = parts[0].strip_prefix("writer").unwrap();
            let writer_idx = writer_part.parse::<usize>().unwrap();
            let batch_num = parts[1].parse::<usize>().unwrap();
            
            received_map.insert((writer_idx, batch_num), value.to_string());
        }

        // Verify all expected batches were received
        for writer_idx in 0..num_writers {
            for batch_num in 0..batches_per_writer {
                let expected_value = format!("writer{}_batch{}", writer_idx, batch_num);
                let actual_value = received_map.get(&(writer_idx, batch_num))
                    .expect(&format!("Missing batch writer{}_batch{} for reader {}", writer_idx, batch_num, reader_idx));
                assert_eq!(actual_value, &expected_value);
            }
        }
    }

    // Close the backend
    backend_ref.ask(TransportBackendActorMessage::Close).await?;

    Ok(())
} 