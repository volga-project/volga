use crate::runtime::actors::TransportBackendActor;
use crate::transport::test_utils::{DataReaderActor, DataWriterActor, DataReaderMessage, DataWriterMessage};
use crate::transport::channel::Channel;
use crate::common::data_batch::DataBatch;
use anyhow::Result;
use kameo::{Actor, spawn};
use tokio::runtime::Runtime;
use std::sync::Arc;
use tokio::sync::mpsc;

#[test]
fn test_actor_transport_exchange() -> Result<()> {
    // Configuration
    let num_writers = 50;
    let num_readers = 50;
    let batches_per_writer = 10;

    // Create a runtime for transport operations
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        // Create test data for each writer
        let test_batches: Vec<Vec<DataBatch>> = (0..num_writers)
            .map(|writer_idx| {
                (0..batches_per_writer)
                    .map(|batch_idx| {
                        DataBatch::new(
                            None,
                            vec![format!("writer{}_batch{}", writer_idx, batch_idx)],
                        )
                    })
                    .collect()
            })
            .collect();

        // Create transport backend actor
        let transport_backend = TransportBackendActor::new();
        let transport_backend_ref = spawn(transport_backend);

        // Create writer actors
        let mut writer_actors = Vec::new();
        for i in 0..num_writers {
            let actor = DataWriterActor::new(format!("writer{}", i));
            let actor_ref = spawn(actor);
            writer_actors.push(actor_ref);
        }

        // Create reader actors
        let mut reader_actors = Vec::new();
        for i in 0..num_readers {
            let actor = DataReaderActor::new(format!("reader{}", i));
            let actor_ref = spawn(actor);
            reader_actors.push(actor_ref);
        }

        // Create and register channels between each writer and reader
        for writer_idx in 0..num_writers {
            for reader_idx in 0..num_readers {
                let channel_id = format!("writer{}_to_reader{}", writer_idx, reader_idx);
                let (sender, receiver) = mpsc::channel(100);

                // Register sender with writer
                writer_actors[writer_idx].tell(DataWriterMessage::RegisterSender {
                    channel_id: channel_id.clone(),
                    sender,
                }).await;

                // Register receiver with reader
                reader_actors[reader_idx].tell(DataReaderMessage::RegisterReceiver {
                    channel_id: channel_id.clone(),
                    receiver,
                }).await;
            }
        }

        // Write test data from each writer to each reader
        for (writer_idx, writer) in writer_actors.iter().enumerate() {
            for reader_idx in 0..num_readers {
                for batch in &test_batches[writer_idx] {
                    writer.tell(DataWriterMessage::WriteBatch {
                        channel_id: format!("writer{}_to_reader{}", writer_idx, reader_idx),
                        batch: batch.clone(),
                    }).await;
                }
            }
        }

        // Read and verify data from each reader
        let mut all_reader_batches = Vec::new();
        for reader in reader_actors.iter() {
            let mut reader_batches = Vec::new();
            // Each reader should receive batches_per_writer from each writer
            for _ in 0..(num_writers * batches_per_writer) {
                let result = reader.ask(DataReaderMessage::ReadBatch).await?;
                if let Some(batch) = result {
                    reader_batches.push(batch);
                } else {
                    break;
                }
            }
            all_reader_batches.push(reader_batches);
        }

        // Verify received data
        assert_eq!(all_reader_batches.len(), num_readers);
        for reader_batches in all_reader_batches {
            assert_eq!(reader_batches.len(), num_writers * batches_per_writer);
            
            // Group batches by writer
            let mut writer_batches: Vec<Vec<String>> = vec![Vec::new(); num_writers];
            for batch in reader_batches {
                let content = batch.record_batch()[0].clone();
                // Extract writer index from the batch content (format: "writer{idx}_batch{idx}")
                let writer_idx = content.split('_').next().unwrap()
                    .trim_start_matches("writer")
                    .parse::<usize>()
                    .unwrap();
                writer_batches[writer_idx].push(content);
            }

            // Verify each writer's batches
            for writer_idx in 0..num_writers {
                assert_eq!(writer_batches[writer_idx].len(), batches_per_writer);
                
                // Verify batches are in order for this writer
                for batch_idx in 0..batches_per_writer {
                    let expected = format!("writer{}_batch{}", writer_idx, batch_idx);
                    let actual = writer_batches[writer_idx][batch_idx].clone();
                    assert_eq!(actual, expected, 
                        "Batches from writer {} are not in order. Expected {} but got {}", 
                        writer_idx, expected, actual);
                }
            }
        }

        Ok(())
    })
} 