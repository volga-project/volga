// use crate::transport::{
//     transport_backend::TransportBackend,
//     transport_client::{TransportClient, DataWriter, DataReader},
//     channel::Channel,
// };
// use crate::common::data_batch::DataBatch;
// use anyhow::Result;
// use std::sync::Arc;
// use tokio::sync::Mutex;
// use tokio::runtime::Runtime;

// #[test]
// fn test_transport_exchange() -> Result<()> {
//     // Configuration
//     let num_writers = 10;
//     let num_readers = 10;
//     let batches_per_writer = 10;

//     // Create a runtime for transport operations
//     let runtime = Runtime::new()?;

//     // Create test data for each writer
//     let test_batches: Vec<Vec<DataBatch>> = (0..num_writers)
//         .map(|writer_idx| {
//             (0..batches_per_writer)
//                 .map(|batch_idx| {
//                     DataBatch::new(
//                         None,
//                         vec![format!("writer{}_batch{}", writer_idx, batch_idx)],
//                     )
//                 })
//                 .collect()
//         })
//         .collect();

//     // Create transport backend
//     let transport_backend = Arc::new(Mutex::new(crate::transport::InMemoryTransportBackend::new()));

//     // Create writers and readers
//     let mut writers: Vec<TransportClient> = (0..num_writers)
//         .map(|i| TransportClient::new(format!("writer{}", i)))
//         .collect();
    
//     let mut readers: Vec<TransportClient> = (0..num_readers)
//         .map(|i| TransportClient::new(format!("reader{}", i)))
//         .collect();

//     // Register clients and create channels
//     runtime.block_on(async {
//         let mut backend = transport_backend.lock().await;
        
//         // Register all clients
//         for writer in &writers {
//             backend.register_client(writer.vertex_id().to_string(), writer.clone()).await?;
//         }
//         for reader in &readers {
//             backend.register_client(reader.vertex_id().to_string(), reader.clone()).await?;
//         }
        
//         // Create and register channels between each writer and reader
//         for writer_idx in 0..num_writers {
//             for reader_idx in 0..num_readers {
//                 let channel = Channel::Local {
//                     channel_id: format!("writer{}_to_reader{}", writer_idx, reader_idx),
//                 };
                
//                 // Register channel for writer (output)
//                 backend.register_channel(
//                     format!("writer{}", writer_idx),
//                     channel.clone(),
//                     false,
//                 ).await?;
                
//                 // Register channel for reader (input)
//                 backend.register_channel(
//                     format!("reader{}", reader_idx),
//                     channel.clone(),
//                     true,
//                 ).await?;
//             }
//         }
        
//         // Start the backend
//         backend.start().await?;
        
//         Ok::<(), anyhow::Error>(())
//     })?;

//     // Write test data from each writer to each reader
//     runtime.block_on(async {
//         for (writer_idx, writer) in writers.iter_mut().enumerate() {
//             let mut writer = writer.writer().expect("Writer should be initialized");
//             for reader_idx in 0..num_readers {
//                 for batch in &test_batches[writer_idx] {
//                     writer.write_batch(
//                         &format!("writer{}_to_reader{}", writer_idx, reader_idx),
//                         batch.clone(),
//                     ).await?;
//                 }
//             }
//         }
//         Ok::<(), anyhow::Error>(())
//     })?;

//     // Read and verify data from each reader
//     let received_batches = runtime.block_on(async {
//         let mut all_reader_batches = Vec::new();
//         for reader in readers.iter_mut() {
//             let mut reader = reader.reader().expect("Reader should be initialized");
//             let mut reader_batches = Vec::new();
//             // Each reader should receive batches_per_writer from each writer
//             for _ in 0..(num_writers * batches_per_writer) {
//                 if let Some(batch) = reader.read_batch().await? {
//                     reader_batches.push(batch);
//                 }
//             }
//             all_reader_batches.push(reader_batches);
//         }
//         Ok::<Vec<Vec<DataBatch>>, anyhow::Error>(all_reader_batches)
//     })?;

//     // Verify received data
//     assert_eq!(received_batches.len(), num_readers);
//     for reader_batches in received_batches {
//         assert_eq!(reader_batches.len(), num_writers * batches_per_writer);
        
//         // Group batches by writer
//         let mut writer_batches: Vec<Vec<String>> = vec![Vec::new(); num_writers];
//         for batch in reader_batches {
//             let content = batch.record_batch()[0].clone();
//             // Extract writer index from the batch content (format: "writer{idx}_batch{idx}")
//             let writer_idx = content.split('_').next().unwrap()
//                 .trim_start_matches("writer")
//                 .parse::<usize>()
//                 .unwrap();
//             writer_batches[writer_idx].push(content);
//         }

//         // Verify each writer's batches
//         for writer_idx in 0..num_writers {
//             assert_eq!(writer_batches[writer_idx].len(), batches_per_writer);
            
//             // Verify batches are in order for this writer
//             for batch_idx in 0..batches_per_writer {
//                 let expected = format!("writer{}_batch{}", writer_idx, batch_idx);
//                 let actual = writer_batches[writer_idx][batch_idx].clone();
//                 assert_eq!(actual, expected, 
//                     "Batches from writer {} are not in order. Expected {} but got {}", 
//                     writer_idx, expected, actual);
//             }
//         }
//     }

//     // Close the transport backend
//     runtime.block_on(async {
//         let mut backend = transport_backend.lock().await;
//         backend.close().await
//     })?;

//     Ok(())
// }
