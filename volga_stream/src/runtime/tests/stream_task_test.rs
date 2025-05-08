// use crate::runtime::{
//     task::{StreamTask, Task},
//     operator::MapOperator,
//     runtime_context::RuntimeContext,
//     partition::{create_partition, PartitionType},
//     execution_graph::OperatorConfig,
// };
// use crate::transport::{
//     transport_backend::TransportBackend,
//     transport_client::{TransportClient, DataWriter, DataReader},
//     channel::Channel,
//     InMemoryTransportBackend,
// };
// use crate::common::data_batch::DataBatch;
// use anyhow::Result;
// use std::{sync::Arc, time::Duration};
// use tokio::sync::Mutex;
// use tokio::runtime::Runtime;

// #[test]
// fn test_stream_task_map_operator() -> Result<()> {

//     // Create a runtime for all operations
//     let runtime = Runtime::new()?;

//     // Create test data
//     let test_batches = vec![
//         DataBatch::new(None, vec!["test1".to_string()]),
//         DataBatch::new(None, vec!["test2".to_string()]),
//         DataBatch::new(None, vec!["test3".to_string()]),
//     ];

//     // Create transport backend
//     let transport_backend = Arc::new(Mutex::new(InMemoryTransportBackend::new()));

//     // Create task with MapOperator
//     let task = Arc::new(Mutex::new(StreamTask::new(
//         "task1".to_string(),
//         OperatorConfig::MapConfig(std::collections::HashMap::new()),
//         RuntimeContext::new(
//             0, // task_id
//             0, // task_index
//             1, // parallelism
//             0, // operator_id
//             "map".to_string(), // operator_name
//             None, // job_config
//             None, // compute_pool
//         ),
//     )?));

//     // Create external writer and reader clients
//     let input_client = TransportClient::new("input".to_string());
//     let output_client = TransportClient::new("output".to_string());

//     // Register clients and create channels
//     runtime.block_on(async {
//         let mut backend = transport_backend.lock().await;
        
//         // Register all clients
//         let task_client = {
//             let task = task.lock().await;
//             task.transport_client()
//         };
//         let task_client_id = task_client.vertex_id().to_string();
//         backend.register_client(task_client_id.clone(), task_client).await?;
//         backend.register_client(input_client.vertex_id().to_string(), input_client.clone()).await?;
//         backend.register_client(output_client.vertex_id().to_string(), output_client.clone()).await?;
        
//         // Create and register input channel (from external writer to task)
//         let input_channel = Channel::Local {
//             channel_id: "input_to_task".to_string(),
//         };
//         backend.register_channel(
//             input_client.vertex_id().to_string(),
//             input_channel.clone(),
//             false,
//         ).await?;
//         backend.register_channel(
//             task_client_id.clone(),
//             input_channel.clone(),
//             true,
//         ).await?;

//         // Create and register output channel (from task to external reader)
//         let output_channel = Channel::Local {
//             channel_id: "task_to_output".to_string(),
//         };
//         backend.register_channel(
//             task_client_id.clone(),
//             output_channel.clone(),
//             false,
//         ).await?;
//         backend.register_channel(
//             output_client.vertex_id().to_string(),
//             output_channel.clone(),
//             true,
//         ).await?;

//         // Start the backend
//         backend.start().await?;
        
//         Ok::<(), anyhow::Error>(())
//     })?;

//     // Set up the task's output collector
//     runtime.block_on(async {
//         let mut task = task.lock().await;
//         task.create_or_update_collector(
//             "task_to_output".to_string(),
//             create_partition(PartitionType::Forward),
//             "output".to_string(),
//         )?;
//         Ok::<(), anyhow::Error>(())
//     })?;

//     // Start the task
//     let task_clone = task.clone();
//     runtime.block_on(async {
//         let mut task = task_clone.lock().await;
//         task.open().await?;
//         task.run().await?;
//         Ok::<(), anyhow::Error>(())
//     });

//     // Write test data using external writer
//     runtime.block_on(async {
//         let mut writer = input_client.writer().expect("Writer should be initialized");
//         for batch in &test_batches {
//             writer.write_batch("input_to_task", batch.clone()).await?;
//         }
//         Ok::<(), anyhow::Error>(())
//     })?;

//     // Read and verify output using external reader
//     let received_batches = runtime.block_on(async {
//         let reader = output_client.reader().expect("Reader should be initialized");
//         let mut batches = Vec::new();
//         // We expect the same number of batches as input
//         let mut reader = reader;
//         for _ in 0..test_batches.len() {
//             if let Some(batch) = reader.read_batch_with_params(Some(Duration::from_millis(100)), Some(10)).await? {
//                 batches.push(batch);
//             }
//         }
//         Ok::<Vec<DataBatch>, anyhow::Error>(batches)
//     })?;

//     // Verify received data
//     assert_eq!(received_batches.len(), test_batches.len());
//     for (expected, actual) in test_batches.iter().zip(received_batches.iter()) {
//         assert_eq!(actual.record_batch(), expected.record_batch());
//     }

//     // Wait for task to complete
//     runtime.block_on(async {
//         let mut task = task.lock().await;
//         task.close().await?;    
//         Ok::<(), anyhow::Error>(())
//     })?;

//     // // Close the transport backend
//     // runtime.block_on(async {
//     //     let mut backend = transport_backend.lock().await;
//     //     backend.close().await
//     // })?;

//     Ok(())
// } 