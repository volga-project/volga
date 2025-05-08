// use crate::runtime::actors::{StreamTaskActor, StreamTaskMessage};
// use crate::runtime::task::StreamTask;
// use crate::runtime::runtime_context::RuntimeContext;
// use crate::runtime::execution_graph::OperatorConfig;
// use crate::common::data_batch::DataBatch;
// use crate::transport::test_utils::{DataReaderActor, DataWriterActor, DataReaderMessage, DataWriterMessage};
// use crate::transport::channel::Channel;
// use anyhow::Result;
// use kameo::{Actor, spawn};
// use tokio::runtime::Runtime;
// use std::sync::Arc;
// use tokio::sync::Mutex;
// use std::time::Duration;

// #[test]
// fn test_stream_task_actor() -> Result<()> {
//     let runtime = Runtime::new()?;
//     runtime.block_on(async {
//         // Create test data
//         let test_batches = vec![
//             DataBatch::new(None, vec!["test1".to_string()]),
//             DataBatch::new(None, vec!["test2".to_string()]),
//             DataBatch::new(None, vec!["test3".to_string()]),
//         ];

//         // Create task with MapOperator
//         let task = StreamTask::new(
//             "task1".to_string(),
//             OperatorConfig::MapConfig(std::collections::HashMap::new()),
//             RuntimeContext::new(
//                 0, // task_id
//                 0, // task_index
//                 1, // parallelism
//                 0, // operator_id
//                 "map".to_string(), // operator_name
//                 None, // job_config
//             ),
//         )?;

//         // Create external writer and reader actors
//         let input_actor = DataWriterActor::new("input".to_string());
//         let output_actor = DataReaderActor::new("output".to_string());

//         let input_ref = spawn(input_actor);
//         let output_ref = spawn(output_actor);

//         // Create task actor
//         let task_actor = StreamTaskActor::new(task);
//         let task_ref = spawn(task_actor);

//         // Create channels
//         let (input_sender, input_receiver) = tokio::sync::mpsc::channel(100);
//         let (output_sender, output_receiver) = tokio::sync::mpsc::channel(100);

//         // Register channels
//         input_ref.tell(DataWriterMessage::RegisterSender {
//             channel_id: "input_to_task".to_string(),
//             sender: input_sender,
//         }).await?;

//         output_ref.tell(DataReaderMessage::RegisterReceiver {
//             channel_id: "task_to_output".to_string(),
//             receiver: output_receiver,
//         }).await?;

//         // Open task
//         task_ref.tell(StreamTaskMessage::Open).await?;

//         // Create collector for task to output
//         task_ref.tell(StreamTaskMessage::CreateCollector {
//             channel_id: "task_to_output".to_string(),
//             partition_type: crate::runtime::partition::PartitionType::Forward,
//             target_operator_id: "output".to_string(),
//         }).await?;

//         // Run task
//         task_ref.tell(StreamTaskMessage::Run).await?;

//         // Write test data using external writer
//         for batch in &test_batches {
//             input_ref.tell(DataWriterMessage::WriteBatch {
//                 channel_id: "input_to_task".to_string(),
//                 batch: batch.clone(),
//             }).await?;
//         }

//         // Read and verify output using external reader
//         let mut received_batches = Vec::new();
//         for _ in 0..test_batches.len() {
//             let result = output_ref.ask(DataReaderMessage::ReadBatch).await?;
//             if let Some(batch) = result {
//                 received_batches.push(batch);
//             }
//         }

//         // Verify received data
//         assert_eq!(received_batches.len(), test_batches.len());
//         for (expected, actual) in test_batches.iter().zip(received_batches.iter()) {
//             assert_eq!(actual.record_batch(), expected.record_batch());
//         }

//         // Close task
//         task_ref.tell(StreamTaskMessage::Close).await?;

//         Ok(())
//     })
// } 