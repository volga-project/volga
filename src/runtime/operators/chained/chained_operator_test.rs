use crate::{
    common::{Message, WatermarkMessage, MAX_WATERMARK_VALUE},
    test_utils::common::{create_test_string_batch, gen_unique_grpc_port},
    runtime::{
        functions::map::{MapFunction, MapFunctionTrait},
        operators::{
            chained::chained_operator::ChainedOperator, operator::{OperatorConfig, OperatorTrait, OperatorType}, sink::sink_operator::SinkConfig, source::source_operator::{SourceConfig, VectorSourceConfig}
        }, runtime_context::RuntimeContext},
    storage::{InMemoryStorageClient, InMemoryStorageServer}
};
use anyhow::Result;
use async_trait::async_trait;
use arrow::array::StringArray;
use std::sync::Arc;

// Simple test map functions
#[derive(Debug, Clone)]
struct AddPrefixMapFunction {
    prefix: String,
}

#[async_trait]
impl MapFunctionTrait for AddPrefixMapFunction {
    fn map(&self, message: Message) -> Result<Message> {
        let record_batch = message.record_batch();
        let schema = record_batch.schema();
        
        let input_array = record_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        
        let output_array: StringArray = input_array.iter()
            .map(|v| v.map(|x| format!("{}{}", self.prefix, x)))
            .collect();
        
        let new_batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(output_array)]
        )?;
        
        Ok(Message::new(message.upstream_vertex_id(), new_batch, message.ingest_timestamp(), None))
    }
}

#[derive(Debug, Clone)]
struct ToUpperCaseMapFunction;

#[async_trait]
impl MapFunctionTrait for ToUpperCaseMapFunction {
    fn map(&self, message: Message) -> Result<Message> {
        let record_batch = message.record_batch();
        let schema = record_batch.schema();
        
        let input_array = record_batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        
        let output_array: StringArray = input_array.iter()
            .map(|v| v.map(|x| x.to_uppercase()))
            .collect();
        
        let new_batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(output_array)]
        )?;
        
        Ok(Message::new(message.upstream_vertex_id(), new_batch, message.ingest_timestamp(), None))
    }
}

// TODO enable tests when chaining is implemented properly
// TODO test sink chaining

// #[tokio::test]
async fn test_chained_operator_map_map() {
    let configs = vec![
        OperatorConfig::MapConfig(MapFunction::new_custom(AddPrefixMapFunction { prefix: "PREFIX_".to_string() })),
        OperatorConfig::MapConfig(MapFunction::new_custom(ToUpperCaseMapFunction)),
    ];
    let mut chained_operator = ChainedOperator::new(OperatorConfig::ChainedConfig(configs));
    let context = RuntimeContext::new("test_vertex".to_string().into(), 0, 1, None, None, None);
    
    chained_operator.open(&context).await.unwrap();
    
    // Set up input stream
    let input_batch = create_test_string_batch(vec!["hello".to_string(), "world".to_string()]);
    let input_message = Message::new(None, input_batch, None, None);
    let watermark = Message::Watermark(WatermarkMessage::new("test".to_string(), 1000, None));
    
    let input_stream = Box::pin(futures::stream::iter(vec![input_message, watermark]));
    chained_operator.set_input(Some(input_stream));
    
    // Test processing regular message
    let result = chained_operator.poll_next().await.get_result_message();
    let result_array = result.record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(result_array.value(0), "PREFIX_HELLO");
    assert_eq!(result_array.value(1), "PREFIX_WORLD");
    
    // Test processing watermark message
    let watermark_result = chained_operator.poll_next().await.get_result_message();
    assert!(matches!(watermark_result, Message::Watermark(_)));
    
    chained_operator.close().await.unwrap();
}

// #[tokio::test]
async fn test_chained_operator_source_map() {
    // Create test messages for source
    let test_messages = vec![
        Message::new(None, create_test_string_batch(vec!["hello".to_string()]), None, None),
        Message::new(None, create_test_string_batch(vec!["world".to_string()]), None, None),
        Message::Watermark(WatermarkMessage::new("source".to_string(), MAX_WATERMARK_VALUE, None)),
    ];
    let configs = vec![
        OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(VectorSourceConfig::new(test_messages))),
        OperatorConfig::MapConfig(MapFunction::new_custom(ToUpperCaseMapFunction)),
    ];

    let mut chained_operator = ChainedOperator::new(OperatorConfig::ChainedConfig(configs));
    let context = RuntimeContext::new("test_vertex".to_string().into(), 0, 1, None, None, None);
    
    // Test open
    chained_operator.open(&context).await.unwrap();
    
    // Test next from source - should get regular messages first
    let result1 = chained_operator.poll_next().await.get_result_message();
    let result2 = chained_operator.poll_next().await.get_result_message();
    
    // Verify results are uppercase
    let array1 = result1.record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap();
    let array2 = result2.record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap();
    
    assert_eq!(array1.value(0), "HELLO");
    assert_eq!(array2.value(0), "WORLD");
    
    // Test next watermark - should pass through unchanged
    let watermark_result = chained_operator.poll_next().await.get_result_message();
    assert!(matches!(watermark_result, Message::Watermark(_)));
    
    // Test close
    chained_operator.close().await.unwrap();
}

#[tokio::test]
async fn test_chained_operator_type() {
    // Test with only map operators (Processor)
    let map_configs = vec![
        OperatorConfig::MapConfig(MapFunction::new_custom(AddPrefixMapFunction { prefix: "PREFIX_".to_string() })),
        OperatorConfig::MapConfig(MapFunction::new_custom(ToUpperCaseMapFunction)),
    ];
    let map_chained = ChainedOperator::new(OperatorConfig::ChainedConfig(map_configs));
    assert_eq!(map_chained.operator_type(), OperatorType::Processor);
    
    // Test with source operator (Source)
    let source_configs = vec![
        OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![]))),
        OperatorConfig::MapConfig(MapFunction::new_custom(ToUpperCaseMapFunction)),
    ];
    let source_chained = ChainedOperator::new(OperatorConfig::ChainedConfig(source_configs));
    assert_eq!(source_chained.operator_type(), OperatorType::Source);
    
    // Test with sink operator (Sink)
    let sink_configs = vec![
        OperatorConfig::MapConfig(MapFunction::new_custom(ToUpperCaseMapFunction)),
        OperatorConfig::SinkConfig(SinkConfig::in_memory_grpc("test")),
    ];
    let sink_chained = ChainedOperator::new(OperatorConfig::ChainedConfig(sink_configs));
    assert_eq!(sink_chained.operator_type(), OperatorType::Sink);
    
    // Test with both source and sink operators (ChainedSourceSink)
    let source_sink_configs = vec![
        OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![]))),
        OperatorConfig::MapConfig(MapFunction::new_custom(ToUpperCaseMapFunction)),
        OperatorConfig::SinkConfig(SinkConfig::in_memory_grpc("test")),
    ];
    let source_sink_chained = ChainedOperator::new(OperatorConfig::ChainedConfig(source_sink_configs));
    assert_eq!(source_sink_chained.operator_type(), OperatorType::ChainedSourceSink);
}

// #[tokio::test]
async fn test_chained_operator_source_map_sink() {
    // Create test messages for source
    let test_messages = vec![
        Message::new(None, create_test_string_batch(vec!["hello".to_string()]), None, None),
        Message::new(None, create_test_string_batch(vec!["world".to_string()]), None, None),
    ];

    let num_messages = test_messages.len();
    
    // Set up gRPC storage server
    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());
    let mut storage_server = InMemoryStorageServer::new();
    storage_server.start(&storage_server_addr).await.unwrap();
    
    let configs = vec![
        OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(VectorSourceConfig::new(test_messages))),
        OperatorConfig::MapConfig(MapFunction::new_custom(ToUpperCaseMapFunction)),
        OperatorConfig::SinkConfig(SinkConfig::in_memory_grpc(format!(
            "http://{}",
            storage_server_addr
        ))),
    ];
    
    let mut chained_operator = ChainedOperator::new(OperatorConfig::ChainedConfig(configs));
    
    // Verify operator type is ChainedSourceSink
    assert_eq!(chained_operator.operator_type(), OperatorType::ChainedSourceSink);
    
    let context = RuntimeContext::new("test_vertex".to_string().into(), 0, 1, None, None, None);
    
    // Test open
    chained_operator.open(&context).await.unwrap();
    
    for _ in 0..num_messages {
        chained_operator.poll_next().await;
    }
    
    // Test close
    chained_operator.close().await.unwrap();
    
    // Verify messages were actually stored in the gRPC storage
    let mut client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await.unwrap();
    let stored_messages = client.get_vector().await.unwrap();
    
    // Should have received 2 messages
    assert_eq!(stored_messages.len(), 2);
    
    // Verify the stored messages are uppercase
    let stored_array1 = stored_messages[0].record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap();
    let stored_array2 = stored_messages[1].record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap();
    
    assert_eq!(stored_array1.value(0), "HELLO");
    assert_eq!(stored_array2.value(0), "WORLD");
    
    // Clean up
    storage_server.stop().await;
}