use crate::{
    api::{logical_graph::LogicalGraph, streaming_context::StreamingContext},
    common::{test_utils::gen_unique_grpc_port, WatermarkMessage, MAX_WATERMARK_VALUE, message::Message, test_utils::create_test_string_batch},
    executor::{local_executor::LocalExecutor, executor::Executor},
    runtime::{
        functions::map::{MapFunction, MapFunctionTrait},
        operators::{operator::OperatorConfig, sink::sink_operator::SinkConfig, source::source_operator::{SourceConfig, VectorSourceConfig}},
    },
    storage::{InMemoryStorageClient, InMemoryStorageServer}
};
use anyhow::Result;
use tokio::runtime::Runtime;
use async_trait::async_trait;
use arrow::array::StringArray;

#[derive(Debug, Clone)]
struct IdentityMapFunction;

#[async_trait]
impl MapFunctionTrait for IdentityMapFunction {
    fn map(&self, message: Message) -> Result<Message> {
        Ok(message)
    }
}

// TODO test early worker interruption/shutdown via setting state to finished from outside
#[test]
fn test_worker_execution() -> Result<()> {
    let runtime = Runtime::new()?;

    let mut test_messages = vec![
        Message::new(None, create_test_string_batch(vec!["test1".to_string()]), None),
        Message::new(None, create_test_string_batch(vec!["test2".to_string()]), None),
        Message::new(None, create_test_string_batch(vec!["test3".to_string()]), None),
    ];

    let num_messages = test_messages.len();

    test_messages.push(Message::Watermark(WatermarkMessage::new(
        "source".to_string(),
        MAX_WATERMARK_VALUE,
        None,
    )));

    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());
    
    // Create streaming context with the logical graph and executor
    let context = StreamingContext::new()
        .with_parallelism(1)
        .with_logical_graph(
            // Define operator chain: source -> map -> sink
            LogicalGraph::from_operator_list(vec![
                OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(VectorSourceConfig::new(test_messages.clone()))),
                OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
                OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig(format!("http://{}", storage_server_addr))),
            ], 1, false))
        .with_executor(Box::new(LocalExecutor::new()));

    let (vector_messages, map_messages) = runtime.block_on(async {
        let mut storage_server = InMemoryStorageServer::new();
        storage_server.start(&storage_server_addr).await.unwrap();
        
        context.execute().await.unwrap();
        
        let mut client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await.unwrap();
        let vector_messages = client.get_vector().await.unwrap();
        let map_messages = client.get_map().await.unwrap();
        storage_server.stop().await;
        (vector_messages, map_messages)
    });

    assert_eq!(vector_messages.len(), num_messages);
    assert_eq!(map_messages.len(), 0);
    for (expected, actual) in test_messages.iter().zip(vector_messages.iter()) {
        assert_eq!(actual.record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), expected.record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0));
    }

    Ok(())
} 