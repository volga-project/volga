use crate::{
    api::{logical_graph::LogicalGraph, ExecutionProfile, PipelineContext, PipelineSpecBuilder},
    common::{test_utils::{create_test_string_batch, gen_unique_grpc_port}, message::{Message, WatermarkMessage}, MAX_WATERMARK_VALUE},
    runtime::{
        functions::{key_by::KeyByFunction, map::{MapFunction, MapFunctionTrait}},
        operators::{operator::OperatorConfig, sink::sink_operator::SinkConfig, source::source_operator::{SourceConfig, VectorSourceConfig}},
    },
    storage::{InMemoryStorageClient, InMemoryStorageServer}
};
use anyhow::Result;
use tokio::runtime::Runtime;
use arrow::array::StringArray;
use async_trait::async_trait;

#[derive(Debug, Clone)]
struct KeyedToRegularMapFunction;

#[async_trait]
impl MapFunctionTrait for KeyedToRegularMapFunction {
    fn map(&self, message: Message) -> Result<Message> {
        let value = message.record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0);
        let upstream_vertex_id = message.upstream_vertex_id();
        let ingest_ts = message.ingest_timestamp();
        let extras = message.get_extras();
        match message {
            Message::Keyed(keyed_message) => {
                // Create a new regular message with the same record batch
                Ok(Message::new(upstream_vertex_id, keyed_message.base.record_batch, ingest_ts, extras))
            }
            _ => Ok(message), // Pass through non-keyed messages (like watermarks)
        }
    }
}

// TODO this fails - fix
// #[test]
fn test_distributed_execution() -> Result<()> {
    let parallelism_per_worker = 2;
    let num_messages_per_source = 10;
    
    let runtime = Runtime::new()?;

    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());

    let result_messages = runtime.block_on(async {
        // Start storage server
        let mut storage_server = InMemoryStorageServer::new();
        storage_server.start(&storage_server_addr).await.unwrap();
        println!("[TEST] Started storage server on {}", storage_server_addr);

        // Create test data
        let mut source_messages = Vec::new();
        
        // Add regular messages
        for i in 0..num_messages_per_source {
            source_messages.push(Message::new(
                None,
                create_test_string_batch(vec![format!("value_{}", i)]),
                None,
                None
            ));
        }
        
        // Add max watermark as the last message
        source_messages.push(Message::Watermark(WatermarkMessage::new(
            "source".to_string(),
            MAX_WATERMARK_VALUE,
            None,
        )));

        // Define operator chain: source -> keyby -> map -> sink
        let operators = vec![
            OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(VectorSourceConfig::new(source_messages))),
            OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["value".to_string()])),
            OperatorConfig::MapConfig(MapFunction::new_custom(KeyedToRegularMapFunction)),
            OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig(format!("http://{}", storage_server_addr))),
        ];

        // Create streaming context with local full orchestration (master + worker servers)
        // TODO: this is semantically wrong now that worker count is derived; fix when enabling test.
        let total_parallelism = parallelism_per_worker;
        let spec = PipelineSpecBuilder::new()
            .with_parallelism(total_parallelism)
            .with_logical_graph(LogicalGraph::from_linear_operators(operators, total_parallelism, false))
            .with_execution_profile(ExecutionProfile::local_default())
            .build();
        let context = PipelineContext::new(spec);

        println!("[TEST] Starting distributed execution");
        
        context.execute().await.unwrap();
        println!("[TEST] Distributed execution completed successfully");

        // Get results from storage
        let mut client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await.unwrap();
        let result_messages = client.get_vector().await.unwrap();
        
        // Stop storage server
        storage_server.stop().await;
        
        result_messages
    });

    println!("[TEST] Test completed");

    // Verify results
    let result_len = result_messages.len();
    let mut values = Vec::new();
    for message in result_messages {
        let value = message.record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0);
        values.push(value.to_string());
    }

    println!("[TEST] Result values: {:?}", values);
    let total_parallelism = parallelism_per_worker;
    let expected_total_messages = num_messages_per_source * total_parallelism;
    assert_eq!(result_len, expected_total_messages, 
        "Expected {} messages ({} unique values × {} total parallelism), got {}", 
        expected_total_messages, num_messages_per_source, total_parallelism, values.len());

    // Verify all expected values are present, each appearing total_parallelism times
    for i in 0..num_messages_per_source {
        let value = format!("value_{}", i);
        let count = values.iter().filter(|&v| v == &value).count();
        assert_eq!(count, total_parallelism, 
            "Expected value '{}' to appear {} times (total parallelism), but found {} times", 
            value, total_parallelism, count);
    }

    Ok(())
} 