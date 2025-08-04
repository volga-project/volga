use crate::{common::test_utils::{create_test_string_batch, gen_unique_grpc_port}, runtime::{
    functions::{
        key_by::KeyByFunction,
        map::{MapFunction, MapFunctionTrait},
    }, operators::{operator::OperatorConfig, sink::sink_operator::SinkConfig, source::source_operator::{SourceConfig, VectorSourceConfig}}, worker::{Worker, WorkerConfig}
}, transport::transport_backend_actor::TransportBackendType, storage::{InMemoryStorageClient, InMemoryStorageServer}};
use crate::common::message::{Message, WatermarkMessage};
use crate::common::MAX_WATERMARK_VALUE;
use crate::runtime::tests::graph_test_utils::{create_linear_test_execution_graph, TestLinearGraphConfig};
use anyhow::Result;
use tokio::runtime::Runtime;
use arrow::array::StringArray;
use async_trait::async_trait;

#[derive(Debug, Clone)]
struct IdentityMapFunction;

#[async_trait]
impl MapFunctionTrait for IdentityMapFunction {
    fn map(&self, _message: Message) -> Result<Message> {
        Ok(_message)
    }
}

/// Runs a test with the given configuration and returns the result messages
async fn run_test_with_config(
    chained: bool,
) -> Result<Vec<Message>> {
    // Create test data
    let mut test_messages = vec![
        Message::new(None, create_test_string_batch(vec!["test1".to_string()]), None),
        Message::new(None, create_test_string_batch(vec!["test2".to_string()]), None),
        Message::new(None, create_test_string_batch(vec!["test3".to_string()]), None),
    ];

    test_messages.push(Message::Watermark(WatermarkMessage::new(
        "source".to_string(),
        MAX_WATERMARK_VALUE,
        None,
    )));

    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());

    // Define operator chain: source -> map1 -> keyby -> map2 -> sink
    let operators = vec![
        OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(VectorSourceConfig::new(test_messages))),
        OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
        OperatorConfig::KeyByConfig(KeyByFunction::new_arrow_key_by(vec!["value".to_string()])),
        OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
        OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig(format!("http://{}", storage_server_addr))),
    ];

    let config = TestLinearGraphConfig {
        operators,
        parallelism: 4,
        chained,
        is_remote: false,
        num_workers_per_operator: None,
    };

    let (graph, _) = create_linear_test_execution_graph(config).await;

    // Create worker config - use all vertices from the graph
    let vertex_ids: Vec<String> = graph.get_vertices().keys().cloned().collect();
    let worker_config = WorkerConfig::new(
        graph,
        vertex_ids,
        1,
        TransportBackendType::InMemory,
    );
    let mut worker = Worker::new(worker_config);

    // Run the test
    let result_messages = tokio::task::spawn_blocking(move || {
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            let mut storage_server = InMemoryStorageServer::new();
            storage_server.start(&storage_server_addr).await.unwrap();
            worker.execute_worker_lifecycle_for_testing().await;
            let mut client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await.unwrap();
            let result_messages = client.get_vector().await.unwrap();
            storage_server.stop().await;
            result_messages
        })
    }).await.unwrap();

    Ok(result_messages)
}

/// Compares two result sets and verifies they are identical
fn verify_results_identical(result1: &[Message], result2: &[Message], test_name: &str) {
    assert_eq!(result1.len(), result2.len(), 
        "{}: Expected same number of messages, got {} vs {}", 
        test_name, result1.len(), result2.len());

    for (i, (msg1, msg2)) in result1.iter().zip(result2.iter()).enumerate() {
        let value1 = msg1.record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0);
        let value2 = msg2.record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0);
        assert_eq!(value1, value2, 
            "{}: Message {} differs: '{}' vs '{}'", 
            test_name, i, value1, value2);
    }
}

#[test]
fn test_chained_vs_unchained_consistency() -> Result<()> {
    let runtime = Runtime::new()?;
    
    let (chained_results, unchained_results) = runtime.block_on(async {
        let chained_results = run_test_with_config(true).await?;
        let unchained_results = run_test_with_config(false).await?;
        Ok::<_, anyhow::Error>((chained_results, unchained_results))
    })?;

    verify_results_identical(&chained_results, &unchained_results, "Local chained vs unchained");
    
    println!("[TEST] Local chained vs unchained: {} messages, results identical", chained_results.len());
    Ok(())
} 