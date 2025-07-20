use crate::{common::{test_utils::gen_unique_grpc_port, WatermarkMessage, MAX_WATERMARK_VALUE}, runtime::{
    execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionVertex}, functions::map::{MapFunction, MapFunctionTrait}, operators::{operator::OperatorConfig, sink::sink_operator::SinkConfig, source::source_operator::{SourceConfig, VectorSourceConfig}}, partition::{ForwardPartition, PartitionType}, storage::{InMemoryStorageClient, InMemoryStorageServer}, worker::{Worker, WorkerConfig}
}, transport::transport_backend_actor::TransportBackendType};
use crate::common::message::Message;
use crate::common::test_utils::create_test_string_batch;
use crate::runtime::tests::graph_test_utils::{create_test_execution_graph, TestGraphConfig};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::runtime::Runtime;
use std::thread;
use std::time::Duration;
use kameo::{Actor, spawn};
use crate::transport::channel::Channel;
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

    // Define operator chain: source -> map -> sink
    let operators = vec![
        ("source".to_string(), OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(VectorSourceConfig::new(test_messages.clone())))),
        ("map".to_string(), OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction))),
        ("sink".to_string(), OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageGrpcSinkConfig(format!("http://{}", storage_server_addr)))),
    ];

    let (graph, _) = create_test_execution_graph(TestGraphConfig {
        operators,
        parallelism: 1,
        chained: false,
        is_remote: false,
        num_workers_per_operator: None,
    });

    let vertex_ids = graph.get_vertices().keys().cloned().collect();
    let mut worker = Worker::new(WorkerConfig::new(
        graph,
        vertex_ids,
        4,
        TransportBackendType::InMemory,
    ));

    let (vector_messages, map_messages) = runtime.block_on(async {
        let mut storage_server = InMemoryStorageServer::new();
        storage_server.start(&storage_server_addr).await.unwrap();
        worker.execute_worker_lifecycle_for_testing().await;
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