#![cfg(test)]

use crate::{
    api::{
        compile_logical_graph, spec::pipeline::ExecutionProfile, ConnectorConfigs,
        PipelineSpecBuilder,
    },
    common::{
        message::Message,
        test_utils::{
            create_test_string_batch, gen_unique_grpc_port, verify_message_records_match,
        },
        WatermarkMessage, MAX_WATERMARK_VALUE,
    },
    runtime::tests::pipeline_exec,
    runtime::operators::{
        sink::sink_operator::SinkConfig,
        source::source_operator::{SourceConfig, VectorSourceConfig},
    },
    storage::{InMemoryStorageClient, InMemoryStorageServer},
};
use anyhow::Result;
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
use tokio::runtime::Runtime;

// TODO test early worker interruption/shutdown via setting state to finished from outside
#[test]
fn test_worker_execution() -> Result<()> {
    let runtime = Runtime::new()?;

    let expected_messages = vec![
        Message::new(
            None,
            create_test_string_batch(vec!["test1".to_string()]),
            None,
            None,
        ),
        Message::new(
            None,
            create_test_string_batch(vec!["test2".to_string()]),
            None,
            None,
        ),
        Message::new(
            None,
            create_test_string_batch(vec!["test3".to_string()]),
            None,
            None,
        ),
    ];

    let mut test_messages = expected_messages.clone();
    test_messages.push(Message::Watermark(WatermarkMessage::new(
        "source".to_string(),
        MAX_WATERMARK_VALUE,
        None,
    )));

    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());

    // Create schema for the test table
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));

    // Create spec using SQL
    let spec = PipelineSpecBuilder::new()
        .with_parallelism(1)
        .sql("SELECT value FROM test_table")
        .with_execution_profile(ExecutionProfile::SingleWorker {
            num_threads_per_task: 4,
        })
        .build();
    let mut connector_configs = ConnectorConfigs::default();
    connector_configs.sources.insert(
        "test_table".to_string(),
        (
            SourceConfig::VectorSourceConfig(VectorSourceConfig::new(test_messages)),
            schema,
        ),
    );
    connector_configs.sink = Some(SinkConfig::InMemoryStorageGrpcSinkConfig(format!(
        "http://{}",
        storage_server_addr
    )));
    let logical_graph = compile_logical_graph(&spec, Some(&connector_configs));

    let vector_messages = runtime.block_on(async {
        let mut storage_server = InMemoryStorageServer::new();
        storage_server.start(&storage_server_addr).await.unwrap();

        pipeline_exec::execute(spec, logical_graph)
            .await
            .unwrap();

        let mut client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr))
            .await
            .unwrap();
        let vector_messages = client.get_vector().await.unwrap();
        storage_server.stop().await;
        vector_messages
    });

    // Use the new utility function to verify all records match
    verify_message_records_match(
        &expected_messages,
        &vector_messages,
        "worker_execution_test",
        true,
    );

    Ok(())
}
