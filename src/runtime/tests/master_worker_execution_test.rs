use std::collections::HashMap;

use anyhow::Result;
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow_integration_test::schema_to_json;
use datafusion::common::ScalarValue;

use crate::{
    api::{
        compile_logical_graph,
        spec::connectors::{SinkSpec, SourceSpec, SourceSpecKind},
        spec::pipeline::ExecutionProfile,
        PipelineSpecBuilder, TaskWorkerAssignmentStrategyType,
    },
    common::test_utils::gen_unique_grpc_port,
    executor::local_master_worker,
    runtime::functions::source::datagen_source::{DatagenSpec, FieldGenerator},
    storage::{InMemoryStorageClient, InMemoryStorageServer},
};

async fn run_execution(assignment_strategy: TaskWorkerAssignmentStrategyType) -> Result<()> {
    let num_workers = match assignment_strategy {
        TaskWorkerAssignmentStrategyType::SingleWorker => 1,
        TaskWorkerAssignmentStrategyType::OperatorPerWorker => 3,
    };

    let total_parallelism = 4;
    let num_messages_per_source = 10;

    let storage_server_addr = format!("127.0.0.1:{}", gen_unique_grpc_port());
    let mut storage_server = InMemoryStorageServer::new();
    storage_server.start(&storage_server_addr).await?;

    let schema = Schema::new(vec![Field::new("value", DataType::Utf8, false)]);
    let schema_json = schema_to_json(&schema);
    let expected_total_rows = num_messages_per_source * total_parallelism;
    let values = (0..num_messages_per_source)
        .map(|i| ScalarValue::Utf8(Some(format!("value_{}", i))))
        .collect::<Vec<_>>();
    let mut fields = HashMap::new();
    fields.insert("value".to_string(), FieldGenerator::Values { values });
    let is_operator_per_worker = matches!(
        assignment_strategy,
        TaskWorkerAssignmentStrategyType::OperatorPerWorker
    );
    let spec = PipelineSpecBuilder::new()
        .with_parallelism(total_parallelism)
        .with_source(
            SourceSpec::new(
                "test_table",
                SourceSpecKind::Datagen(DatagenSpec {
                rate: None,
                limit: Some(expected_total_rows),
                run_for_s: None,
                batch_size: 64,
                fields,
                replayable: true,
                }),
                schema_json,
            )
        )
        .with_sink(SinkSpec::InMemoryStorageGrpc {
            server_addr: format!("http://{}", storage_server_addr),
        })
        .sql("SELECT value FROM test_table")
        .with_execution_profile(ExecutionProfile::MasterWorker {
            num_threads_per_task: total_parallelism,
        })
        .with_node_assignment_strategy(assignment_strategy)
        .build();
    let logical_graph = compile_logical_graph(&spec, None);
    if is_operator_per_worker {
        let logical_graph_node_count = logical_graph.get_nodes().count();
        assert_eq!(
            num_workers, logical_graph_node_count,
            "OperatorPerWorker strategy expects worker count to match source->projection->sink logical node count"
        );
        assert_eq!(
            num_workers, 3,
            "Expected source->projection->sink query to use 3 workers/logical nodes in OperatorPerWorker test"
        );
    }

    local_master_worker::execute(spec, logical_graph, num_workers).await?;

    let mut client = InMemoryStorageClient::new(format!("http://{}", storage_server_addr)).await?;
    let result_messages = client.get_vector().await?;
    storage_server.stop().await;

    let mut value_counts: HashMap<String, usize> = HashMap::new();
    let mut total_rows = 0usize;
    for message in result_messages {
        let batch = message.record_batch();
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("value column must be StringArray");
        for row_idx in 0..batch.num_rows() {
            total_rows += 1;
            let value = values.value(row_idx).to_string();
            *value_counts.entry(value).or_insert(0) += 1;
        }
    }

    assert_eq!(
        total_rows, expected_total_rows,
        "Expected {} total rows, got {}",
        expected_total_rows, total_rows
    );
    assert_eq!(
        value_counts.len(),
        num_messages_per_source,
        "Expected {} unique values, got {}",
        num_messages_per_source,
        value_counts.len()
    );

    for i in 0..num_messages_per_source {
        let value = format!("value_{}", i);
        let count = value_counts.get(&value).copied().unwrap_or(0);
        assert!(
            count > 0,
            "Expected value '{}' to appear at least once, got {}",
            value,
            count
        );
    }

    Ok(())
}

// TODO: These tests pass individually but can fail when running the full suite (likely cross-test interference).

#[tokio::test]
async fn test_master_single_worker() -> Result<()> {
    run_execution(TaskWorkerAssignmentStrategyType::SingleWorker).await
}

#[tokio::test]
async fn test_master_multiple_workers() -> Result<()> {
    run_execution(TaskWorkerAssignmentStrategyType::OperatorPerWorker).await
}
