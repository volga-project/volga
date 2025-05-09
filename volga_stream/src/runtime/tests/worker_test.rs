use crate::runtime::{
    execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionVertex, OperatorConfig, SourceConfig, SinkConfig}, operator::SourceOperator, partition::{ForwardPartition, PartitionType}, worker::Worker
};
use crate::common::data_batch::DataBatch;
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::runtime::Runtime;
use std::thread;
use std::time::Duration;
use crate::runtime::storage::in_memory_storage_actor::{InMemoryStorageActor, InMemoryStorageMessage};
use kameo::{Actor, spawn};
use crate::transport::channel::Channel;

#[test]
fn test_worker() -> Result<()> {
    // Create runtime for async operations
    let runtime = Runtime::new()?;

    // Create test data
    let test_batches = vec![
        DataBatch::new(None, vec!["test1".to_string()]),
        DataBatch::new(None, vec!["test2".to_string()]),
        DataBatch::new(None, vec!["test3".to_string()]),
    ];

    // Create storage actor
    let storage_actor = InMemoryStorageActor::new();
    let storage_ref = runtime.block_on(async {
        spawn(storage_actor)
    });

    // Create execution graph
    let mut graph = ExecutionGraph::new();

    // Create source vertex
    let source_vertex = ExecutionVertex::new(
        "source".to_string(),
        OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(test_batches.clone())),
    );
    graph.add_vertex(source_vertex);

    // Create map vertex
    let map_vertex = ExecutionVertex::new(
        "map".to_string(),
        OperatorConfig::MapConfig(std::collections::HashMap::new()),
    );
    graph.add_vertex(map_vertex);

    // Create sink vertex with storage actor
    let sink_vertex = ExecutionVertex::new(
        "sink".to_string(),
        OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageActorSinkConfig(storage_ref.clone())),
    );
    graph.add_vertex(sink_vertex);

    // Add edges
    let source_to_map = ExecutionEdge::new(
        "source".to_string(),
        "map".to_string(),
        PartitionType::Forward,
        Channel::Local {
            channel_id: "source_to_map".to_string(),
        },
    );
    graph.add_edge(source_to_map)?;

    let map_to_sink = ExecutionEdge::new(
        "map".to_string(),
        "sink".to_string(),
        PartitionType::Forward,
        Channel::Local {
            channel_id: "map_to_sink".to_string(),
        },
    );
    graph.add_edge(map_to_sink)?;

    // Create and start worker
    let mut worker = Worker::new(
        graph,
        vec!["source".to_string(), "map".to_string(), "sink".to_string()],
        1,
    );
    worker.start()?;

    // Wait for processing to complete
    thread::sleep(Duration::from_secs(1));

    // Verify results by reading from storage actor
    let result = runtime.block_on(async {
        storage_ref.ask(InMemoryStorageMessage::GetVector).await
    })?;
    let result_batch = result.expect("Expected data in storage");
    let result_records = result_batch.record_batch();
    assert_eq!(result_records.len(), test_batches.len());
    for (expected, actual) in test_batches.iter().zip(result_records.iter()) {
        assert_eq!(actual, &expected.record_batch()[0]);
    }

    // Close worker
    worker.close()?;

    Ok(())
} 