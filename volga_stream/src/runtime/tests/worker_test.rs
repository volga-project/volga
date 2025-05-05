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

#[test]
fn test_linear_execution_graph() -> Result<()> {
    // Create test data
    let test_batches = vec![
        DataBatch::new(None, vec!["test1".to_string()]),
        DataBatch::new(None, vec!["test2".to_string()]),
        DataBatch::new(None, vec!["test3".to_string()]),
    ];

    // Create execution graph
    let mut graph = ExecutionGraph::new();

    // Create source config with test data
    let source_config = SourceConfig::VectorSourceConfig(Arc::new(Mutex::new(test_batches)));

    // Create sink config with output vector
    let sink_output = Arc::new(Mutex::new(Vec::<DataBatch>::new()));
    let sink_config = SinkConfig::VectorSinkConfig(sink_output.clone());

    // Create vertices
    let source_vertex = ExecutionVertex::new(
        "0".to_string(),
        OperatorConfig::SourceConfig(source_config),
    );
    let map_vertex = ExecutionVertex::new(
        "1".to_string(),
        OperatorConfig::MapConfig(HashMap::new()),
    );
    let sink_vertex = ExecutionVertex::new(
        "2".to_string(),
        OperatorConfig::SinkConfig(sink_config),
    );

    // Add vertices to graph
    graph.add_vertex(source_vertex);
    graph.add_vertex(map_vertex);
    graph.add_vertex(sink_vertex);

    // Create edges with forward partitions
    let source_to_map = ExecutionEdge::new(
        "0".to_string(),
        "1".to_string(),
        PartitionType::Forward,
        crate::transport::channel::Channel::Local {
            channel_id: "0_to_1".to_string(),
        },
    );

    let map_to_sink = ExecutionEdge::new(
        "1".to_string(),
        "2".to_string(),
        PartitionType::Forward,
        crate::transport::channel::Channel::Local {
            channel_id: "1_to_2".to_string(),
        },
    );

    // Add edges to graph
    graph.add_edge(source_to_map)?;
    graph.add_edge(map_to_sink)?;

    // Create worker with all vertices
    let mut worker = Worker::new(
        graph,
        vec!["0".to_string(), "1".to_string(), "2".to_string()],
        1, // num_io_threads
        1, // num_compute_threads
    );

    // Start the worker
    worker.start()?;

    // Let it run for a short time
    thread::sleep(Duration::from_secs(2));

    // Close the worker
    worker.close()?;

    // Create a runtime to read the sink output
    let runtime = Runtime::new()?;
    let output = runtime.block_on(async {
        sink_output.lock().await
    });

    // Verify output
    assert_eq!(output.len(), 3);
    assert_eq!(output[0].record_batch()[0], "test1");
    assert_eq!(output[1].record_batch()[0], "test2");
    assert_eq!(output[2].record_batch()[0], "test3");

    Ok(())
} 