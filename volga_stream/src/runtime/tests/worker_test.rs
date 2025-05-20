use crate::runtime::{
    execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionVertex, OperatorConfig, SourceConfig, SinkConfig}, 
    operator::SourceOperator, 
    partition::{ForwardPartition, PartitionType}, 
    worker::Worker,
    // map_function::{MapFunction, MapFunctionTrait},
    functions::{
        map::MapFunction,
        map::MapFunctionTrait,
    },
    storage::in_memory_storage_actor::{InMemoryStorageActor, InMemoryStorageMessage, InMemoryStorageReply},
};
use crate::common::data_batch::DataBatch;
use crate::common::test_utils::create_test_string_batch;
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
    async fn map(&self, batch: DataBatch) -> Result<DataBatch> {
        Ok(batch)
    }
}

#[test]
fn test_worker() -> Result<()> {
    // Create runtime for async operations
    let runtime = Runtime::new()?;

    // Create test data
    let test_batches = vec![
        DataBatch::new(None, create_test_string_batch(vec!["test1".to_string()])?),
        DataBatch::new(None, create_test_string_batch(vec!["test2".to_string()])?),
        DataBatch::new(None, create_test_string_batch(vec!["test3".to_string()])?),
    ];

    println!("Creating storage actor...");
    // Create storage actor
    let storage_actor = InMemoryStorageActor::new();
    let storage_ref = runtime.block_on(async {
        spawn(storage_actor)
    });

    println!("Created storage actor");

    // Create execution graph
    let mut graph = ExecutionGraph::new();

    // Create source vertex
    let source_vertex = ExecutionVertex::new(
        "source".to_string(),
        OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(test_batches.clone())),
        1,
        0,
    );
    graph.add_vertex(source_vertex);

    // Create map vertex with identity function
    let map_vertex = ExecutionVertex::new(
        "map".to_string(),
        OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
        1,
        0,
    );
    graph.add_vertex(map_vertex);

    // Create sink vertex with storage actor
    let sink_vertex = ExecutionVertex::new(
        "sink".to_string(),
        OperatorConfig::SinkConfig(SinkConfig::InMemoryStorageActorSinkConfig(storage_ref.clone())),
        1,
        0,
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

    println!("Worker starting...");
    worker.start()?;
    println!("Worker started");

    // Wait for processing to complete
    thread::sleep(Duration::from_secs(1));

    // Verify results by reading from storage actor
    let result = runtime.block_on(async {
        storage_ref.ask(InMemoryStorageMessage::GetVector).await
    })?;
    
    match result {
        InMemoryStorageReply::Vector(result_batches) => {
            assert_eq!(result_batches.len(), test_batches.len());
            for (expected, actual) in test_batches.iter().zip(result_batches.iter()) {
                let expected_value = expected.record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0);
                let actual_value = actual.record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0);
                assert_eq!(actual_value, expected_value);
            }
        }
        _ => panic!("Expected Vector reply from storage actor"),
    }

    // Close worker
    worker.close()?;

    Ok(())
} 