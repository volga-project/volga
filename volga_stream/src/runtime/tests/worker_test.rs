use crate::runtime::{
    execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionVertex, OperatorConfig, SourceConfig, SinkConfig}, 
    operator::SourceOperator, 
    partition::{ForwardPartition, PartitionType}, 
    worker::Worker,
    functions::{
        map::MapFunction,
        map::MapFunctionTrait,
    },
    storage::in_memory_storage_actor::{InMemoryStorageActor, InMemoryStorageMessage, InMemoryStorageReply},
};
use crate::common::message::Message;
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
    fn map(&self, message: Message) -> Result<Message> {
        Ok(message)
    }
}

// TODO test early worker interruption/shutdown via setting state to finished from outside
#[test]
fn test_worker() -> Result<()> {
    let runtime = Runtime::new()?;

    // TODO use max watermarks for shutdown
    let test_messages = vec![
        Message::new(None, create_test_string_batch(vec!["test1".to_string()]), None),
        Message::new(None, create_test_string_batch(vec!["test2".to_string()]), None),
        Message::new(None, create_test_string_batch(vec!["test3".to_string()]), None),
    ];

    println!("Creating storage actor...");
    let storage_actor = InMemoryStorageActor::new();
    let storage_ref = runtime.block_on(async {
        spawn(storage_actor)
    });

    println!("Created storage actor");

    let mut graph = ExecutionGraph::new();

    // Create source vertex
    let source_vertex = ExecutionVertex::new(
        "source".to_string(),
        OperatorConfig::SourceConfig(SourceConfig::VectorSourceConfig(test_messages.clone())),
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
        "map".to_string(),
        PartitionType::Forward,
        Channel::Local {
            channel_id: "source_to_map".to_string(),
        },
    );
    graph.add_edge(source_to_map);

    let map_to_sink = ExecutionEdge::new(
        "map".to_string(),
        "sink".to_string(),
        "sink".to_string(),
        PartitionType::Forward,
        Channel::Local {
            channel_id: "map_to_sink".to_string(),
        },
    );
    graph.add_edge(map_to_sink);

    let mut worker = Worker::new(
        graph,
        vec!["source".to_string(), "map".to_string(), "sink".to_string()],
        1,
    );

    runtime.block_on(async {
        worker.execute_worker_lifecycle_for_testing().await;
    });

    // Verify results by reading from storage actor
    let result = runtime.block_on(async {
        storage_ref.ask(InMemoryStorageMessage::GetVector).await
    })?;
    
    match result {
        InMemoryStorageReply::Vector(result_messages) => {
            assert_eq!(result_messages.len(), test_messages.len());
            for (expected, actual) in test_messages.iter().zip(result_messages.iter()) {
                let expected_value = expected.record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0);
                let actual_value = actual.record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0);
                assert_eq!(actual_value, expected_value);
            }
        }
        _ => panic!("Expected Vector reply from storage actor"),
    }

    Ok(())
} 