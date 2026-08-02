use crate::common::message::Message;
use crate::common::test_utils::{create_test_string_batch, gen_unique_grpc_port, IdentityMapFunction};
use crate::runtime::execution_graph::{gen_edge_id, ExecutionEdge, ExecutionGraph, ExecutionVertex};
use crate::runtime::functions::map::MapFunction;
use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::partition::PartitionType;
use crate::runtime::VertexId;
use crate::transport::channel::Channel;
use crate::transport::test_utils::{
    TestDataReaderActor, TestDataReaderMessage, TestDataWriterActor, TestDataWriterMessage,
};
use crate::transport::transport_backend_actor::{TransportBackendActor, TransportBackendActorMessage};
use crate::transport::TransportBackendTrait;
use arrow::array::StringArray;
use kameo::spawn;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[derive(Clone, Copy, Debug)]
pub enum MeshChannelMode {
    LocalOnly,
    RemoteOnly,
    MixedLocalRemote,
}

#[derive(Clone, Debug)]
pub struct MeshTestConfig {
    pub num_writer_nodes: usize,
    pub num_reader_nodes: usize,
    pub num_writers_per_node: usize,
    pub num_readers_per_node: usize,
    pub messages_per_writer: usize,
    pub mode: MeshChannelMode,
    pub startup_wait: Duration,
    pub processing_wait: Duration,
    pub read_timeout: Duration,
}

struct MeshGraph {
    graph: ExecutionGraph,
    writer_vertex_ids: Vec<String>,
    reader_vertex_ids: Vec<String>,
    vertex_node_ids: HashMap<String, usize>,
}

fn build_mesh_graph(config: &MeshTestConfig) -> MeshGraph {
    let mut graph = ExecutionGraph::new();
    let mut writer_vertex_ids = Vec::new();
    let mut reader_vertex_ids = Vec::new();
    let mut vertex_node_ids: HashMap<String, usize> = HashMap::new();
    let mut remote_port_by_target_node: HashMap<usize, i32> = HashMap::new();

    for node_idx in 0..config.num_writer_nodes {
        for writer_idx in 0..config.num_writers_per_node {
            let vertex_id = format!("writer_node_{}_writer_{}", node_idx, writer_idx);
            graph.add_vertex(ExecutionVertex::new(
                vertex_id.clone(),
                "writer_operator".to_string(),
                OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
                1,
                0,
            ));
            vertex_node_ids.insert(vertex_id.clone(), node_idx);
            writer_vertex_ids.push(vertex_id);
        }
    }

    for node_idx in 0..config.num_reader_nodes {
        for reader_idx in 0..config.num_readers_per_node {
            let vertex_id = format!("reader_node_{}_reader_{}", node_idx, reader_idx);
            graph.add_vertex(ExecutionVertex::new(
                vertex_id.clone(),
                "reader_operator".to_string(),
                OperatorConfig::MapConfig(MapFunction::new_custom(IdentityMapFunction)),
                1,
                0,
            ));
            let reader_node_id = match config.mode {
                // Keep reader node ids in a disjoint namespace to force purely remote paths.
                MeshChannelMode::RemoteOnly => config.num_writer_nodes + node_idx,
                MeshChannelMode::LocalOnly | MeshChannelMode::MixedLocalRemote => node_idx,
            };
            vertex_node_ids.insert(vertex_id.clone(), reader_node_id);
            reader_vertex_ids.push(vertex_id);
        }
    }

    for writer_vertex_id in &writer_vertex_ids {
        for reader_vertex_id in &reader_vertex_ids {
            let source_node_id = *vertex_node_ids
                .get(writer_vertex_id)
                .expect("writer node id should exist");
            let target_node_id = *vertex_node_ids
                .get(reader_vertex_id)
                .expect("reader node id should exist");

            let channel = match config.mode {
                MeshChannelMode::LocalOnly => {
                    Channel::new_local(writer_vertex_id.clone(), reader_vertex_id.clone())
                }
                MeshChannelMode::RemoteOnly => {
                    let target_port = *remote_port_by_target_node
                        .entry(target_node_id)
                        .or_insert_with(|| gen_unique_grpc_port() as i32);
                    Channel::new_remote(
                        writer_vertex_id.clone(),
                        reader_vertex_id.clone(),
                        "127.0.0.1".to_string(),
                        format!("node-{}", source_node_id),
                        "127.0.0.1".to_string(),
                        format!("node-{}", target_node_id),
                        target_port,
                    )
                }
                MeshChannelMode::MixedLocalRemote => {
                    if source_node_id == target_node_id {
                        Channel::new_local(writer_vertex_id.clone(), reader_vertex_id.clone())
                    } else {
                        let target_port = *remote_port_by_target_node
                            .entry(target_node_id)
                            .or_insert_with(|| gen_unique_grpc_port() as i32);
                        Channel::new_remote(
                            writer_vertex_id.clone(),
                            reader_vertex_id.clone(),
                            "127.0.0.1".to_string(),
                            format!("node-{}", source_node_id),
                            "127.0.0.1".to_string(),
                            format!("node-{}", target_node_id),
                            target_port,
                        )
                    }
                }
            };

            let edge = ExecutionEdge::new(
                writer_vertex_id.clone(),
                reader_vertex_id.clone(),
                reader_vertex_id.clone(),
                PartitionType::Forward,
                Some(channel),
            );
            graph.add_edge(edge);
        }
    }

    MeshGraph {
        graph,
        writer_vertex_ids,
        reader_vertex_ids,
        vertex_node_ids,
    }
}

pub async fn run_many_to_many_case<F>(config: MeshTestConfig, backend_factory: F)
where
    F: Fn() -> Box<dyn TransportBackendTrait>,
{
    let mesh = build_mesh_graph(&config);
    let writer_vertex_set: HashSet<String> = mesh.writer_vertex_ids.iter().cloned().collect();

    let mut node_to_vertex_ids: HashMap<usize, Vec<VertexId>> = HashMap::new();
    for (vertex_id, node_id) in &mesh.vertex_node_ids {
        node_to_vertex_ids
            .entry(*node_id)
            .or_default()
            .push(Arc::<str>::from(vertex_id.as_str()));
    }

    let mut node_ids = node_to_vertex_ids.keys().copied().collect::<Vec<_>>();
    node_ids.sort_unstable();

    let mut backend_refs = Vec::new();
    let mut writer_refs = HashMap::new();
    let mut reader_refs = HashMap::new();

    for node_id in node_ids {
        let mut backend = backend_factory();
        let vertex_ids = node_to_vertex_ids
            .remove(&node_id)
            .expect("node vertex ids should exist");
        let configs = backend.init_channels(&mesh.graph, vertex_ids);

        let backend_ref = spawn(TransportBackendActor::new(backend));
        backend_refs.push(backend_ref);

        for (vertex_id, transport_cfg) in configs {
            let vertex_key = vertex_id.as_ref().to_string();
            if writer_vertex_set.contains(&vertex_key) {
                let writer_actor = TestDataWriterActor::new(vertex_id.clone(), transport_cfg);
                writer_refs.insert(vertex_key, spawn(writer_actor));
            } else {
                let reader_actor = TestDataReaderActor::new(vertex_id.clone(), transport_cfg);
                reader_refs.insert(vertex_key, spawn(reader_actor));
            }
        }
    }

    for backend_ref in &backend_refs {
        backend_ref
            .ask(TransportBackendActorMessage::Start)
            .await
            .expect("backend start should succeed");
    }

    sleep(config.startup_wait).await;

    for writer_idx in 0..mesh.writer_vertex_ids.len() {
        let writer_vertex_id = &mesh.writer_vertex_ids[writer_idx];
        let writer_ref = writer_refs
            .get(writer_vertex_id)
            .expect("writer actor should exist");
        writer_ref
            .ask(TestDataWriterMessage::Start)
            .await
            .expect("writer start should succeed");

        for message_idx in 0..config.messages_per_writer {
            let message = Message::new(
                Some(format!("writer_{}_stream", writer_idx)),
                create_test_string_batch(vec![format!("writer_{}_batch_{}", writer_idx, message_idx)]),
                Some(100),
                None,
            );
            for reader_vertex_id in &mesh.reader_vertex_ids {
                let edge_id = gen_edge_id(writer_vertex_id, reader_vertex_id);
                let channel = mesh
                    .graph
                    .get_edge(&edge_id)
                    .expect("edge should exist")
                    .get_channel();
                writer_ref
                    .ask(TestDataWriterMessage::WriteMessage {
                        channel,
                        message: message.clone(),
                    })
                    .await
                    .expect("writer send should succeed");
            }
        }

        writer_ref
            .ask(TestDataWriterMessage::FlushAndClose)
            .await
            .expect("writer flush should succeed");
    }

    sleep(config.processing_wait).await;

    let expected_per_reader = mesh.writer_vertex_ids.len() * config.messages_per_writer;
    for reader_idx in 0..mesh.reader_vertex_ids.len() {
        let reader_vertex_id = &mesh.reader_vertex_ids[reader_idx];
        let reader_ref = reader_refs
            .get(reader_vertex_id)
            .expect("reader actor should exist");

        let mut received_messages = Vec::new();
        let mut received_map: HashMap<(usize, usize), String> = HashMap::new();
        for _ in 0..expected_per_reader {
            let result = tokio::time::timeout(
                config.read_timeout,
                reader_ref.ask(TestDataReaderMessage::ReadMessage),
            )
            .await
            .unwrap_or_else(|_| {
                let mut missing = Vec::new();
                for writer_idx in 0..mesh.writer_vertex_ids.len() {
                    for message_idx in 0..config.messages_per_writer {
                        if !received_map.contains_key(&(writer_idx, message_idx)) {
                            missing.push(format!("writer_{}_batch_{}", writer_idx, message_idx));
                        }
                    }
                }
                let preview = missing.into_iter().take(12).collect::<Vec<_>>().join(", ");
                panic!(
                    "Timed out waiting for reader {} message: received {}/{}; missing sample: [{}]",
                    reader_idx,
                    received_messages.len(),
                    expected_per_reader,
                    preview
                );
            })
            .expect("reader receive should succeed");
            if let Some(message) = result {
                let value = message
                    .record_batch()
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("value column should be Utf8")
                    .value(0)
                    .to_string();
                let parts: Vec<&str> = value.split("_batch_").collect();
                let writer_part = parts[0].strip_prefix("writer_").unwrap();
                let parsed_writer_idx = writer_part.parse::<usize>().unwrap();
                let parsed_message_idx = parts[1].parse::<usize>().unwrap();
                received_map.insert((parsed_writer_idx, parsed_message_idx), value);
                received_messages.push(message);
            }
        }

        assert_eq!(
            received_messages.len(),
            expected_per_reader,
            "Reader {} did not receive all expected batches",
            reader_idx
        );

        for writer_idx in 0..mesh.writer_vertex_ids.len() {
            for message_idx in 0..config.messages_per_writer {
                let expected = format!("writer_{}_batch_{}", writer_idx, message_idx);
                let actual = received_map
                    .get(&(writer_idx, message_idx))
                    .unwrap_or_else(|| {
                        panic!(
                            "Missing message writer_{}_batch_{} for reader {}",
                            writer_idx, message_idx, reader_idx
                        )
                    });
                assert_eq!(actual, &expected);
            }
        }
    }

    let close_futures = backend_refs.iter().map(|backend_ref| {
        tokio::time::timeout(
            Duration::from_secs(10),
            backend_ref.ask(TransportBackendActorMessage::Close),
        )
    });
    let close_results = futures::future::join_all(close_futures).await;
    for close_result in close_results {
        close_result
            .expect("backend close timed out")
            .expect("backend close should succeed");
    }
}
