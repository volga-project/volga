use crate::common::message::Message;
use crate::common::ports::gen_unique_grpc_port;
use crate::runtime::execution_graph::{
    gen_edge_id, ExecutionEdge, ExecutionGraph, ExecutionVertex,
};
use crate::runtime::functions::map::MapFunction;
use crate::runtime::health::WorkerHealth;
use crate::runtime::operators::operator::{MessageStream, OperatorConfig};
use crate::runtime::partition::PartitionType;
use crate::runtime::VertexId;
use crate::test_utils::common::{create_test_string_batch, IdentityMapFunction};
use crate::transport::channel::Channel;
use crate::transport::transport_backend_actor::{
    TransportBackendActor, TransportBackendActorMessage,
};
use crate::transport::transport_client::{DataReader, DataWriter};
use crate::transport::transport_spec::TransportSpec;
use crate::transport::TransportBackendTrait;
use arrow::array::StringArray;
use futures::StreamExt;
use kameo::spawn;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::task::JoinHandle;
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
    /// `None` → default 8192. Isolation cases use a small bound (e.g. 2).
    pub queue_size_records: Option<u32>,
    /// Reader indices whose streams are never polled. Other readers must still finish.
    pub paused_reader_indices: Vec<usize>,
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
    let queue = config
        .queue_size_records
        .unwrap_or(TransportSpec::DEFAULT_QUEUE_RECORDS);

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
                MeshChannelMode::LocalOnly => Channel::new_local_with_queue(
                    writer_vertex_id.clone(),
                    reader_vertex_id.clone(),
                    queue,
                ),
                MeshChannelMode::RemoteOnly => {
                    let target_port = *remote_port_by_target_node
                        .entry(target_node_id)
                        .or_insert_with(|| gen_unique_grpc_port() as i32);
                    Channel::new_remote_with_queue(
                        writer_vertex_id.clone(),
                        reader_vertex_id.clone(),
                        "127.0.0.1".to_string(),
                        format!("node-{}", source_node_id),
                        "127.0.0.1".to_string(),
                        format!("node-{}", target_node_id),
                        target_port,
                        queue,
                    )
                }
                MeshChannelMode::MixedLocalRemote => {
                    if source_node_id == target_node_id {
                        Channel::new_local_with_queue(
                            writer_vertex_id.clone(),
                            reader_vertex_id.clone(),
                            queue,
                        )
                    } else {
                        let target_port = *remote_port_by_target_node
                            .entry(target_node_id)
                            .or_insert_with(|| gen_unique_grpc_port() as i32);
                        Channel::new_remote_with_queue(
                            writer_vertex_id.clone(),
                            reader_vertex_id.clone(),
                            "127.0.0.1".to_string(),
                            format!("node-{}", source_node_id),
                            "127.0.0.1".to_string(),
                            format!("node-{}", target_node_id),
                            target_port,
                            queue,
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

fn batch_payload(writer_idx: usize, message_idx: usize) -> String {
    format!("writer_{}_batch_{}", writer_idx, message_idx)
}

fn parse_batch_payload(value: &str) -> (usize, usize) {
    let parts: Vec<&str> = value.split("_batch_").collect();
    let writer_part = parts[0]
        .strip_prefix("writer_")
        .expect("payload should start with writer_");
    let writer_idx = writer_part.parse::<usize>().expect("writer idx");
    let message_idx = parts[1].parse::<usize>().expect("message idx");
    (writer_idx, message_idx)
}

fn payload_string(message: &Message) -> String {
    message
        .record_batch()
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("value column should be Utf8")
        .value(0)
        .to_string()
}

fn assert_ordered_delivery(
    reader_idx: usize,
    received: &[(usize, usize)],
    num_writers: usize,
    messages_per_writer: usize,
) {
    let mut per_writer: HashMap<usize, Vec<usize>> = HashMap::new();
    for &(writer_idx, message_idx) in received {
        per_writer.entry(writer_idx).or_default().push(message_idx);
    }
    for writer_idx in 0..num_writers {
        let got = per_writer.get(&writer_idx).cloned().unwrap_or_default();
        let expected: Vec<usize> = (0..messages_per_writer).collect();
        assert_eq!(
            got, expected,
            "Reader {reader_idx} writer {writer_idx} delivery order mismatch"
        );
    }
}

async fn drain_reader(
    reader_idx: usize,
    mut stream: MessageStream,
    expected: usize,
    read_timeout: Duration,
    num_writers: usize,
    messages_per_writer: usize,
) -> Vec<(usize, usize)> {
    let mut received = Vec::with_capacity(expected);
    let mut received_map: HashMap<(usize, usize), String> = HashMap::new();
    for _ in 0..expected {
        let message = match tokio::time::timeout(read_timeout, stream.next()).await {
            Ok(Some(message)) => message,
            Ok(None) => panic!(
                "Reader {reader_idx} stream closed after {}/{expected} messages",
                received.len()
            ),
            Err(_) => {
                let mut missing = Vec::new();
                for writer_idx in 0..num_writers {
                    for message_idx in 0..messages_per_writer {
                        if !received_map.contains_key(&(writer_idx, message_idx)) {
                            missing.push(batch_payload(writer_idx, message_idx));
                        }
                    }
                }
                let preview = missing.into_iter().take(12).collect::<Vec<_>>().join(", ");
                panic!(
                    "Timed out waiting for reader {} message: received {}/{}; missing sample: [{}]",
                    reader_idx,
                    received.len(),
                    expected,
                    preview
                );
            }
        };
        let value = payload_string(&message);
        let parsed = parse_batch_payload(&value);
        received_map.insert(parsed, value);
        received.push(parsed);
    }
    received
}

/// Independent per-edge send loops (cloned `DataWriter`).
/// One paused dest must not prevent other dests from receiving the full stream.
pub async fn run_many_to_many_case<F>(config: MeshTestConfig, backend_factory: F)
where
    F: Fn() -> Box<dyn TransportBackendTrait>,
{
    let mesh = build_mesh_graph(&config);
    let writer_vertex_set: HashSet<String> = mesh.writer_vertex_ids.iter().cloned().collect();
    let paused: HashSet<usize> = config.paused_reader_indices.iter().copied().collect();

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
    let mut writers: HashMap<String, DataWriter> = HashMap::new();
    let mut reader_streams: HashMap<String, MessageStream> = HashMap::new();

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
                let writer = DataWriter::new(
                    vertex_id.clone(),
                    transport_cfg.writer_senders.expect("writer senders"),
                    transport_cfg.metrics_labels,
                    Arc::new(WorkerHealth::new()),
                );
                writers.insert(vertex_key, writer);
            } else {
                let reader = DataReader::new(
                    vertex_id.clone(),
                    transport_cfg.reader_receivers.expect("reader receivers"),
                );
                let (stream, _control) = reader.message_stream_with_control();
                reader_streams.insert(vertex_key, stream);
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

    let expected_per_reader = mesh.writer_vertex_ids.len() * config.messages_per_writer;
    let num_writers = mesh.writer_vertex_ids.len();
    let messages_per_writer = config.messages_per_writer;

    let mut drain_handles: Vec<(usize, JoinHandle<Vec<(usize, usize)>>)> = Vec::new();
    // Keep paused streams alive: dropping them closes BatchReceiver and paused send returns false.
    let mut _held_streams: Vec<MessageStream> = Vec::new();
    for (reader_idx, reader_vertex_id) in mesh.reader_vertex_ids.iter().enumerate() {
        let stream = reader_streams
            .remove(reader_vertex_id)
            .expect("reader stream should exist");
        if paused.contains(&reader_idx) {
            _held_streams.push(stream);
            continue;
        }
        let read_timeout = config.read_timeout;
        drain_handles.push((
            reader_idx,
            tokio::spawn(async move {
                drain_reader(
                    reader_idx,
                    stream,
                    expected_per_reader,
                    read_timeout,
                    num_writers,
                    messages_per_writer,
                )
                .await
            }),
        ));
    }

    let mut live_sends: Vec<JoinHandle<()>> = Vec::new();
    let mut paused_sends: Vec<JoinHandle<()>> = Vec::new();
    for (writer_idx, writer_vertex_id) in mesh.writer_vertex_ids.iter().enumerate() {
        let writer = writers
            .get(writer_vertex_id)
            .expect("writer should exist")
            .clone();
        for (reader_idx, reader_vertex_id) in mesh.reader_vertex_ids.iter().enumerate() {
            let edge_id = gen_edge_id(writer_vertex_id, reader_vertex_id);
            let channel = mesh
                .graph
                .get_edge(&edge_id)
                .expect("edge should exist")
                .get_channel();
            let mut edge_writer = writer.clone();
            let handle = tokio::spawn(async move {
                for message_idx in 0..messages_per_writer {
                    let message = Message::new(
                        Some(format!("writer_{}_stream", writer_idx)),
                        create_test_string_batch(vec![batch_payload(writer_idx, message_idx)]),
                        Some(100),
                        None,
                    );
                    let success = edge_writer.write_message(&channel, &message).await;
                    assert!(
                        success,
                        "writer {writer_idx} → reader {reader_idx} send failed at batch {message_idx}"
                    );
                }
            });
            if paused.contains(&reader_idx) {
                paused_sends.push(handle);
            } else {
                live_sends.push(handle);
            }
        }
    }

    // Paused-edge sends are allowed to block; they must not return false (assert
    // in the task) and must not stall other edges. Abort them after live traffic
    // so the test can close without waiting on the paused dest.
    let send_timeout = config.read_timeout;
    tokio::time::timeout(send_timeout, async {
        for handle in live_sends {
            handle.await.expect("live send task panicked");
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "live sends stalled after {send_timeout:?}; a paused dest must not block other edges"
        )
    });

    for (reader_idx, handle) in drain_handles {
        let received = handle.await.expect("drain task panicked");
        assert_eq!(
            received.len(),
            expected_per_reader,
            "Reader {reader_idx} did not receive all expected batches"
        );
        assert_ordered_delivery(reader_idx, &received, num_writers, messages_per_writer);
    }

    for handle in paused_sends {
        handle.abort();
        match handle.await {
            Err(e) if e.is_cancelled() => {}
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Ok(()) => panic!("paused dest send completed; queue bound should have blocked"),
            Err(e) => panic!("{e}"),
        }
    }

    sleep(config.processing_wait).await;

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
