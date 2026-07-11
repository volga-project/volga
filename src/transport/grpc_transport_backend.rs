use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;
use crate::common::message::Message;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::health::{WorkerFatalReason, WorkerHealth};
use crate::runtime::VertexId;
use crate::transport::batch_channel::{batch_bounded_channel, BatchReceiver, BatchSender};
use crate::transport::channel::Channel;
use crate::transport::grpc::grpc_streaming_service::{
    MessageStreamClient, MessageStreamServiceImpl,
    message_stream::message_stream_service_server::MessageStreamServiceServer,
};
use crate::transport::TransportBackend;
use async_trait::async_trait;
use tonic::transport::Server;
use std::sync::Arc;
use tokio::sync::oneshot;

use super::transport_client::TransportClientConfig;

type ChannelId = String;
type NodeId = String;
type NodeIp = String;
type TransportPort = i32;
type RemoteNodeAddr = (NodeId, NodeIp, TransportPort);

type ReaderReceivers = HashMap<ChannelId, BatchReceiver>;
type WriterSenders = HashMap<ChannelId, BatchSender>;

type RemoteReaderSenders = HashMap<ChannelId, BatchSender>;
type RemoteWriterReceivers = HashMap<ChannelId, BatchReceiver>;

type LocalChannels = HashMap<ChannelId, (BatchSender, Option<BatchReceiver>)>;

/// gRPC transport backend for workers that have at least one remote edge.
///
/// Topology model:
/// - Local channels are wired directly in `init_channels` (same as in-memory semantics):
///   source task gets a writer sender, target task gets a reader receiver.
/// - Remote channels are split into:
///   - `remote_reader_senders`: ingress queues for remote input channels
///   - `remote_writer_receivers`: egress queues for remote output channels
/// - Remote output channels are also indexed by destination node via `channel_to_node`,
///   and clients are created per destination node in `nodes`.
/// - This means remote traffic is multiplexed by `channel_id` over shared per-node client
///   streams/queues (not dedicated task-to-task sockets). Under skew or slow consumers,
///   this can create cross-channel backpressure / head-of-line effects on shared paths.
///
/// Runtime flow:
/// 1) `start_reading_side` optionally starts a gRPC server (if remote inputs exist) and
///    routes received `(message, channel_id)` items into `remote_reader_senders`.
/// 2) `start_writing_side` optionally starts per-node gRPC clients (if remote outputs exist)
///    and drains `remote_writer_receivers`, routing each channel to its destination node.
/// 3) `close` stops server/clients/tasks and joins all handles.
pub struct GrpcTransportBackend {
    server_handle: Option<tokio::task::JoinHandle<()>>,
    client_handles: Vec<tokio::task::JoinHandle<()>>,
    reader_task: Option<tokio::task::JoinHandle<()>>,
    writer_task: Option<tokio::task::JoinHandle<()>>,

    remote_reader_senders: Option<HashMap<ChannelId, BatchSender>>,
    remote_writer_receivers: Option<HashMap<ChannelId, BatchReceiver>>,

    nodes: Option<Vec<RemoteNodeAddr>>,
    channel_to_node: Option<HashMap<ChannelId, NodeId>>,
    port: Option<i32>,

    shutdown_tx: Option<oneshot::Sender<()>>,
    shutdown_rx: Option<oneshot::Receiver<()>>,
    running: Arc<AtomicBool>,
    worker_health: Arc<WorkerHealth>,
}

impl GrpcTransportBackend {
    const GRPC_SERVER_QUEUE_SIZE: usize = 10; // single mpsc channel reading from grpc server and forwarding to local clients (readers)
    const GRPC_CLIENT_QUEUE_SIZE: usize = 10; // mpsc channel per peer node, reading local client (writers) and forwarding to remote grpc clients

    pub fn new(worker_health: Arc<WorkerHealth>) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        Self {
            server_handle: None,
            client_handles: Vec::new(),
            reader_task: None,
            writer_task: None,
            remote_reader_senders: None,
            remote_writer_receivers: None,
            nodes: None,
            channel_to_node: None,
            port: None,
            shutdown_tx: Some(shutdown_tx),
            shutdown_rx: Some(shutdown_rx),
            running: Arc::new(AtomicBool::new(false)),
            worker_health,
        }
    }

    fn get_host_port_from_in_channels(&self, in_channels: &[Channel]) -> i32 {
        let mut ports = in_channels
            .iter()
            .map(|channel| match channel {
                Channel::Remote {
                    target_transport_port,
                    ..
                } => *target_transport_port,
                Channel::Local { .. } => panic!("Expected only remote channels"),
            })
            .collect::<Vec<_>>();

        ports.sort_unstable();
        ports.dedup();

        match ports.as_slice() {
            [port] => *port,
            [] => panic!("Expected at least one remote input channel"),
            _ => panic!("All ports should be the same, channels: {:?}", in_channels),
        }
    }

    fn get_remote_nodes_from_out_channels(
        &self,
        out_channels: &[Channel],
    ) -> (Vec<RemoteNodeAddr>, HashMap<ChannelId, NodeId>) {
        let mut nodes_by_id: HashMap<NodeId, (NodeIp, TransportPort)> = HashMap::new();
        let mut channel_to_node: HashMap<ChannelId, NodeId> = HashMap::new();

        for channel in out_channels {
            let (
                channel_id,
                target_node_id,
                target_node_ip,
                target_port,
            ) = match channel {
                Channel::Remote {
                    channel_id,
                    target_node_id,
                    target_node_ip,
                    target_transport_port,
                    ..
                } => (
                    channel_id.clone(),
                    target_node_id.clone(),
                    target_node_ip.clone(),
                    *target_transport_port,
                ),
                Channel::Local { .. } => panic!("Expected only remote channels"),
            };

            channel_to_node.insert(channel_id, target_node_id.clone());

            match nodes_by_id.entry(target_node_id.clone()) {
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert((target_node_ip, target_port));
                }
                std::collections::hash_map::Entry::Occupied(o) => {
                    let (existing_ip, _existing_port) = o.get();
                    if existing_ip != &target_node_ip {
                        panic!("Node id<->ip mismatch");
                    }
                }
            }
        }

        let node_list: Vec<RemoteNodeAddr> = nodes_by_id
            .into_iter()
            .map(|(node_id, (ip, port))| (node_id, ip, port))
            .collect();
        (node_list, channel_to_node)
    }

    async fn start_reading_side(
        &mut self,
        remote_reader_senders: HashMap<ChannelId, BatchSender>,
    ) {
        // Start reading side only when this worker has remote input channels.
        let Some(port) = self.port else {
            return;
        };

        let (server_tx, mut server_rx) = mpsc::channel(Self::GRPC_SERVER_QUEUE_SIZE);
        let shutdown_rx = self
            .shutdown_rx
            .take()
            .expect("[GRPC_BACKEND] missing shutdown receiver");
        let server_handle = tokio::spawn(async move {
            let addr = format!("0.0.0.0:{}", port)
                .parse()
                .expect("[GRPC_BACKEND] invalid listen address");
            let service = MessageStreamServiceImpl::new(server_tx);
            let svc = MessageStreamServiceServer::new(service);

            println!("[GRPC_BACKEND] Starting gRPC server on {}", addr);

            Server::builder()
                .add_service(svc)
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.await;
                    println!("[SERVER] Received shutdown signal");
                })
                .await
                .expect("[GRPC_BACKEND] gRPC server failed");
        });
        self.server_handle = Some(server_handle);

        // Route messages from gRPC server ingress to remote input channel queues.
        let running = self.running.clone();
        let worker_health = self.worker_health.clone();
        let reader_task = tokio::spawn(async move {
            while running.load(std::sync::atomic::Ordering::Relaxed) {
                match time::timeout(Duration::from_millis(100), server_rx.recv()).await {
                    Ok(Some((message, channel_id))) => {
                        // THIS WILL CAUSE BACKPRESSURE FOR ALL CHANNELS IF ONE CHANNEL IS BLOCKED
                        let sender = remote_reader_senders.get(&channel_id).unwrap();
                        while running.load(std::sync::atomic::Ordering::Relaxed) {
                            match time::timeout(Duration::from_millis(100), sender.send(message.clone()))
                                .await
                            {
                                Ok(Ok(())) => break,
                                Ok(Err(e)) => {
                                    worker_health.report_fatal(
                                        WorkerFatalReason::TransportDisconnect,
                                        format!(
                                            "[GRPC_BACKEND] Failed to forward message to channel {}: {}",
                                            channel_id, e
                                        ),
                                    );
                                    return;
                                }
                                Err(_) => continue,
                            }
                        }
                    }
                    Ok(None) => {
                        if running.load(std::sync::atomic::Ordering::Relaxed) {
                            worker_health.report_fatal(
                                WorkerFatalReason::TransportDisconnect,
                                "[GRPC_BACKEND] Server channel closed".to_string(),
                            );
                            return;
                        }
                    }
                    Err(_) => continue,
                }
            }
        });
        self.reader_task = Some(reader_task);
    }

    async fn start_writing_side(
        &mut self,
        remote_writer_receivers: HashMap<ChannelId, BatchReceiver>,
    ) {
        if remote_writer_receivers.is_empty() {
            return;
        }

        let channel_to_node: HashMap<ChannelId, NodeId> =
            self.channel_to_node.take().expect("Remote channel mapping should be found");
        let nodes = self.nodes.take().expect("Remote nodes should be found");
        let worker_health = self.worker_health.clone();
        let mut client_txs: HashMap<NodeId, mpsc::Sender<(Message, ChannelId)>> = HashMap::new();

        for (peer_node_id, peer_node_ip, target_port) in nodes {
            let server_addr = format!("http://{}:{}", peer_node_ip, target_port);
            let (client_tx, client_rx) = mpsc::channel(Self::GRPC_CLIENT_QUEUE_SIZE);
            client_txs.insert(peer_node_id.clone(), client_tx);
            let worker_health = worker_health.clone();
            let client_stream_task = tokio::spawn(async move {
                let mut client = match MessageStreamClient::connect(server_addr.clone()).await {
                    Ok(client) => client,
                    Err(e) => {
                        worker_health.report_fatal(
                            WorkerFatalReason::TransportDisconnect,
                            format!(
                                "[GRPC_BACKEND] failed to connect remote node {}: {}",
                                server_addr, e
                            ),
                        );
                        return;
                    }
                };
                if let Err(e) = client.stream_messages(client_rx).await {
                    worker_health.report_fatal(
                        WorkerFatalReason::TransportDisconnect,
                        format!("[GRPC_BACKEND] remote stream_messages failed: {}", e),
                    );
                }
            });
            self.client_handles.push(client_stream_task);
        }

        // Route remote output channel queues to per-node gRPC clients.
        let running = self.running.clone();
        let worker_health = self.worker_health.clone();
        let writer_task = tokio::spawn(async move {
            let mut receiver_futures = Vec::new();

            for (channel_id, mut receiver) in remote_writer_receivers {
                let channel_id_clone = channel_id.clone();
                let client_txs_clone = client_txs.clone();
                let channel_to_node_clone = channel_to_node.clone();
                let worker_health = worker_health.clone();

                let r = running.clone();
                let future = async move {
                    while r.load(std::sync::atomic::Ordering::Relaxed) {
                        match time::timeout(Duration::from_millis(100), receiver.recv()).await {
                            Ok(Some(message)) => {
                                let node_id = channel_to_node_clone
                                    .get(&channel_id_clone)
                                    .unwrap_or_else(|| {
                                        panic!(
                                            "[GRPC_BACKEND] Missing remote target mapping for channel {}",
                                            channel_id_clone
                                        )
                                    });
                                let client_tx = client_txs_clone.get(node_id).unwrap_or_else(|| {
                                    panic!(
                                        "[GRPC_BACKEND] Missing remote client for node {} (channel {})",
                                        node_id, channel_id_clone
                                    )
                                });
                                while r.load(std::sync::atomic::Ordering::Relaxed) {
                                    match time::timeout(
                                        Duration::from_millis(100),
                                        client_tx.send((message.clone(), channel_id_clone.clone())),
                                    )
                                    .await
                                    {
                                        Ok(Ok(())) => break,
                                        Ok(Err(e)) => {
                                            worker_health.report_fatal(
                                                WorkerFatalReason::TransportDisconnect,
                                                format!(
                                                    "[GRPC_BACKEND] Failed to send message to client {}: {}",
                                                    node_id, e
                                                ),
                                            );
                                            return;
                                        }
                                        Err(_) => continue,
                                    }
                                }
                            }
                            Ok(None) => break,
                            Err(_) => continue,
                        }
                    }
                };

                receiver_futures.push(Box::pin(future));
            }

            let _ = futures::future::join_all(receiver_futures).await;
        });
        self.writer_task = Some(writer_task);
    }

    fn derive_remote_connection_args(
        &self,
        input_channels: &[Channel],
        output_channels: &[Channel],
    ) -> (
        Option<TransportPort>,
        Option<HashMap<ChannelId, NodeId>>,
        Option<Vec<RemoteNodeAddr>>,
    ) {
        let remote_input_channels: Vec<Channel> = input_channels
            .iter()
            .filter_map(|channel| match channel {
                Channel::Remote { .. } => Some(channel.clone()),
                Channel::Local { .. } => None,
            })
            .collect();
        let port = if remote_input_channels.is_empty() {
            None
        } else {
            Some(self.get_host_port_from_in_channels(&remote_input_channels))
        };

        let remote_output_channels: Vec<Channel> = output_channels
            .iter()
            .filter_map(|channel| match channel {
                Channel::Remote { .. } => Some(channel.clone()),
                Channel::Local { .. } => None,
            })
            .collect();
        let (channel_to_node, remote_nodes) = if remote_output_channels.is_empty() {
            (None, None)
        } else {
            let (remote_nodes, channel_to_node) =
                self.get_remote_nodes_from_out_channels(&remote_output_channels);
            (Some(channel_to_node), Some(remote_nodes))
        };

        (port, channel_to_node, remote_nodes)
    }

    fn build_channel_maps(
        input_channels: &[Channel],
        output_channels: &[Channel],
    ) -> (
        ReaderReceivers,
        WriterSenders,
        RemoteReaderSenders,
        RemoteWriterReceivers,
    ) {
        let mut local_channels: LocalChannels = HashMap::new();
        let mut reader_receivers: ReaderReceivers = HashMap::new();
        let mut writer_senders: WriterSenders = HashMap::new();
        let mut remote_reader_senders: RemoteReaderSenders = HashMap::new();
        let mut remote_writer_receivers: RemoteWriterReceivers = HashMap::new();

        for channel in output_channels.iter().cloned() {
            let channel_id = channel.get_channel_id();
            let queue_size = channel.get_queue_size_records();
            match channel {
                Channel::Local { .. } => {
                    let (tx, _rx) = local_channels
                        .entry(channel_id.clone())
                        .or_insert_with(|| {
                            let (tx, rx) = batch_bounded_channel(queue_size);
                            (tx, Some(rx))
                        });
                    writer_senders.insert(channel_id, tx.clone());
                }
                Channel::Remote { .. } => {
                    let (tx, rx) = batch_bounded_channel(queue_size);
                    writer_senders.insert(channel_id.clone(), tx);
                    remote_writer_receivers.insert(channel_id, rx);
                }
            }
        }

        for channel in input_channels.iter().cloned() {
            let channel_id = channel.get_channel_id();
            let queue_size = channel.get_queue_size_records();
            match channel {
                Channel::Local { .. } => {
                    let (_tx, rx_opt) = local_channels
                        .entry(channel_id.clone())
                        .or_insert_with(|| {
                            let (tx, rx) = batch_bounded_channel(queue_size);
                            (tx, Some(rx))
                        });
                    let rx = rx_opt.take().expect("Local channel receiver should exist");
                    reader_receivers.insert(channel_id, rx);
                }
                Channel::Remote { .. } => {
                    let (tx, rx) = batch_bounded_channel(queue_size);
                    remote_reader_senders.insert(channel_id.clone(), tx);
                    reader_receivers.insert(channel_id, rx);
                }
            }
        }

        (
            reader_receivers,
            writer_senders,
            remote_reader_senders,
            remote_writer_receivers,
        )
    }

    fn build_transport_client_configs(
        execution_graph: &ExecutionGraph,
        vertex_ids: &[VertexId],
        mut reader_receivers: ReaderReceivers,
        writer_senders: &WriterSenders,
    ) -> HashMap<VertexId, TransportClientConfig> {
        let mut transport_client_configs: HashMap<VertexId, TransportClientConfig> = HashMap::new();

        for vertex_id in vertex_ids {
            let config = transport_client_configs
                .entry(vertex_id.clone())
                .or_insert(TransportClientConfig::new(vertex_id.clone()));

            let (input_edges, output_edges) = execution_graph
                .get_edges_for_vertex(vertex_id.as_ref())
                .expect("vertex should exist");

            for edge in input_edges {
                let channel_id = edge.get_channel().get_channel_id();
                config.add_reader_receiver(
                    channel_id.clone(),
                    reader_receivers
                        .remove(&channel_id)
                        .expect("reader receiver should exist"),
                );
            }

            for edge in output_edges {
                let channel_id = edge.get_channel().get_channel_id();
                config.add_writer_sender(
                    channel_id.clone(),
                    writer_senders
                        .get(&channel_id)
                        .expect("writer sender should exist")
                        .clone(),
                );
            }
        }

        transport_client_configs
    }
}

#[async_trait]
impl TransportBackend for GrpcTransportBackend {
    async fn start(&mut self) {
        self.running.store(true, std::sync::atomic::Ordering::Release);

        let remote_reader_senders = self.remote_reader_senders.take().unwrap();
        self.start_reading_side(remote_reader_senders).await;

        let remote_writer_receivers = self.remote_writer_receivers.take().unwrap();
        self.start_writing_side(remote_writer_receivers).await;
    }

    async fn close(&mut self) {
        self.running.store(false, std::sync::atomic::Ordering::Release);

        // Send shutdown signal to server
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        if let Some(server_handle) = self.server_handle.take() {
            let _ = server_handle.await;
        }

        for handle in self.client_handles.drain(..) {
            let _ = handle.await;
        }

        if let Some(reader_task) = self.reader_task.take() {
            let _ = reader_task.await;
        }

        if let Some(writer_task) = self.writer_task.take() {
            let _ = writer_task.await;
        }

        println!("[GRPC_BACKEND] All tasks completed");
    }

    fn init_channels(
        &mut self,
        execution_graph: &ExecutionGraph,
        vertex_ids: Vec<VertexId>,
    ) -> HashMap<VertexId, TransportClientConfig> {
        let mut input_channels = Vec::new();
        let mut output_channels = Vec::new();

        for vertex_id in &vertex_ids {
            let (input_edges, output_edges) = execution_graph
                .get_edges_for_vertex(vertex_id.as_ref())
                .expect("vertex should exist");
            input_channels.extend(input_edges.iter().map(|edge| edge.get_channel()));
            output_channels.extend(output_edges.iter().map(|edge| edge.get_channel()));
        }

        let (port, channel_to_node, nodes) =
            self.derive_remote_connection_args(&input_channels, &output_channels);
        self.port = port;
        self.channel_to_node = channel_to_node;
        self.nodes = nodes;
        let (
            reader_receivers,
            writer_senders,
            remote_reader_senders,
            remote_writer_receivers,
        ) = Self::build_channel_maps(&input_channels, &output_channels);
        let transport_client_configs = Self::build_transport_client_configs(
            execution_graph,
            &vertex_ids,
            reader_receivers,
            &writer_senders,
        );

        // Store the senders and receivers for the tasks
        self.remote_reader_senders = Some(remote_reader_senders);
        self.remote_writer_receivers = Some(remote_writer_receivers);

        transport_client_configs
    }
} 