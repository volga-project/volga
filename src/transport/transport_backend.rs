use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::health::WorkerHealth;
use crate::runtime::metrics::MetricsLabels;
use crate::runtime::VertexId;
use crate::transport::batch_channel::{batch_bounded_channel, BatchReceiver, BatchSender};
use crate::transport::channel::Channel;
use crate::transport::tcp::{self, EdgeIdentity, RemoteEndpoint};
use crate::transport::TransportBackendTrait;

use super::transport_client::TransportClientConfig;

type ChannelId = String;

type ReaderReceivers = HashMap<ChannelId, BatchReceiver>;
type WriterSenders = HashMap<ChannelId, BatchSender>;

type RemoteReaderSenders = HashMap<ChannelId, (EdgeIdentity, BatchSender)>;
type RemoteWriterReceivers = HashMap<ChannelId, (EdgeIdentity, RemoteEndpoint, BatchReceiver)>;

type LocalChannels = HashMap<ChannelId, (BatchSender, Option<BatchReceiver>)>;

/// Unified transport backend:
/// - local channels are in-proc `BatchChannel` wiring
/// - remote channels: one TCP connection per logical edge, drained by a per-edge pump
///
/// Flow control is the 8192-record queue plus the TCP window. No credit protocol.
pub struct TransportBackend {
    server_handle: Option<JoinHandle<()>>,
    pump_handles: Vec<JoinHandle<()>>,

    remote_reader_senders: Option<RemoteReaderSenders>,
    remote_writer_receivers: Option<RemoteWriterReceivers>,

    port: Option<i32>,
    has_remote_channels: bool,

    shutdown_tx: Option<oneshot::Sender<()>>,
    shutdown_rx: Option<oneshot::Receiver<()>>,
    running: Arc<AtomicBool>,
    worker_health: Arc<WorkerHealth>,
    metrics_labels: Option<MetricsLabels>,
}

impl TransportBackend {
    pub fn new(worker_health: Arc<WorkerHealth>) -> Self {
        Self::new_with_labels(worker_health, None)
    }

    pub fn new_with_labels(
        worker_health: Arc<WorkerHealth>,
        metrics_labels: Option<MetricsLabels>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        Self {
            server_handle: None,
            pump_handles: Vec::new(),
            remote_reader_senders: None,
            remote_writer_receivers: None,
            port: None,
            has_remote_channels: false,
            shutdown_tx: Some(shutdown_tx),
            shutdown_rx: Some(shutdown_rx),
            running: Arc::new(AtomicBool::new(false)),
            worker_health,
            metrics_labels,
        }
    }

    fn listen_port_from_in_channels(in_channels: &[Channel]) -> i32 {
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

    fn edge_identity(channel: &Channel) -> EdgeIdentity {
        EdgeIdentity {
            channel_id: channel.get_channel_id(),
            task_id: channel.get_source_vertex_id(),
            target_task_id: channel.get_target_vertex_id(),
        }
    }

    fn remote_endpoint(channel: &Channel) -> RemoteEndpoint {
        match channel {
            Channel::Remote {
                target_node_ip,
                target_transport_port,
                ..
            } => RemoteEndpoint {
                host: target_node_ip.clone(),
                port: u16::try_from(*target_transport_port).unwrap_or_else(|_| {
                    panic!("invalid transport port {target_transport_port}")
                }),
            },
            Channel::Local { .. } => panic!("Expected remote channel"),
        }
    }

    fn derive_listen_port(input_channels: &[Channel]) -> Option<i32> {
        let remote_input_channels: Vec<Channel> = input_channels
            .iter()
            .filter(|channel| matches!(channel, Channel::Remote { .. }))
            .cloned()
            .collect();
        if remote_input_channels.is_empty() {
            None
        } else {
            Some(Self::listen_port_from_in_channels(&remote_input_channels))
        }
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
            match &channel {
                Channel::Local { .. } => {
                    let (tx, _rx) = local_channels.entry(channel_id.clone()).or_insert_with(|| {
                        let (tx, rx) = batch_bounded_channel(queue_size);
                        (tx, Some(rx))
                    });
                    writer_senders.insert(channel_id, tx.clone());
                }
                Channel::Remote { .. } => {
                    let (tx, rx) = batch_bounded_channel(queue_size);
                    writer_senders.insert(channel_id.clone(), tx);
                    remote_writer_receivers.insert(
                        channel_id,
                        (
                            Self::edge_identity(&channel),
                            Self::remote_endpoint(&channel),
                            rx,
                        ),
                    );
                }
            }
        }

        for channel in input_channels.iter().cloned() {
            let channel_id = channel.get_channel_id();
            let queue_size = channel.get_queue_size_records();
            match &channel {
                Channel::Local { .. } => {
                    let (_tx, rx_opt) =
                        local_channels.entry(channel_id.clone()).or_insert_with(|| {
                            let (tx, rx) = batch_bounded_channel(queue_size);
                            (tx, Some(rx))
                        });
                    let rx = rx_opt.take().expect("Local channel receiver should exist");
                    reader_receivers.insert(channel_id, rx);
                }
                Channel::Remote { .. } => {
                    let (tx, rx) = batch_bounded_channel(queue_size);
                    remote_reader_senders
                        .insert(channel_id.clone(), (Self::edge_identity(&channel), tx));
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
impl TransportBackendTrait for TransportBackend {
    async fn start(&mut self) {
        self.running
            .store(true, std::sync::atomic::Ordering::Release);

        if !self.has_remote_channels {
            return;
        }

        if let Some(port) = self.port {
            let listener = match tcp::bind_ingress(port).await {
                Ok(listener) => listener,
                Err(e) => {
                    self.worker_health.report_fatal(
                        crate::runtime::health::WorkerFatalReason::TransportDisconnect,
                        format!("[TCP] bind port {port} failed: {e}"),
                    );
                    return;
                }
            };
            let senders = Arc::new(std::sync::Mutex::new(
                self.remote_reader_senders.take().unwrap(),
            ));
            let shutdown_rx = self
                .shutdown_rx
                .take()
                .expect("[TCP] missing shutdown receiver");
            let worker_health = self.worker_health.clone();
            let running = self.running.clone();
            let labels = self.metrics_labels.clone();
            self.server_handle = Some(tokio::spawn(async move {
                tcp::listen_ingress(
                    listener,
                    senders,
                    worker_health,
                    running,
                    labels,
                    shutdown_rx,
                )
                .await;
            }));
        }

        if let Some(remote_writer_receivers) = self.remote_writer_receivers.take() {
            for (_channel_id, (identity, endpoint, rx)) in remote_writer_receivers {
                let worker_health = self.worker_health.clone();
                let running = self.running.clone();
                let labels = self.metrics_labels.clone();
                self.pump_handles.push(tokio::spawn(async move {
                    tcp::pump_egress(rx, endpoint, identity, worker_health, running, labels).await;
                }));
            }
        }
    }

    async fn close(&mut self) {
        self.running
            .store(false, std::sync::atomic::Ordering::Release);

        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        if let Some(server_handle) = self.server_handle.take() {
            server_handle.abort();
            let _ = server_handle.await;
        }

        for handle in self.pump_handles.drain(..) {
            handle.abort();
            let _ = handle.await;
        }

        println!("[TCP] All tasks completed");
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

        let port = Self::derive_listen_port(&input_channels);
        let has_remote_out = output_channels
            .iter()
            .any(|channel| matches!(channel, Channel::Remote { .. }));
        self.has_remote_channels = port.is_some() || has_remote_out;
        self.port = port;

        let (reader_receivers, writer_senders, remote_reader_senders, remote_writer_receivers) =
            Self::build_channel_maps(&input_channels, &output_channels);
        let transport_client_configs = Self::build_transport_client_configs(
            execution_graph,
            &vertex_ids,
            reader_receivers,
            &writer_senders,
        );

        self.remote_reader_senders = Some(remote_reader_senders);
        self.remote_writer_receivers = Some(remote_writer_receivers);

        transport_client_configs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::channel::Channel;

    #[test]
    fn remote_endpoint_keeps_compose_hostname() {
        let channel = Channel::new_remote(
            "src".into(),
            "dst".into(),
            "worker-0".into(),
            "worker-0".into(),
            "worker-1".into(),
            "worker-1".into(),
            60052,
        );
        let endpoint = TransportBackend::remote_endpoint(&channel);
        assert_eq!(endpoint.host, "worker-1");
        assert_eq!(endpoint.port, 60052);
        assert!(
            format!("{endpoint}")
                .parse::<std::net::SocketAddr>()
                .is_err(),
            "compose DNS names are not SocketAddr"
        );
    }
}
