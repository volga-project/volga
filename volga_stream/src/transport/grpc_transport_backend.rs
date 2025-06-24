use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;
use crate::common::message::Message;
use crate::runtime::execution_graph::ExecutionGraph;
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

pub struct GrpcTransportBackend {
    server_handle: Option<tokio::task::JoinHandle<()>>,
    client_handles: Vec<tokio::task::JoinHandle<()>>,
    reader_task: Option<tokio::task::JoinHandle<()>>,
    writer_task: Option<tokio::task::JoinHandle<()>>,

    reader_senders: Option<HashMap<String, mpsc::Sender<Message>>>,
    writer_receivers: Option<HashMap<String, mpsc::Receiver<Message>>>,

    nodes: Option<Vec<(String, String, i32)>>,
    channel_to_node: Option<HashMap<String, String>>,
    port: Option<i32>,

    shutdown_tx: Option<oneshot::Sender<()>>,
    shutdown_rx: Option<oneshot::Receiver<()>>,
    running: Arc<AtomicBool>,
}

impl GrpcTransportBackend {
    const CHANNEL_BUFFER_SIZE: usize = 100;

    pub fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        Self {
            server_handle: None,
            client_handles: Vec::new(),
            reader_task: None,
            writer_task: None,
            reader_senders: None,
            writer_receivers: None,
            nodes: None,
            channel_to_node: None,
            port: None,
            shutdown_tx: Some(shutdown_tx),
            shutdown_rx: Some(shutdown_rx),
            running: Arc::new(AtomicBool::new(false))
        }
    }

    fn get_host_port_from_in_channels(&self, in_channels: &[Channel]) -> i32 {
        if in_channels.len() == 0 {
            panic!("No channels provided");
        }

        let mut p: Option<i32> = None;
        for channel in in_channels {
            if let Channel::Remote { target_port: port, .. } = channel {
                if p.is_none() {
                    p = Some(*port);
                } else {
                    if p.unwrap() != *port {
                        panic!("All ports should be the same, channels: {:?}", in_channels);
                    }
                }
            } else {
                panic!("Exepected remote channels")
            }
        }
        if p.is_none() {
            panic!("All ports should be the same {:?}", in_channels)
        }

        return p.unwrap()
    }

    fn get_remote_nodes_from_out_channels(&self, out_channels: &[Channel]) -> (Vec<(String, String, i32)>, HashMap<String, String>) {
        if out_channels.len() == 0 {
            panic!("No channels provided");
        }
        let mut nodes = HashMap::new();
        let mut channel_to_node = HashMap::new();
        
        for channel in out_channels {
            if let Channel::Remote { target_node_ip, target_node_id, target_port, .. } = channel {
                let channel_id = channel.get_channel_id().clone();
                channel_to_node.insert(channel_id, target_node_id.clone());
                
                let ip_and_port = nodes.get(target_node_id);
                if ip_and_port.is_none() {
                    nodes.insert(target_node_id.clone(), (target_node_ip.clone(), *target_port));
                } else {
                    if ip_and_port.unwrap().0 != *target_node_ip {
                        panic!("Node id<->ip mismatch");
                    }
                }
            } else {
                panic!("Exepected remote channels")
            }
        }
        
        let node_list: Vec<(String, String, i32)> = nodes.into_iter()
            .map(|(node_id, (ip, port))| (node_id, ip, port))
            .collect();
        (node_list, channel_to_node)
    }
}

#[async_trait]
impl TransportBackend for GrpcTransportBackend {
    async fn start(&mut self) {
        self.running.store(true, std::sync::atomic::Ordering::Release);

        let reader_senders = self.reader_senders.take().unwrap();
        
        // Start reading side if needed
        if reader_senders.len() != 0{
            // Start server
            let port = self.port.expect("Port should be found");
            let (server_tx, mut server_rx) = mpsc::channel(Self::CHANNEL_BUFFER_SIZE);
            let shutdown_rx = self.shutdown_rx.take().unwrap();
            let server_handle = tokio::spawn(async move {
                let addr = format!("127.0.0.1:{}", port).parse().unwrap();
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
                    .unwrap();
            });
            self.server_handle = Some(server_handle);

            // Start reader task
            // routes data from server to reader channels
            let runnning = self.running.clone();
            let reader_task = tokio::spawn(async move {
                while runnning.load(std::sync::atomic::Ordering::Relaxed) {
                    match time::timeout(Duration::from_millis(100), server_rx.recv()).await {
                        Ok(Some((message, channel_id))) => {
                            // THIS WILL CAUSE BACKPRESSURE FOR ALL CHANNELS IF ONE CHANNEL IS BLOCKED
                            let sender = reader_senders.get(&channel_id).unwrap();
                            while runnning.load(std::sync::atomic::Ordering::Relaxed) {
                                match time::timeout(Duration::from_millis(100), sender.send(message.clone())).await {
                                    Ok(Ok(())) => {
                                        break;
                                    }, 
                                    Ok(Err(e)) => {
                                        panic!("[GRPC_BACKEND] Failed to forward message to channel {}: {}", channel_id, e);
                                    },
                                    Err(_) => {
                                        // timeout
                                        continue;
                                    }
                                }
                            }
                        },
                        Ok(None) => {
                            if runnning.load(std::sync::atomic::Ordering::Relaxed) {
                                panic!("[GRPC_BACKEND] Server channel closed");
                            }
                        },
                        Err(_) => {
                            // timeout
                            continue
                        },
                    }
                }
            });
            self.reader_task = Some(reader_task);
        }

        // Start writing side if needed
        let writer_receivers = self.writer_receivers.take().unwrap();
        
        if writer_receivers.len() != 0 {

            // Start clients for each peer node
            let channel_to_node: HashMap<String, String> = self.channel_to_node.take().expect("Nodes should be found");
            let nodes = self.nodes.take().expect("Nodes should be found");
            let mut client_txs: HashMap<String, mpsc::Sender<(Message, String)>> = HashMap::new();
            
            // println!("[GRPC_BACKEND] Setting up clients for nodes: {:?}", nodes);
            // println!("[GRPC_BACKEND] Channel to node mapping: {:?}", channel_to_node);
            
            for (peer_node_id, peer_node_ip, target_port) in nodes {
                let server_addr = format!("http://{}:{}", peer_node_ip, target_port);
                let (client_tx, client_rx) = mpsc::channel(Self::CHANNEL_BUFFER_SIZE);
                client_txs.insert(peer_node_id.clone(), client_tx);
                // println!("[GRPC_BACKEND] Created client for node_id: {} at {}", node_id, server_addr);
                let client_stream_task = tokio::spawn(async move {
                    let mut client = MessageStreamClient::connect(server_addr).await.unwrap();
                    client.stream_messages(client_rx).await.unwrap();
                });
                
                self.client_handles.push(client_stream_task);
            }

            let running = self.running.clone();
            
            // Start writer task
            // routes data from writer channels to clients based on peer node<->channel mapping
            let writer_task = tokio::spawn(async move {
                let mut receiver_futures = Vec::new();
                
                // Create futures for all writer receivers
                for (channel_id, mut receiver) in writer_receivers {
                    let channel_id_clone = channel_id.clone();
                    let client_txs_clone = client_txs.clone();
                    let channel_to_node_clone = channel_to_node.clone();
                    
                    let r = running.clone();
                    let future = async move {
                        
                        // THIS WILL CAUSE BACKPRESSURE FOR ALL CHANNELS IF ONE CHANNEL IS BLOCKED
                        while r.load(std::sync::atomic::Ordering::Relaxed) {
                            match time::timeout(Duration::from_millis(100), receiver.recv()).await {
                                Ok(Some(message)) => {
                                    let node_id = channel_to_node_clone.get(&channel_id_clone).unwrap();
                                    // println!("[GRPC_BACKEND] Looking up client for node_id: {} (channel: {})", node_id, channel_id_clone);
                                    // println!("[GRPC_BACKEND] Available client_txs keys: {:?}", client_txs_clone.keys().collect::<Vec<_>>());
                                    let client_tx = client_txs_clone.get(node_id).unwrap();
                                    while r.load(std::sync::atomic::Ordering::Relaxed) {
                                        match time::timeout(Duration::from_millis(100), client_tx.send((message.clone(), channel_id_clone.clone()))).await {
                                            Ok(Ok(())) => {
                                                break;
                                            }, 
                                            Ok(Err(e)) => {
                                                panic!("[GRPC_BACKEND] Failed to send message to client {}: {}", node_id, e);
                                            },
                                            Err(_) => {
                                                // timeout
                                                continue;
                                            }
                                        }
                                    }
                                },
                                Ok(None) => {
                                    if r.load(std::sync::atomic::Ordering::Relaxed) {
                                        panic!("[GRPC_BACKEND] Client channel closed");
                                    }
                                },
                                Err(_) => {
                                    // timeout
                                    continue
                                },
                            }
                        }
                    };
                    
                    receiver_futures.push(Box::pin(future));
                }
                
                let _ = futures::future::join_all(receiver_futures).await;
            });
            self.writer_task = Some(writer_task);
        }
    }

    async fn close(&mut self) {
        self.running.store(false, std::sync::atomic::Ordering::Release);

        // Send shutdown signal to server
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        // Wait for all tasks to complete
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

    fn init_channels(&mut self, execution_graph: &ExecutionGraph, vertex_ids: Vec<String>) -> HashMap<String, TransportClientConfig> {
        let mut all_input_edges = Vec::new();
        let mut all_output_edges = Vec::new();
        
        for vertex_id in &vertex_ids {
            let (input_edges, output_edges) = execution_graph.get_edges_for_vertex(vertex_id).unwrap();
            all_input_edges.extend(input_edges);
            all_output_edges.extend(output_edges);
        }

        let input_channels: Vec<Channel> = all_input_edges.iter().map(|edge| edge.channel.clone()).collect();
        let output_channels: Vec<Channel> = all_output_edges.iter().map(|edge| edge.channel.clone()).collect();

        if input_channels.len() != 0 {
            self.port = Some(self.get_host_port_from_in_channels(&input_channels));
        }

        if output_channels.len() != 0 {
            let (remote_nodes, channel_to_node) = self.get_remote_nodes_from_out_channels(&output_channels);
            self.channel_to_node = Some(channel_to_node);
            self.nodes = Some(remote_nodes);
        }

        let mut reader_receivers = HashMap::new();
        let mut reader_senders = HashMap::new();
        
        for edge in &all_input_edges {
            let channel_id = edge.channel.get_channel_id().clone();
            let (tx, rx) = mpsc::channel(Self::CHANNEL_BUFFER_SIZE);
            reader_senders.insert(channel_id.clone(), tx);
            reader_receivers.insert(channel_id.clone(), rx);
        }

        let mut writer_receivers = HashMap::new();
        let mut writer_senders = HashMap::new();

        for edge in &all_output_edges {
            let channel_id = edge.channel.get_channel_id().clone();
            let (tx, rx) = mpsc::channel(Self::CHANNEL_BUFFER_SIZE);
            writer_senders.insert(channel_id.clone(), tx);
            writer_receivers.insert(channel_id.clone(), rx);
        }

        let mut transport_client_configs: HashMap<String, TransportClientConfig> = HashMap::new();
        
        for vertex_id in vertex_ids {
            let config = transport_client_configs.entry(vertex_id.clone()).or_insert(TransportClientConfig::new(vertex_id.clone()));
            
            let (input_edges, output_edges) = execution_graph.get_edges_for_vertex(&vertex_id).unwrap();
            
            for edge in input_edges {
                let channel_id = edge.channel.get_channel_id().clone();
                config.add_reader_receiver(channel_id.clone(), reader_receivers.remove(&channel_id).unwrap());
            }

            for edge in output_edges {
                let channel_id = edge.channel.get_channel_id().clone();
                config.add_writer_sender(channel_id.clone(), writer_senders.get(&channel_id).unwrap().clone());
            }
        }

        // Store the senders and receivers for the tasks
        self.reader_senders = Some(reader_senders);
        self.writer_receivers = Some(writer_receivers);

        transport_client_configs
    }
} 