use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use tokio::sync::mpsc;
use crate::common::message::Message;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::transport::channel::Channel;
use crate::transport::grpc::grpc_streaming_service::{
    MessageStreamClient, MessageStreamServiceImpl,
    message_stream::message_stream_service_server::MessageStreamServiceServer,
};
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

    nodes: Option<Vec<(String, String)>>,
    channel_to_node: Option<HashMap<String, String>>,
    port: Option<i32>,

    shutdown_tx: Option<oneshot::Sender<()>>,
    running: Arc<AtomicBool>,
}

impl GrpcTransportBackend {
    const CHANNEL_BUFFER_SIZE: usize = 100;

    pub fn new() -> Self {
        Self {
            server_handle: None,
            client_handles: Vec::new(),
            reader_task: None,
            writer_task: None,
            // writer_senders: None,
            reader_senders: None,
            writer_receivers: None,
            // reader_receivers: None,
            nodes: None,
            channel_to_node: None,
            port: None,
            shutdown_tx: None,
            running: Arc::new(AtomicBool::new(false))
        }
    }

    fn get_port_from_remote_channels(&self, channels: &[Channel]) -> i32 {
        let mut p: Option<i32> = None;
        for channel in channels {
            if let Channel::Remote { port, .. } = channel {
                if p.is_none() {
                    p = Some(*port);
                } else {
                    if p.unwrap() != *port {
                        panic!("All ports should be the same");
                    }
                }
            } else {
                panic!("Exepected remote channels")
            }
        }
        if p.is_none() {
            panic!("All ports should be the same")
        }

        return p.unwrap()
    }

    fn get_remote_nodes_from_channels(&self, channels: &[Channel]) -> (Vec<(String, String)>, HashMap<String, String>) {
        let mut nodes = HashMap::new();
        let mut channel_to_node = HashMap::new();
        
        for channel in channels {
            if let Channel::Remote { target_node_ip, target_node_id, .. } = channel {
                let channel_id = channel.get_channel_id().clone();
                channel_to_node.insert(channel_id, target_node_id.clone());
                
                let ip = nodes.get(target_node_id);
                if ip.is_none() {
                    nodes.insert(target_node_id.clone(), target_node_ip.clone());
                } else {
                    if ip.unwrap() != target_node_ip {
                        panic!("Node id<->ip mismatch");
                    }
                }
            } else {
                panic!("Exepected remote channels")
            }
        }
        
        let node_list: Vec<(String, String)> = nodes.into_iter().collect();
        (node_list, channel_to_node)
    }
}

#[async_trait]
impl super::transport_backend::TransportBackend for GrpcTransportBackend {
    async fn start(&mut self) {
        // Start server
        // let (port, server_tx) = self.server_config.take().unwrap();
        let port = self.port.unwrap();
        let (server_tx, mut server_rx) = mpsc::channel(Self::CHANNEL_BUFFER_SIZE);
        let server_handle = tokio::spawn(async move {
            let addr = format!("127.0.0.1:{}", port).parse().unwrap();
            let service = MessageStreamServiceImpl::new(server_tx);
            let svc = MessageStreamServiceServer::new(service);

            println!("[GRPC_BACKEND] Starting gRPC server on {}", addr);
            
            Server::builder()
                .add_service(svc)
                .serve(addr)
                .await
                .unwrap();
            // TODO add interrupt
        });
        self.server_handle = Some(server_handle);

        // Start reader task
        let reader_senders = self.reader_senders.take().unwrap();
        let reader_task = tokio::spawn(async move {
            // TODO add timeout and running check
            while let Some((message, channel_id)) = server_rx.recv().await {
                let sender = reader_senders.get(&channel_id).unwrap();
                if let Err(e) = sender.send(message).await {
                    panic!("[GRPC_BACKEND] Failed to forward message to channel {}: {}", channel_id, e);
                }
            }
        });
        self.reader_task = Some(reader_task);

        // Start clients for each peer node
        let channel_to_node = self.channel_to_node.take().unwrap();
        let nodes = self.nodes.take().unwrap();
        let mut client_txs: HashMap<String, mpsc::Sender<(Message, String)>> = HashMap::new();
        for (node_ip, node_id) in nodes {
            let server_addr = format!("http://{}", node_ip); // TODO add port from channel
            let (client_tx, client_rx) = mpsc::channel(Self::CHANNEL_BUFFER_SIZE);
            client_txs.insert(node_id, client_tx);
            let client_stream_task = tokio::spawn(async move {
                let mut client = MessageStreamClient::connect(server_addr).await.unwrap();
                client.stream_messages(client_rx).await.unwrap();
            });
            
            self.client_handles.push(client_stream_task);
        }

        let writer_receivers = self.writer_receivers.take().unwrap();
        let writer_task = tokio::spawn(async move {
            let mut receiver_futures = Vec::new();
            
            // Create futures for all writer receivers
            for (channel_id, mut receiver) in writer_receivers {
                let channel_id_clone = channel_id.clone();
                let client_txs_clone = client_txs.clone();
                let channel_to_node_clone = channel_to_node.clone();
                
                let future = async move {
                    loop { // TODO timeout and running check
                        match receiver.recv().await {
                            Some(message) => {
                                
                                // Find the target node for this channel
                                let node_id = channel_to_node_clone.get(&channel_id_clone).unwrap();
                                let client_tx = client_txs_clone.get(node_id).unwrap();
                                if let Err(e) = client_tx.send((message, channel_id_clone.clone())).await {
                                    panic!("[GRPC_BACKEND] Failed to send message to client {}: {}", node_id, e);
                                }
                            }
                            None => break, // Receiver closed
                        }
                    }
                };
                
                receiver_futures.push(Box::pin(future));
            }
            
            // Wait for all receiver tasks to complete
            let _ = futures::future::join_all(receiver_futures).await;
        });
        self.writer_task = Some(writer_task);
    }

    async fn close(&mut self) {
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

        self.port = Some(self.get_port_from_remote_channels(&input_channels));

        let (remote_nodes, channel_to_node) = self.get_remote_nodes_from_channels(&output_channels);
        self.channel_to_node = Some(channel_to_node);
        self.nodes = Some(remote_nodes);

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

        transport_client_configs
    }
} 