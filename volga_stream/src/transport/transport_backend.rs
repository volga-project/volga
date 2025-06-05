use std::collections::HashMap;
use tokio::sync::mpsc;
use crate::common::message::Message;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::transport::channel::Channel;
use async_trait::async_trait;

use super::transport_client::TransportClientConfig;

#[async_trait]
pub trait TransportBackend: Send + Sync {
    async fn start(&mut self);
    async fn close(&mut self);
    fn init_channels(&mut self, execution_graph: &ExecutionGraph, vertex_ids: Vec<String>) -> HashMap<String, TransportClientConfig>;
}

pub struct InMemoryTransportBackend {
    senders: HashMap<String, mpsc::Sender<Message>>, // keep references so channels are not closed
    receivers: HashMap<String, mpsc::Receiver<Message>>,
}

impl InMemoryTransportBackend {
    const CHANNEL_BUFFER_SIZE: usize = 100;

    pub fn new() -> Self {
        Self {
            senders: HashMap::new(),
            receivers: HashMap::new(),
        }
    }

    fn register_channel(
        &mut self,
        transport_client_configs: &mut HashMap<String, TransportClientConfig>, 
        vertex_id: String, 
        channel: Channel, 
        is_in: bool
    ) {
        // Only handle local channels
        let channel_id = match &channel {
            Channel::Local { channel_id } => channel_id.clone(),
            _ => panic!("Only local channels are supported"),
        };

        // Create a new channel if it doesn't exist
        if !self.senders.contains_key(&channel_id) {
            let (tx, rx) = mpsc::channel(Self::CHANNEL_BUFFER_SIZE);
            self.senders.insert(channel_id.clone(), tx);
            self.receivers.insert(channel_id.clone(), rx);
        }

        let config = transport_client_configs.entry(vertex_id.clone()).or_insert(TransportClientConfig::new(vertex_id.clone()));

        if is_in {
            // For readers, we need to move the receiver since it can't be cloned
            if let Some(rx) = self.receivers.remove(&channel_id) {
                config.add_reader_receiver(channel_id, rx);
            }
        } else {
            // For writers, we can clone the sender since it's designed for multiple producers
            if let Some(tx) = self.senders.get(&channel_id) {
                config.add_writer_sender(channel_id, tx.clone());
            }
        }
    }
}

#[async_trait]
impl TransportBackend for InMemoryTransportBackend {
    async fn start(&mut self) {
    }

    async fn close(&mut self) {
    }

    fn init_channels(&mut self, execution_graph: &ExecutionGraph, vertex_ids: Vec<String>) -> HashMap<String, TransportClientConfig> {
        let mut transport_client_configs: HashMap<String, TransportClientConfig> = HashMap::new();
        
        for vertex_id in vertex_ids {
            let (input_edges, output_edges) = execution_graph.get_edges_for_vertex(&vertex_id).unwrap();
            for edge in input_edges {
                let channel = edge.channel.clone();
                self.register_channel(
                    &mut transport_client_configs,
                    vertex_id.clone(),
                    channel,
                    true,
                );
            }

            for edge in output_edges {
                let channel = edge.channel.clone();
                self.register_channel(
                    &mut transport_client_configs,
                    vertex_id.clone(),
                    channel,
                    false,
                );
            }
        }
        return transport_client_configs;
    }
} 