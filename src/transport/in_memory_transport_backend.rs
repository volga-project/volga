use std::collections::HashMap;
use crate::common::ids::ChannelId;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::VertexId;
use crate::transport::batch_channel::{batch_bounded_channel, BatchReceiver, BatchSender};
use crate::transport::channel::Channel;
use crate::transport::TransportBackend;
use async_trait::async_trait;

use super::transport_client::TransportClientConfig;

pub struct InMemoryTransportBackend {
    senders: HashMap<ChannelId, BatchSender>, // keep references so channels are not closed
    receivers: HashMap<ChannelId, BatchReceiver>,
}

impl InMemoryTransportBackend {
    pub fn new() -> Self {
        Self {
            senders: HashMap::new(),
            receivers: HashMap::new(),
        }
    }

    fn register_local_channel(
        &mut self,
        transport_client_configs: &mut HashMap<VertexId, TransportClientConfig>,
        vertex_id: VertexId,
        channel: Channel,
        is_in: bool
    ) {
        // Only handle local channels
        let channel_id = match &channel {
            Channel::Local { channel_id, ..} => *channel_id,
            _ => panic!("Only local channels are supported"),
        };

        // Create a new channel if it doesn't exist
        if !self.senders.contains_key(&channel_id) {
            let (tx, rx) = batch_bounded_channel(channel.get_queue_size_records());
            self.senders.insert(channel_id, tx);
            self.receivers.insert(channel_id, rx);
        }

        let config = transport_client_configs
            .entry(vertex_id)
            .or_insert(TransportClientConfig::new(vertex_id));

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

    fn init_channels(
        &mut self,
        execution_graph: &ExecutionGraph,
        vertex_ids: Vec<VertexId>,
    ) -> HashMap<VertexId, TransportClientConfig> {
        let mut transport_client_configs: HashMap<VertexId, TransportClientConfig> = HashMap::new();

        for vertex_id in vertex_ids {
            let (input_edges, output_edges) = execution_graph.get_edges_for_vertex(vertex_id).unwrap();
            for edge in input_edges {
                let channel = edge.get_channel();
                self.register_local_channel(
                    &mut transport_client_configs,
                    vertex_id,
                    channel,
                    true,
                );
            }

            for edge in output_edges {
                let channel = edge.get_channel();
                self.register_local_channel(
                    &mut transport_client_configs,
                    vertex_id,
                    channel,
                    false,
                );
            }
        }
        return transport_client_configs;
    }
}
