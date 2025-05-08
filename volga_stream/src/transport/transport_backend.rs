use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::mpsc;
use crate::common::data_batch::DataBatch;
use crate::transport::channel::Channel;
use crate::transport::transport_client::TransportClient;
use async_trait::async_trait;

#[async_trait]
pub trait TransportBackend: Send + Sync {
    async fn start(&mut self) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
    async fn register_channel(&mut self, vertex_id: String, channel: Channel, is_input: bool) -> Result<()>;
    async fn register_client(&mut self, vertex_id: String, client: TransportClient) -> Result<()>;
}

pub struct InMemoryTransportBackend {
    clients: HashMap<String, TransportClient>,
    senders: HashMap<String, mpsc::Sender<DataBatch>>,
    receivers: HashMap<String, mpsc::Receiver<DataBatch>>,
}

impl InMemoryTransportBackend {
    pub fn new() -> Self {
        Self {
            clients: HashMap::new(),
            senders: HashMap::new(),
            receivers: HashMap::new(),
        }
    }
}

#[async_trait]
impl TransportBackend for InMemoryTransportBackend {
    async fn start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    async fn register_channel(&mut self, vertex_id: String, channel: Channel, is_in: bool) -> Result<()> {
        // Only handle local channels
        let channel_id = match &channel {
            Channel::Local { channel_id } => channel_id.clone(),
            _ => return Err(anyhow::anyhow!("Only local channels are supported")),
        };

        // Create a new channel if it doesn't exist
        // TODO we should use a more efficient way to handle this
        if !self.senders.contains_key(&channel_id) {
            let (tx, rx) = mpsc::channel(100); // Buffer size of 100
            self.senders.insert(channel_id.clone(), tx);
            self.receivers.insert(channel_id.clone(), rx);
        }

        // Register the channel with the appropriate client
        if let Some(client) = self.clients.get_mut(&vertex_id) {
            if is_in {
                if let Some(rx) = self.receivers.remove(&channel_id) {
                    client.register_receiver(channel_id, rx).await?;
                }
            } else {
                if let Some(tx) = self.senders.remove(&channel_id) {
                    client.register_sender(channel_id, tx).await?;
                }
            }
        }

        Ok(())
    }

    async fn register_client(&mut self, vertex_id: String, client: TransportClient) -> Result<()> {
        self.clients.insert(vertex_id, client);
        Ok(())
    }
} 