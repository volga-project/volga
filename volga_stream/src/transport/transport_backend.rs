use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
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
    mpsc_channels: HashMap<String, (Arc<Mutex<mpsc::Sender<DataBatch>>>, Arc<Mutex<mpsc::Receiver<DataBatch>>>)>,
}

impl InMemoryTransportBackend {
    pub fn new() -> Self {
        Self {
            clients: HashMap::new(),
            mpsc_channels: HashMap::new(),
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
        if !self.mpsc_channels.contains_key(&channel_id) {
            let (tx, rx) = mpsc::channel(100); // Buffer size of 100
            self.mpsc_channels.insert(
                channel_id.clone(),
                (Arc::new(Mutex::new(tx)), Arc::new(Mutex::new(rx))),
            );
        }

        // Register the channel with the appropriate client
        if let Some(client) = self.clients.get_mut(&vertex_id) {
            let (tx, rx) = self.mpsc_channels.get(&channel_id).unwrap().clone();
            if is_in {
                client.register_in_channel(channel_id, rx).await?;
            } else {
                client.register_out_channel(channel_id, tx).await?;
            }
        }

        Ok(())
    }

    async fn register_client(&mut self, vertex_id: String, client: TransportClient) -> Result<()> {
        self.clients.insert(vertex_id, client);
        Ok(())
    }
} 