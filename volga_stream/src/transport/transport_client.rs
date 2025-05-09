use anyhow::{Result, anyhow};
use std::collections::HashMap;
use tokio::sync::mpsc;
use crate::common::data_batch::DataBatch;
use std::fmt;
use std::time::Duration;
use tokio::time;

pub struct DataReader {
    vertex_id: String,
    receivers: HashMap<String, mpsc::Receiver<DataBatch>>,
    default_timeout: Duration,
    default_retries: usize,
}

impl DataReader {
    pub fn new(vertex_id: String) -> Self {
        Self {
            vertex_id,
            receivers: HashMap::new(),
            default_timeout: Duration::from_millis(10000),
            default_retries: 0,
        }
    }

    pub fn register_receiver(&mut self, channel_id: String, receiver: mpsc::Receiver<DataBatch>) {
        self.receivers.insert(channel_id, receiver);
    }

    pub async fn read_batch(&mut self) -> Result<Option<DataBatch>> {
        self.read_batch_with_params(None, None).await
    }

    pub async fn read_batch_with_params(
        &mut self,
        timeout_duration: Option<Duration>,
        retries: Option<usize>
    ) -> Result<Option<DataBatch>> {
        let timeout_duration = timeout_duration.unwrap_or(self.default_timeout);
        let retries = retries.unwrap_or(self.default_retries);
        let mut attempts = 0;

        while attempts <= retries {
            if self.receivers.is_empty() {
                return Err(anyhow!("Attempted to read batch from DataReader {} with no channels registered", self.vertex_id));
            }

            // Create a future that completes when any channel has data
            let mut futures = Vec::new();
            for (channel_id, receiver) in self.receivers.iter_mut() {
                futures.push(Box::pin(async move {
                    match time::timeout(timeout_duration, receiver.recv()).await {
                        Ok(Some(batch)) => Some((channel_id.clone(), batch)),
                        Ok(None) => None,
                        Err(_) => None,
                    }
                }));
            }

            // Wait for the first channel to have data
            let result = tokio::select! {
                result = futures::future::select_all(futures) => {
                    match result.0 {
                        Some((channel_id, batch)) => Some(batch),
                        None => None,
                    }
                }
            };

            if result.is_some() {
                println!("DataReader {:?} read batch: {:?}", self.vertex_id, result.clone());
                return Ok(result);
            }
            attempts += 1;
        }
        Ok(None)
    }
}

impl fmt::Debug for DataReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataReader")
            .field("vertex_id", &self.vertex_id)
            .field("channels", &self.receivers.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[derive(Clone)]
pub struct DataWriter {
    vertex_id: String,
    senders: HashMap<String, mpsc::Sender<DataBatch>>,
    default_timeout: Duration,
    default_retries: usize,
}

impl DataWriter {
    pub fn new(vertex_id: String) -> Self {
        Self {
            vertex_id,
            senders: HashMap::new(),
            default_timeout: Duration::from_millis(100),
            default_retries: 0,
        }
    }

    pub fn register_sender(&mut self, channel_id: String, sender: mpsc::Sender<DataBatch>) {
        self.senders.insert(channel_id, sender);
    }

    pub async fn write_batch(&mut self, channel_id: &str, batch: DataBatch) -> Result<()> {
        self.write_batch_with_params(channel_id, batch, None, None).await
    }

    pub async fn write_batch_with_params(
        &mut self,
        channel_id: &str,
        batch: DataBatch,
        timeout_duration: Option<Duration>,
        retries: Option<usize>
    ) -> Result<()> {
        let timeout_duration = timeout_duration.unwrap_or(self.default_timeout);
        let retries = retries.unwrap_or(self.default_retries);
        let mut attempts = 0;

        while attempts <= retries {
            if self.senders.is_empty() {
                return Err(anyhow!("Attempted to write batch to DataWriter with no channels registered"));
            }
            
            if let Some(sender) = self.senders.get(channel_id) {
                match time::timeout(timeout_duration, sender.send(batch.clone())).await {
                    Ok(Ok(())) => {
                        println!("DataWriter {:?} wrote batch: {:?}", self.vertex_id, batch);
                        return Ok(());
                    }
                    Ok(Err(_)) => {
                        return Err(anyhow!("Channel {} closed", channel_id));
                    }
                    Err(_) => {
                        attempts += 1;
                        continue;
                    }
                }
            } else {
                return Err(anyhow!("Channel {} not found", channel_id));
            }
        }
        Err(anyhow!("Failed to write batch after {} retries", retries))
    }
}

impl fmt::Debug for DataWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataWriter")
            .field("vertex_id", &self.vertex_id)
            .field("channels", &self.senders.keys().collect::<Vec<_>>())
            .finish()
    }
}

pub struct TransportClient {
    vertex_id: String,
    pub reader: Option<DataReader>,
    pub writer: Option<DataWriter>,
}

impl TransportClient {
    pub fn new(vertex_id: String) -> Self {
        Self {
            vertex_id: vertex_id.clone(),
            reader: Some(DataReader::new(vertex_id.clone())),
            writer: Some(DataWriter::new(vertex_id)),
        }
    }

    pub fn vertex_id(&self) -> &str {
        &self.vertex_id
    }

    pub async fn register_receiver(&mut self, channel_id: String, receiver: mpsc::Receiver<DataBatch>) -> Result<()> {
        if let Some(reader) = &mut self.reader {
            reader.register_receiver(channel_id, receiver);
            Ok(())
        } else {
            Err(anyhow!("Reader not initialized"))
        }
    }

    pub async fn register_sender(&mut self, channel_id: String, sender: mpsc::Sender<DataBatch>) -> Result<()> {
        if let Some(writer) = &mut self.writer {
            writer.register_sender(channel_id, sender);
            Ok(())
        } else {
            Err(anyhow!("Writer not initialized"))
        }
    }
}

impl fmt::Debug for TransportClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransportClient")
            .field("vertex_id", &self.vertex_id)
            .field("reader", &self.reader)
            .field("writer", &self.writer)
            .finish()
    }
} 