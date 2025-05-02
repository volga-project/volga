use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use crate::common::data_batch::DataBatch;

#[derive(Clone)]
pub struct DataReader {
    channels: HashMap<String, Arc<Mutex<mpsc::Receiver<DataBatch>>>>,
}

impl DataReader {
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
        }
    }

    pub fn register_channel(&mut self, channel_id: String, receiver: Arc<Mutex<mpsc::Receiver<DataBatch>>>) {
        self.channels.insert(channel_id, receiver);
    }

    pub async fn read_batch(&mut self) -> Result<Option<DataBatch>> {
        if self.channels.is_empty() {
            return Ok(None);
        }

        // Create a future that completes when any channel has data
        let mut futures = Vec::new();
        for (channel_id, receiver) in self.channels.iter_mut() {
            futures.push(Box::pin(async move {
                let mut receiver = receiver.lock().await;
                match receiver.recv().await {
                    Some(batch) => Some((channel_id.clone(), batch)),
                    None => None,
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

        Ok(result)
    }
}

#[derive(Clone)]
pub struct DataWriter {
    channels: HashMap<String, Arc<Mutex<mpsc::Sender<DataBatch>>>>,
}

impl DataWriter {
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
        }
    }

    pub fn register_channel(&mut self, channel_id: String, sender: Arc<Mutex<mpsc::Sender<DataBatch>>>) {
        self.channels.insert(channel_id, sender);
    }

    pub async fn write_batch(&mut self, channel_id: &str, batch: DataBatch) -> Result<()> {
        if let Some(sender) = self.channels.get(channel_id) {
            let mut sender = sender.lock().await;
            sender.send(batch).await?;
            Ok(())
        } else {
            Err(anyhow!("Channel {} not found", channel_id))
        }
    }
}

#[derive(Clone)]
pub struct TransportClient {
    reader: Option<DataReader>,
    writer: Option<DataWriter>,
}

impl TransportClient {
    pub fn new() -> Self {
        Self {
            reader: Some(DataReader::new()),
            writer: Some(DataWriter::new()),
        }
    }

    pub fn register_in_channel(&mut self, channel_id: String, receiver: Arc<Mutex<mpsc::Receiver<DataBatch>>>) -> Result<()> {
        if let Some(reader) = &mut self.reader {
            reader.register_channel(channel_id, receiver);
            Ok(())
        } else {
            Err(anyhow!("Reader not initialized"))
        }
    }

    pub fn register_out_channel(&mut self, channel_id: String, sender: Arc<Mutex<mpsc::Sender<DataBatch>>>) -> Result<()> {
        if let Some(writer) = &mut self.writer {
            writer.register_channel(channel_id, sender);
            Ok(())
        } else {
            Err(anyhow!("Writer not initialized"))
        }
    }

    pub fn reader(&mut self) -> Option<&mut DataReader> {
        self.reader.as_mut()
    }

    pub fn writer(&mut self) -> Option<DataWriter> {
        self.writer.clone()
    }
} 