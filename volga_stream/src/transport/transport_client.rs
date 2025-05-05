use anyhow::{Result, anyhow};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use crate::common::data_batch::DataBatch;
use std::fmt;

#[derive(Clone)]
pub struct DataReader {
    vertex_id: String,
    channels: Arc<Mutex<HashMap<String, Arc<Mutex<mpsc::Receiver<DataBatch>>>>>>,
}

impl DataReader {
    pub fn new(vertex_id: String) -> Self {
        Self {
            vertex_id,
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn vertex_id(&self) -> &str {
        &self.vertex_id
    }

    pub async fn register_channel(&mut self, channel_id: String, receiver: Arc<Mutex<mpsc::Receiver<DataBatch>>>) {
        let mut channels = self.channels.lock().await;
        channels.insert(channel_id, receiver);
    }

    pub async fn read_batch(&mut self) -> Result<Option<DataBatch>> {
        let channels = self.channels.lock().await;
        if channels.is_empty() {
            panic!("Attempted to read batch from DataReader with no channels registered");
        }

        // Create a future that completes when any channel has data
        let mut futures = Vec::new();
        for (channel_id, receiver) in channels.iter() {
            let receiver = receiver.clone();
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

        println!("DataReader {:?} read batch: {:?}", self.vertex_id, result.clone());
        Ok(result)
    }
}

impl fmt::Debug for DataReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataReader")
            .field("vertex_id", &self.vertex_id)
            .field("channels", &"<locked>")
            .finish()
    }
}

#[derive(Clone)]
pub struct DataWriter {
    vertex_id: String,
    channels: Arc<Mutex<HashMap<String, Arc<Mutex<mpsc::Sender<DataBatch>>>>>>,
}

impl DataWriter {
    pub fn new(vertex_id: String) -> Self {
        Self {
            vertex_id,
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn vertex_id(&self) -> &str {
        &self.vertex_id
    }

    pub async fn register_channel(&mut self, channel_id: String, sender: Arc<Mutex<mpsc::Sender<DataBatch>>>) {
        let mut channels = self.channels.lock().await;
        channels.insert(channel_id, sender);
    }

    pub async fn write_batch(&mut self, channel_id: &str, batch: DataBatch) -> Result<()> {
        let channels = self.channels.lock().await;
        if channels.is_empty() {
            panic!("Attempted to write batch to DataWriter with no channels registered");
        }
        
        if let Some(sender) = channels.get(channel_id) {
            let sender = sender.lock().await;
            sender.send(batch.clone()).await?;
            println!("DataWriter {:?} wrote batch: {:?}", self.vertex_id, batch);
            Ok(())
        } else {
            Err(anyhow!("Channel {} not found", channel_id))
        }
    }
}

impl fmt::Debug for DataWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataWriter")
            .field("vertex_id", &self.vertex_id)
            .field("channels", &"<locked>")
            .finish()
    }
}

#[derive(Clone)]
pub struct TransportClient {
    vertex_id: String,
    reader: Option<DataReader>,
    writer: Option<DataWriter>,
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

    pub async fn register_in_channel(&mut self, channel_id: String, receiver: Arc<Mutex<mpsc::Receiver<DataBatch>>>) -> Result<()> {
        if let Some(reader) = &mut self.reader {
            reader.register_channel(channel_id, receiver).await;
            Ok(())
        } else {
            Err(anyhow!("Reader not initialized"))
        }
    }

    pub async fn register_out_channel(&mut self, channel_id: String, sender: Arc<Mutex<mpsc::Sender<DataBatch>>>) -> Result<()> {
        if let Some(writer) = &mut self.writer {
            writer.register_channel(channel_id, sender).await;
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

impl fmt::Debug for TransportClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransportClient")
            .field("vertex_id", &self.vertex_id)
            .field("reader", &self.reader)
            .field("writer", &self.writer)
            .finish()
    }
} 