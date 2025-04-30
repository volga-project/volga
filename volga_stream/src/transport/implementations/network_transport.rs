use async_trait::async_trait;
use crate::common::data_batch::DataBatch;
use anyhow::Result;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as TokioMutex;
use crate::transport::channel::Channel;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use crate::transport::transport::{DataReader, DataWriter, DataReaderConfig, DataWriterConfig};

pub struct NetworkDataReader {
    id: String,
    name: String,
    job_name: String,
    channels: Arc<Vec<Channel>>,
    output_queue: Arc<TokioMutex<Vec<DataBatch>>>,
    output_queue_capacity: usize,
    response_batch_period_ms: Option<usize>,
    running: Arc<AtomicBool>,
}

impl NetworkDataReader {
    pub fn new(
        id: String,
        name: String,
        job_name: String,
        channels: Vec<Channel>,
        config: DataReaderConfig,
    ) -> Self {
        let (output_queue_capacity, response_batch_period_ms) = match config {
            DataReaderConfig::Dummy => (1000, Some(100)),
            DataReaderConfig::Network { output_queue_capacity_bytes, response_batch_period_ms } => {
                (output_queue_capacity_bytes, response_batch_period_ms)
            }
        };

        Self {
            id,
            name,
            job_name,
            channels: Arc::new(channels),
            output_queue: Arc::new(TokioMutex::new(Vec::with_capacity(output_queue_capacity))),
            output_queue_capacity,
            response_batch_period_ms,
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn start(&self) {
        // TODO: Implement network reading logic
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

#[async_trait]
impl DataReader for NetworkDataReader {
    async fn read_batch(&mut self, max_batch_size: usize) -> Result<Option<DataBatch>> {
        let mut queue = self.output_queue.lock().await;
        if queue.is_empty() {
            return Ok(None);
        }

        let batch = queue.remove(0);
        Ok(Some(batch))
    }
}

pub struct NetworkDataWriter {
    id: String,
    name: String,
    job_name: String,
    channels: Arc<Vec<Channel>>,
    in_flight_timeout_s: usize,
    max_capacity_bytes_per_channel: usize,
    running: Arc<AtomicBool>,
    channel_queues: Arc<TokioMutex<HashMap<String, Vec<DataBatch>>>>,
}

impl NetworkDataWriter {
    pub fn new(
        id: String,
        name: String,
        job_name: String,
        channels: Vec<Channel>,
        config: DataWriterConfig,
    ) -> Self {
        let (in_flight_timeout_s, max_capacity_bytes_per_channel) = match config {
            DataWriterConfig::Dummy => (30, 1000000),
            DataWriterConfig::Network { in_flight_timeout_s, max_capacity_bytes_per_channel } => {
                (in_flight_timeout_s, max_capacity_bytes_per_channel)
            }
        };

        Self {
            id,
            name,
            job_name,
            channels: Arc::new(channels),
            in_flight_timeout_s,
            max_capacity_bytes_per_channel,
            running: Arc::new(AtomicBool::new(true)),
            channel_queues: Arc::new(TokioMutex::new(HashMap::new())),
        }
    }

    pub fn start(&self) {
        // TODO: Implement network writing logic
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

#[async_trait]
impl DataWriter for NetworkDataWriter {
    async fn write_batch(&mut self, channel_id: &str, batch: DataBatch) -> Result<()> {
        let mut queues = self.channel_queues.lock().await;
        let queue = queues.entry(channel_id.to_string()).or_insert_with(Vec::new);
        queue.push(batch);
        Ok(())
    }
} 