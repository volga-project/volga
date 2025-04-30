use async_trait::async_trait;
use crate::common::data_batch::DataBatch;
use anyhow::Result;
use std::sync::Arc;
use crate::transport::channel::Channel;

#[derive(Clone)]
pub enum DataReaderConfig {
    Dummy,
    Network {
        output_queue_capacity_bytes: usize,
        response_batch_period_ms: Option<usize>
    }
}

impl Default for DataReaderConfig {
    fn default() -> Self {
        DataReaderConfig::Network {
            output_queue_capacity_bytes: 1000,
            response_batch_period_ms: Some(100)
        }
    }
}

#[derive(Clone)]
pub enum DataWriterConfig {
    Dummy,
    Network {
        in_flight_timeout_s: usize,
        max_capacity_bytes_per_channel: usize
    }
}

impl Default for DataWriterConfig {
    fn default() -> Self {
        DataWriterConfig::Network {
            in_flight_timeout_s: 30,
            max_capacity_bytes_per_channel: 1000000
        }
    }
}

#[async_trait]
pub trait DataReader: Send + Sync {
    async fn read_batch(&mut self, max_batch_size: usize) -> Result<Option<DataBatch>>;
}

#[async_trait]
pub trait DataWriter: Send + Sync {
    async fn write_batch(&mut self, channel_id: &str, batch: DataBatch) -> Result<()>;
}

pub struct Transport {
    pub reader: Option<Box<dyn DataReader>>,
    pub writer: Option<Box<dyn DataWriter>>,
}

pub struct TransportConfig {
    pub reader_config: DataReaderConfig,
    pub writer_config: DataWriterConfig,
    pub channels: Vec<Channel>,
    pub id: String,
    pub name: String,
    pub job_name: String,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            reader_config: DataReaderConfig::default(),
            writer_config: DataWriterConfig::default(),
            channels: Vec::new(),
            id: "default".to_string(),
            name: "default".to_string(),
            job_name: "default".to_string(),
        }
    }
} 