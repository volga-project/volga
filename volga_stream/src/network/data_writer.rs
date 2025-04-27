use async_trait::async_trait;
use crate::common::record::StreamRecord;
use anyhow::Result;

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
pub trait DataWriter: Send + Sync {
    async fn write_batch(&mut self, channel_id: &str, records: Vec<StreamRecord>) -> Result<()>;
}

pub struct DummyDataWriter;

impl DummyDataWriter {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl DataWriter for DummyDataWriter {
    async fn write_batch(&mut self, _channel_id: &str, _records: Vec<StreamRecord>) -> Result<()> {
        Ok(())
    }
}

// Factory function to create DataWriter instances
pub fn create_data_writer(config: DataWriterConfig) -> Box<dyn DataWriter> {
    match config {
        DataWriterConfig::Dummy => Box::new(DummyDataWriter::new()),
        DataWriterConfig::Network { in_flight_timeout_s, max_capacity_bytes_per_channel } => {
            // TODO: Implement network data writer
            Box::new(DummyDataWriter::new())
        }
    }
}