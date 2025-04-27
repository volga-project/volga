use async_trait::async_trait;
use crate::common::record::StreamRecord;
use anyhow::Result;

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

#[async_trait]
pub trait DataReader: Send + Sync {
    async fn read_batch(&mut self, max_batch_size: usize) -> Result<Option<Vec<StreamRecord>>>;
}

pub struct DummyDataReader;

impl DummyDataReader {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl DataReader for DummyDataReader {
    async fn read_batch(&mut self, _max_batch_size: usize) -> Result<Option<Vec<StreamRecord>>> {
        Ok(None)
    }
}

// Factory function to create DataReader instances
pub fn create_data_reader(config: DataReaderConfig) -> Box<dyn DataReader> {
    match config {
        DataReaderConfig::Dummy => Box::new(DummyDataReader::new()),
        DataReaderConfig::Network { output_queue_capacity_bytes, response_batch_period_ms } => {
            // TODO: Implement network data reader
            Box::new(DummyDataReader::new())
        }
    }
}
