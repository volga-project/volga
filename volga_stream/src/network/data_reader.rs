use async_trait::async_trait;
use crate::core::record::StreamRecord;
use anyhow::Result;

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
