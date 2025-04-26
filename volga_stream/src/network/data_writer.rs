use async_trait::async_trait;
use crate::core::record::StreamRecord;
use anyhow::Result;

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