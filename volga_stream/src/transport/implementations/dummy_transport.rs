use async_trait::async_trait;
use crate::common::record::StreamRecord;
use anyhow::Result;
use crate::transport::transport::{DataReader, DataWriter};

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