use crate::core::{processor::Processor, runtime_context::RuntimeContext};
use anyhow::Result;
use async_trait::async_trait;
use std::time::Duration;
use crate::network::data_reader::DataReader;

#[async_trait]
pub trait StreamTask: Send + Sync {
    async fn run(&mut self) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
}

// StreamProcessingTask
pub struct StreamProcessingTask {
    processor: Box<dyn Processor>,
    data_reader: Box<dyn DataReader>,
    runtime_context: Option<RuntimeContext>,
    running: bool,
}

impl StreamProcessingTask {
    const BATCH_SIZE: usize = 1000;
    const READ_RETRY_PERIOD_MS: u64 = 1;
    
    pub fn new(
        processor: Box<dyn Processor>,
        data_reader: Box<dyn DataReader>,
        runtime_context: RuntimeContext,
    ) -> Self {
        Self {
            processor,
            data_reader,
            runtime_context: Some(runtime_context),
            running: true,
        }
    }
}

#[async_trait]
impl StreamTask for StreamProcessingTask {
    async fn run(&mut self) -> Result<()> {
        while self.running {
            if let Some(batch) = self.data_reader.read_batch(Self::BATCH_SIZE).await? {
                self.processor.process_batch(batch, None).await?;
            } else {
                tokio::time::sleep(Duration::from_millis(Self::READ_RETRY_PERIOD_MS)).await;
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.running = false;
        self.processor.close().await
    }
}

// SourceStreamTask
pub struct SourceStreamTask {
    processor: Box<dyn Processor>,
    runtime_context: Option<RuntimeContext>,
    running: bool,
    fetch_interval_ms: u64,
}

impl SourceStreamTask {
    const DEFAULT_FETCH_INTERVAL_MS: u64 = 1;

    pub fn new(
        processor: Box<dyn Processor>,
        runtime_context: RuntimeContext,
        fetch_interval_ms: Option<u64>,
    ) -> Self {
        Self {
            processor,
            runtime_context: Some(runtime_context),
            running: true,
            fetch_interval_ms: fetch_interval_ms.unwrap_or(Self::DEFAULT_FETCH_INTERVAL_MS),
        }
    }
}

#[async_trait]
impl StreamTask for SourceStreamTask {
    async fn run(&mut self) -> Result<()> {
        while self.running {
            self.processor.fetch().await?;
            tokio::time::sleep(Duration::from_millis(self.fetch_interval_ms)).await;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.running = false;
        self.processor.close().await
    }
}