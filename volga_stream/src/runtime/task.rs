use crate::runtime::{processor::Processor, runtime_context::RuntimeContext, collector::OutputCollector};
use anyhow::Result;
use async_trait::async_trait;
use std::time::Duration;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::network::{data_reader::{DataReader, DataReaderConfig, create_data_reader}, data_writer::{DataWriter, DataWriterConfig, create_data_writer}};

#[async_trait]
pub trait StreamTask: Send + Sync {
    async fn open(&mut self) -> Result<()>;
    async fn run(&mut self) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
}

// StreamProcessingTask
pub struct StreamProcessingTask {
    processor: Box<dyn Processor>,
    data_reader: Box<dyn DataReader>,
    data_writer: Box<dyn DataWriter>,
    runtime_context: Option<RuntimeContext>,
    running: bool,
}

impl StreamProcessingTask {
    const BATCH_SIZE: usize = 1000;
    const READ_RETRY_PERIOD_MS: u64 = 1;
    
    pub fn new(
        processor: Box<dyn Processor>,
        data_reader_config: DataReaderConfig,
        data_writer_config: DataWriterConfig,
        runtime_context: RuntimeContext,
    ) -> Self {
        Self {
            processor,
            data_reader: create_data_reader(data_reader_config),
            data_writer: create_data_writer(data_writer_config),
            runtime_context: Some(runtime_context),
            running: true,
        }
    }
}

#[async_trait]
impl StreamTask for StreamProcessingTask {
    async fn open(&mut self) -> Result<()> {
        // Create OutputCollector with the data_writer
        let data_writer = Arc::new(Mutex::new(std::mem::replace(&mut self.data_writer, create_data_writer(DataWriterConfig::Dummy))));
        let collector = Box::new(OutputCollector::new(
            data_writer,
            vec![], // TODO: Get channel IDs from runtime context
            Box::new(crate::runtime::partition::RoundRobinPartition::new()),
        ));

        // Open the processor with the collector
        if let Some(runtime_context) = self.runtime_context.take() {
            self.processor.open(collector, runtime_context).await?;
        }
        Ok(())
    }

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
    data_writer: Box<dyn DataWriter>,
    runtime_context: Option<RuntimeContext>,
    running: bool,
    fetch_interval_ms: u64,
}

impl SourceStreamTask {
    const DEFAULT_FETCH_INTERVAL_MS: u64 = 1;

    pub fn new(
        processor: Box<dyn Processor>,
        data_writer_config: DataWriterConfig,
        runtime_context: RuntimeContext,
        fetch_interval_ms: Option<u64>,
    ) -> Self {
        Self {
            processor,
            data_writer: create_data_writer(data_writer_config),
            runtime_context: Some(runtime_context),
            running: true,
            fetch_interval_ms: fetch_interval_ms.unwrap_or(Self::DEFAULT_FETCH_INTERVAL_MS),
        }
    }
}

#[async_trait]
impl StreamTask for SourceStreamTask {
    async fn open(&mut self) -> Result<()> {
        // Create OutputCollector with the data_writer
        let data_writer = Arc::new(Mutex::new(std::mem::replace(&mut self.data_writer, create_data_writer(DataWriterConfig::Dummy))));
        let collector = Box::new(OutputCollector::new(
            data_writer,
            vec![], // TODO: Get channel IDs from runtime context
            Box::new(crate::runtime::partition::RoundRobinPartition::new()),
        ));

        // Open the processor with the collector
        if let Some(runtime_context) = self.runtime_context.take() {
            self.processor.open(collector, runtime_context).await?;
        }
        Ok(())
    }

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