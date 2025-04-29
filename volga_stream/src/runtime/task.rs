use crate::runtime::{processor::Processor, runtime_context::RuntimeContext, collector::{OutputCollector, Collector}, execution_graph::ExecutionVertex, processor::ProcessorFactory};
use anyhow::Result;
use async_trait::async_trait;
use std::time::Duration;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::transport::transport::{TransportConfig, Transport};
use crate::transport::transport_factory::create_transport;


#[async_trait]
pub trait StreamTask: Send + Sync {
    async fn open(&mut self) -> Result<()>;
    async fn run(&mut self) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
}

// StreamProcessingTask
pub struct StreamProcessingTask {
    processor: Box<dyn Processor>,
    transport: Transport,
    runtime_context: Option<RuntimeContext>,
    running: bool,
}

impl StreamProcessingTask {
    const BATCH_SIZE: usize = 1000;
    const READ_RETRY_PERIOD_MS: u64 = 1;
    
    pub fn new(
        vertex: ExecutionVertex,
        processor_factory: &ProcessorFactory,
        transport_config: TransportConfig,
        runtime_context: RuntimeContext,
    ) -> Result<Self> {
        let processor = processor_factory.create_processor(vertex.operator)?;
        
        Ok(Self {
            processor,
            transport: create_transport(transport_config),
            runtime_context: Some(runtime_context),
            running: true,
        })
    }
}

#[async_trait]
impl StreamTask for StreamProcessingTask {
    async fn open(&mut self) -> Result<()> {
        // Create OutputCollector with the data_writer if it exists
        let collector = if let Some(writer) = self.transport.writer.take() {
            Some(Box::new(OutputCollector::new(
                Arc::new(Mutex::new(writer)),
                vec![], // TODO: Get channel IDs from runtime context
                Box::new(crate::runtime::partition::RoundRobinPartition::new()),
            )) as Box<dyn Collector>)
        } else {
            None
        };

        // Open the processor with the collector
        if let Some(runtime_context) = self.runtime_context.take() {
            self.processor.open(collector, runtime_context).await?;
        }
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        while self.running {
            let reader = self.transport.reader.as_mut().expect("Transport reader must be initialized");
            if let Some(batch) = reader.read_batch(Self::BATCH_SIZE).await? {
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
    transport: Transport,
    runtime_context: Option<RuntimeContext>,
    running: bool,
    fetch_interval_ms: u64,
}

impl SourceStreamTask {
    const DEFAULT_FETCH_INTERVAL_MS: u64 = 1;

    pub fn new(
        vertex: ExecutionVertex,
        processor_factory: &ProcessorFactory,
        transport_config: TransportConfig,
        runtime_context: RuntimeContext,
        fetch_interval_ms: Option<u64>,
    ) -> Result<Self> {
        let processor = processor_factory.create_processor(vertex.operator)?;
        
        Ok(Self {
            processor,
            transport: create_transport(transport_config),
            runtime_context: Some(runtime_context),
            running: true,
            fetch_interval_ms: fetch_interval_ms.unwrap_or(Self::DEFAULT_FETCH_INTERVAL_MS),
        })
    }
}

#[async_trait]
impl StreamTask for SourceStreamTask {
    async fn open(&mut self) -> Result<()> {
        // Create OutputCollector with the data_writer if it exists
        let collector = if let Some(writer) = self.transport.writer.take() {
            Some(Box::new(OutputCollector::new(
                Arc::new(Mutex::new(writer)),
                vec![], // TODO: Get channel IDs from runtime context
                Box::new(crate::runtime::partition::RoundRobinPartition::new()),
            )) as Box<dyn Collector>)
        } else {
            None
        };

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