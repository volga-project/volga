use crate::runtime::{runtime_context::RuntimeContext, collector::{OutputCollector, Collector}, execution_graph::ExecutionVertex};
use anyhow::Result;
use async_trait::async_trait;
use std::time::Duration;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::transport::transport::{TransportConfig, Transport};
use crate::transport::transport_factory::create_transport;
use crate::runtime::operator::Operator;
use crate::common::data_batch::DataBatch;

#[async_trait]
pub trait Task: Send + Sync {
    async fn open(&mut self) -> Result<()>;
    async fn run(&mut self) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
}

pub struct StreamTask {
    operator: Box<dyn Operator>,
    transport: Transport,
    runtime_context: Option<RuntimeContext>,
    collector: Option<Box<dyn Collector>>,
    running: bool,
    fetch_interval_ms: u64,
}

impl StreamTask {
    const BATCH_SIZE: usize = 1000;
    const READ_RETRY_PERIOD_MS: u64 = 1;
    const DEFAULT_FETCH_INTERVAL_MS: u64 = 1;
    
    pub fn new(
        vertex: ExecutionVertex,
        operator: Box<dyn Operator>,
        transport_config: TransportConfig,
        runtime_context: RuntimeContext,
        fetch_interval_ms: Option<u64>,
    ) -> Result<Self> {
        Ok(Self {
            operator,
            transport: create_transport(transport_config),
            runtime_context: Some(runtime_context),
            collector: None,
            running: true,
            fetch_interval_ms: fetch_interval_ms.unwrap_or(Self::DEFAULT_FETCH_INTERVAL_MS),
        })
    }
}

#[async_trait]
impl Task for StreamTask {
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

        // Open the operator with runtime context
        if let Some(runtime_context) = self.runtime_context.take() {
            self.operator.open(runtime_context).await?;
        }
        
        self.collector = collector;
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        while self.running {
            match self.operator.operator_type() {
                crate::runtime::operator::OperatorType::SOURCE => {
                    if let Some(batch) = self.operator.fetch().await? {
                        if let Some(collector) = &mut self.collector {
                            collector.collect_batch(batch).await?;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(self.fetch_interval_ms)).await;
                }
                crate::runtime::operator::OperatorType::PROCESSOR => {
                    let reader = self.transport.reader.as_mut().expect("Transport reader must be initialized");
                    if let Some(batch) = reader.read_batch(Self::BATCH_SIZE).await? {
                        let processed_batch = self.operator.process_batch(batch, None).await?;
                        if let Some(collector) = &mut self.collector {
                            collector.collect_batch(processed_batch).await?;
                        }
                    } else {
                        tokio::time::sleep(Duration::from_millis(Self::READ_RETRY_PERIOD_MS)).await;
                    }
                }
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.running = false;
        self.operator.close().await
    }
}