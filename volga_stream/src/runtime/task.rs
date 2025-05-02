use crate::runtime::{runtime_context::RuntimeContext, collector::{OutputCollector, Collector}, execution_graph::{ExecutionVertex, OperatorConfig}};
use anyhow::Result;
use async_trait::async_trait;
use std::time::Duration;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use crate::transport::transport_client::{TransportClient, DataWriter};
use crate::runtime::operator::{Operator, MapOperator, JoinOperator, SinkOperator, SourceOperator};
use crate::common::data_batch::DataBatch;
use tokio_rayon::rayon::ThreadPool;
use std::collections::HashMap;

#[async_trait]
pub trait Task: Send + Sync {
    async fn open(&mut self) -> Result<()>;
    async fn run(&mut self) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
}

pub struct StreamTask {
    operator: Box<dyn Operator>,
    transport_client: TransportClient,
    runtime_context: Option<RuntimeContext>,
    collector: Option<Box<dyn Collector>>,
    running: bool,
}

impl StreamTask {
    const DEFAULT_FETCH_INTERVAL_MS: u64 = 1;
    
    pub async fn new(
        vertex_id: String,
        operator_config: OperatorConfig,
        runtime_context: RuntimeContext,
    ) -> Result<Self> {
        // Create operator based on config
        let operator: Box<dyn Operator> = match operator_config {
            OperatorConfig::MapConfig(_) => {
                Box::new(MapOperator::new())
            }
            OperatorConfig::JoinConfig(_) => {
                Box::new(JoinOperator::new())
            }
            OperatorConfig::SinkConfig(_) => {
                Box::new(SinkOperator::new())
            }
            OperatorConfig::SourceConfig(_) => {
                Box::new(SourceOperator::new(Vec::new()))
            }
        };

        Ok(Self {
            operator,
            transport_client: TransportClient::new(),
            runtime_context: Some(runtime_context),
            collector: None,
            running: true,
        })
    }

    pub fn transport_client(&self) -> TransportClient {
        self.transport_client.clone()
    }

    pub fn register_channels(
        &mut self,
        input_channels: HashMap<String, Arc<Mutex<mpsc::Receiver<DataBatch>>>>,
        output_channels: HashMap<String, Arc<Mutex<mpsc::Sender<DataBatch>>>>,
    ) -> Result<()> {
        // Register input channels with the data reader
        for (channel_id, receiver) in input_channels {
            self.transport_client.register_in_channel(channel_id, receiver)?;
        }

        // Register output channels with the data writer
        for (channel_id, sender) in output_channels {
            self.transport_client.register_out_channel(channel_id, sender)?;
        }

        Ok(())
    }
}

#[async_trait]
impl Task for StreamTask {
    async fn open(&mut self) -> Result<()> {
        // Create OutputCollector with the data_writer if it exists
        let collector = if let Some(writer) = self.transport_client.writer() {
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
            self.operator.open(&runtime_context).await?;
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
                }
                crate::runtime::operator::OperatorType::PROCESSOR => {
                    if let Some(reader) = self.transport_client.reader() {
                        if let Some(batch) = reader.read_batch().await? {
                            let processed_batch = self.operator.process_batch(batch, None).await?;
                            if let Some(collector) = &mut self.collector {
                                collector.collect_batch(processed_batch).await?;
                            }
                        }
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