use crate::runtime::{runtime_context::RuntimeContext, collector::{OutputCollector, Collector}, execution_graph::{ExecutionVertex, OperatorConfig}, execution_graph::ExecutionGraph, partition::Partition};
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
use std::any::Any;
use futures::future::join_all;

#[async_trait]
pub trait Task: Send + Sync {
    async fn open(&mut self) -> Result<()>;
    async fn run(&mut self) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
}

pub struct StreamTask {
    vertex_id: String,
    operator: Box<dyn Operator>,
    runtime_context: RuntimeContext,
    transport_client: TransportClient,
    collectors: HashMap<String, Box<dyn Collector>>,
    running: bool,
}

impl StreamTask {
    
    pub async fn new(
        vertex_id: String,
        operator_config: OperatorConfig,
        runtime_context: RuntimeContext,
    ) -> Result<Self> {
        let operator: Box<dyn Operator> = match operator_config {
            OperatorConfig::MapConfig(_) => Box::new(MapOperator::new()),
            OperatorConfig::JoinConfig(_) => Box::new(JoinOperator::new()),
            OperatorConfig::SinkConfig(config) => Box::new(SinkOperator::new(config)),
            OperatorConfig::SourceConfig(config) => Box::new(SourceOperator::new(config)),
        };
        let transport_client = TransportClient::new();
        
        Ok(Self {
            vertex_id,
            operator,
            runtime_context,
            transport_client,
            collectors: HashMap::new(),
            running: true,
        })
    }

    pub fn transport_client(&self) -> TransportClient {
        self.transport_client.clone()
    }

    pub fn register_output_channel(
        &mut self,
        channel_id: String,
        partition: Box<dyn Partition>,
        target_operator_id: String,
    ) -> Result<()> {
        let collector = self.collectors.entry(target_operator_id).or_insert_with(|| {
            let data_writer = Arc::new(Mutex::new(DataWriter::new()));
            Box::new(OutputCollector::new(
                data_writer,
                partition,
            )) as Box<dyn Collector>
        });

        if let Some(output_collector) = (collector.as_mut() as &mut dyn Any).downcast_mut::<OutputCollector>() {
            output_collector.register_output_channel_id(channel_id);
        }

        Ok(())
    }

    async fn collect_batch_parallel(&mut self, batch: DataBatch) -> Result<()> {
        let mut futures = Vec::new();
        for collector in self.collectors.values_mut() {
            let batch_clone = batch.clone();
            futures.push(collector.collect_batch(batch_clone));
        }
        join_all(futures).await.into_iter().collect::<Result<Vec<()>>>()?;
        Ok(())
    }
}

#[async_trait]
impl Task for StreamTask {
    async fn open(&mut self) -> Result<()> {
        // Open the operator with runtime context
        self.operator.open(&self.runtime_context).await?;
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        while self.running {
            match self.operator.operator_type() {
                crate::runtime::operator::OperatorType::SOURCE => {
                    if let Some(batch) = self.operator.fetch().await? {
                        self.collect_batch_parallel(batch).await?;
                    }
                }
                _ => {
                    if let Some(reader) = self.transport_client.reader() {
                        if let Some(batch) = reader.read_batch().await? {
                            let processed_batch = self.operator.process_batch(batch, None).await?;
                            self.collect_batch_parallel(processed_batch).await?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.running = false;
        self.operator.close().await?;
        Ok(())
    }
}