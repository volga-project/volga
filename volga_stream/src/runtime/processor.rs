use crate::runtime::{collector::Collector, operator::Operator, runtime_context::RuntimeContext, operator::OperatorType, execution_graph::OperatorOrConfig};
use crate::common::record::StreamRecord;
use anyhow::{Error, Result};
use std::any::Any;
use async_trait::async_trait;

pub struct ProcessorFactory;

impl ProcessorFactory {
    pub fn new() -> Self {
        Self
    }

    pub fn create_processor(&self, operator: OperatorOrConfig) -> Result<Box<dyn Processor>> {
        let operator: Box<dyn Operator> = match operator {
            OperatorOrConfig::Operator(op) => op,
            OperatorOrConfig::Config(config) => {
                let operator_type = config.get("type")
                    .ok_or_else(|| anyhow::anyhow!("Operator type not specified in config"))?;
                
                match operator_type.as_str() {
                    "map" => {
                        // TODO: Create MapOperator with appropriate function
                        todo!("Implement map operator creation")
                    },
                    "join" => {
                        // TODO: Create JoinOperator with appropriate function
                        todo!("Implement join operator creation")
                    },
                    "sink" => {
                        // TODO: Create SinkOperator with appropriate function
                        todo!("Implement sink operator creation")
                    },
                    "source" => {
                        // TODO: Create SourceOperator with appropriate function
                        todo!("Implement source operator creation")
                    },
                    _ => return Err(anyhow::anyhow!("Unknown operator type: {}", operator_type)),
                }
            }
        };

        match operator.operator_type() {
            OperatorType::SOURCE => Ok(Box::new(SourceProcessor::new(operator))),
            OperatorType::PROCESSOR => Ok(Box::new(StreamProcessor::new(operator))),
        }
    }
}

#[async_trait]
pub trait Processor: Send + Sync {
    async fn open(&mut self, collector: Option<Box<dyn Collector>>, runtime_context: RuntimeContext) -> Result<()>;
    async fn process_batch(&mut self, records: Vec<StreamRecord>, stream_id: Option<usize>) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
    async fn finish(&mut self) -> Result<()>;
    fn as_any(&self) -> &dyn Any;

    // Default implementation for non-source processors
    async fn fetch(&mut self) -> Result<()> {
        Err(anyhow::anyhow!("Not a source processor"))
    }
}

pub struct StreamProcessor {
    operator: Box<dyn Operator>,
    runtime_context: Option<RuntimeContext>,
}

impl StreamProcessor {
    pub fn new(operator: Box<dyn Operator>) -> Self {
        Self {
            operator,
            runtime_context: None,
        }
    }
}

#[async_trait]
impl Processor for StreamProcessor {
    async fn open(&mut self, collector: Option<Box<dyn Collector>>, runtime_context: RuntimeContext) -> Result<()> {
        self.runtime_context = Some(runtime_context.clone());
        self.operator.open(collector, runtime_context).await
    }

    async fn process_batch(&mut self, records: Vec<StreamRecord>, stream_id: Option<usize>) -> Result<()> {
        self.operator.process_batch(records, stream_id).await
    }

    async fn close(&mut self) -> Result<()> {
        self.operator.close().await
    }

    async fn finish(&mut self) -> Result<()> {
        self.operator.finish().await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct SourceProcessor {
    operator: Box<dyn Operator>,
    runtime_context: Option<RuntimeContext>,
}

impl SourceProcessor {
    pub fn new(operator: Box<dyn Operator>) -> Self {
        Self {
            operator,
            runtime_context: None,
        }
    }
}

#[async_trait]
impl Processor for SourceProcessor {
    async fn open(&mut self, collector: Option<Box<dyn Collector>>, runtime_context: RuntimeContext) -> Result<()> {
        self.runtime_context = Some(runtime_context.clone());
        self.operator.open(collector, runtime_context).await
    }

    async fn process_batch(&mut self, _records: Vec<StreamRecord>, _stream_id: Option<usize>) -> Result<()> {
        Err(anyhow::anyhow!("SourceProcessor does not process input records"))
    }

    async fn close(&mut self) -> Result<()> {
        self.operator.close().await
    }

    async fn finish(&mut self) -> Result<()> {
        self.operator.finish().await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn fetch(&mut self) -> Result<()> {
        self.operator.fetch().await
    }
}