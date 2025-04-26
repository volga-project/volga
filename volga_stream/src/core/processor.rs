use crate::core::{collector::Collector, operator::Operator, record::StreamRecord, runtime_context::RuntimeContext};
use anyhow::{Error, Result};
use std::any::Any;
use async_trait::async_trait;

#[async_trait]
pub trait Processor: Send + Sync {
    async fn open(&mut self, collectors: Vec<Box<dyn Collector>>, runtime_context: RuntimeContext) -> Result<()>;
    async fn process_batch(&mut self, records: Vec<StreamRecord>, stream_id: Option<usize>) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
    async fn finish(&mut self) -> Result<()>;
    fn as_any(&self) -> &dyn Any;

    // Default implementation for non-source processors
    async fn fetch(&mut self) -> Result<()> {
        Err(Error::new("Not a source processor"))
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
    async fn open(&mut self, collectors: Vec<Box<dyn Collector>>, runtime_context: RuntimeContext) -> Result<()> {
        self.runtime_context = Some(runtime_context.clone());
        self.operator.open(collectors, runtime_context).await
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
    async fn open(&mut self, collectors: Vec<Box<dyn Collector>>, runtime_context: RuntimeContext) -> Result<()> {
        self.runtime_context = Some(runtime_context.clone());
        self.operator.open(collectors, runtime_context).await
    }

    async fn process_batch(&mut self, _records: Vec<StreamRecord>, _stream_id: Option<usize>) -> Result<()> {
        Err(Error::new("SourceProcessor does not process input records"))
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