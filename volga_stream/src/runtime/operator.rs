use async_trait::async_trait;
use crate::runtime::collector::Collector;
use crate::runtime::runtime_context::RuntimeContext;
use crate::common::data_batch::DataBatch;
use anyhow::Result;
use std::any::Any;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc;
use tokio_rayon::rayon::{ThreadPool, ThreadPoolBuilder};
use tokio::sync::Mutex;
use std::fmt;
use crate::runtime::execution_graph::{SourceConfig, SinkConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorType {
    SOURCE,
    PROCESSOR,
}

#[async_trait]
pub trait OperatorTrait: Send + Sync + fmt::Debug {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
    async fn finish(&mut self) -> Result<()>;
    async fn process_batch(&mut self, batch: DataBatch, stream_id: Option<usize>) -> Result<DataBatch> {
        Err(anyhow::anyhow!("process_batch not implemented for this operator"))
    }
    fn operator_type(&self) -> OperatorType;
    async fn fetch(&mut self) -> Result<Option<DataBatch>> {
        Err(anyhow::anyhow!("fetch not implemented for this operator"))
    }
}

#[derive(Debug, Clone)]
pub enum Operator {
    Map(MapOperator),
    Join(JoinOperator),
    Sink(SinkOperator),
    Source(SourceOperator),
}

#[async_trait]
impl OperatorTrait for Operator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        match self {
            Operator::Map(op) => op.open(context).await,
            Operator::Join(op) => op.open(context).await,
            Operator::Sink(op) => op.open(context).await,
            Operator::Source(op) => op.open(context).await,
        }
    }

    async fn close(&mut self) -> Result<()> {
        match self {
            Operator::Map(op) => op.close().await,
            Operator::Join(op) => op.close().await,
            Operator::Sink(op) => op.close().await,
            Operator::Source(op) => op.close().await,
        }
    }

    async fn finish(&mut self) -> Result<()> {
        match self {
            Operator::Map(op) => op.finish().await,
            Operator::Join(op) => op.finish().await,
            Operator::Sink(op) => op.finish().await,
            Operator::Source(op) => op.finish().await,
        }
    }

    async fn process_batch(&mut self, batch: DataBatch, stream_id: Option<usize>) -> Result<DataBatch> {
        match self {
            Operator::Map(op) => op.process_batch(batch, stream_id).await,
            Operator::Join(op) => op.process_batch(batch, stream_id).await,
            Operator::Sink(op) => op.process_batch(batch, stream_id).await,
            Operator::Source(op) => op.process_batch(batch, stream_id).await,
        }
    }

    fn operator_type(&self) -> OperatorType {
        match self {
            Operator::Map(op) => op.operator_type(),
            Operator::Join(op) => op.operator_type(),
            Operator::Sink(op) => op.operator_type(),
            Operator::Source(op) => op.operator_type(),
        }
    }

    async fn fetch(&mut self) -> Result<Option<DataBatch>> {
        match self {
            Operator::Map(op) => op.fetch().await,
            Operator::Join(op) => op.fetch().await,
            Operator::Sink(op) => op.fetch().await,
            Operator::Source(op) => op.fetch().await,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OperatorBase {
    runtime_context: Option<RuntimeContext>,
}

impl OperatorBase {
    pub fn new() -> Self {
        Self {
            runtime_context: None,
        }
    }

    pub fn get_runtime_context(&self) -> Option<&RuntimeContext> {
        self.runtime_context.as_ref()
    }
}

#[async_trait]
impl OperatorTrait for OperatorBase {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.runtime_context = Some(context.clone());
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }
}

#[derive(Debug, Clone)]
pub struct MapOperator {
    base: OperatorBase,
}

impl MapOperator {
    pub fn new() -> Self {
        Self {
            base: OperatorBase::new(),
        }
    }
}

#[async_trait]
impl OperatorTrait for MapOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    async fn finish(&mut self) -> Result<()> {
        self.base.finish().await
    }

    async fn process_batch(&mut self, batch: DataBatch, stream_id: Option<usize>) -> Result<DataBatch> {
        if stream_id != Some(0) {
            return Err(anyhow::anyhow!("Map operator only accepts input from stream 0"));
        }
        let mut result = Vec::new();
        for record in batch.record_batch() {
            result.push(record.clone());
        }

        println!("Map operator processed batch: {:?}", batch);
        Ok(DataBatch::new(None, result))
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }
}

#[derive(Debug, Clone)]
pub struct JoinOperator {
    base: OperatorBase,
    left_buffer: Vec<DataBatch>,
    right_buffer: Vec<DataBatch>,
}

impl JoinOperator {
    pub fn new() -> Self {
        Self {
            base: OperatorBase::new(),
            left_buffer: Vec::new(),
            right_buffer: Vec::new(),
        }
    }
}

#[async_trait]
impl OperatorTrait for JoinOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    async fn finish(&mut self) -> Result<()> {
        self.base.finish().await
    }

    async fn process_batch(&mut self, batch: DataBatch, stream_id: Option<usize>) -> Result<DataBatch> {
        match stream_id {
            Some(0) => self.left_buffer.push(batch),
            Some(1) => self.right_buffer.push(batch),
            _ => return Err(anyhow::anyhow!("Join operator only accepts input from streams 0 and 1")),
        }

        if !self.left_buffer.is_empty() && !self.right_buffer.is_empty() {
            let left = self.left_buffer.remove(0);
            let right = self.right_buffer.remove(0);
            let mut result = Vec::new();
            for record in left.record_batch() {
                result.push(record.clone());
            }
            for record in right.record_batch() {
                result.push(record.clone());
            }
            Ok(DataBatch::new(None, result))
        } else {
            Ok(DataBatch::new(None, Vec::new()))
        }
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }
}

#[derive(Debug, Clone)]
pub struct SinkOperator {
    base: OperatorBase,
    batches: Arc<Mutex<Vec<DataBatch>>>,
}

impl SinkOperator {
    pub fn new(config: SinkConfig) -> Self {
        let batches = match config {
            SinkConfig::VectorSinkConfig(batches) => batches,
        };

        Self {
            base: OperatorBase::new(),
            batches,
        }
    }
}

#[async_trait]
impl OperatorTrait for SinkOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    async fn finish(&mut self) -> Result<()> {
        self.base.finish().await
    }

    async fn process_batch(&mut self, batch: DataBatch, stream_id: Option<usize>) -> Result<DataBatch> {
        if stream_id != Some(0) {
            return Err(anyhow::anyhow!("Sink operator only accepts input from stream 0"));
        }
        let mut batches = self.batches.lock().await;
        batches.push(batch.clone());

        println!("Sink operator processed batch: {:?}", batch);
        Ok(batch)
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }
}

#[derive(Debug, Clone)]
pub struct SourceOperator {
    base: OperatorBase,
    batches: Arc<Mutex<Vec<DataBatch>>>,
    current_index: usize,
}

impl SourceOperator {
    pub fn new(config: SourceConfig) -> Self {
        let batches = match config {
            SourceConfig::VectorSourceConfig(batches) => batches,
        };

        Self {
            base: OperatorBase::new(),
            batches,
            current_index: 0,
        }
    }
}

#[async_trait]
impl OperatorTrait for SourceOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    async fn finish(&mut self) -> Result<()> {
        self.base.finish().await
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::SOURCE
    }

    async fn fetch(&mut self) -> Result<Option<DataBatch>> {
        let batches = self.batches.lock().await;
        
        if self.current_index < batches.len() {
            let batch = batches[self.current_index].clone();
            self.current_index += 1;
            println!("Source operator fetched batch: {:?}", batch);
            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }
}
