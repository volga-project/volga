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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorType {
    SOURCE,
    PROCESSOR,
}

#[derive(Debug, Clone)]
pub enum OperatorConfig {
    MapConfig(HashMap<String, String>),
    JoinConfig(HashMap<String, String>),
    SinkConfig(HashMap<String, String>),
    SourceConfig(HashMap<String, String>),
}

#[async_trait]
pub trait Operator: Send + Sync {
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

pub struct BaseOperator {
    runtime_context: Option<RuntimeContext>,
}

impl BaseOperator {
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
impl Operator for BaseOperator {
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

pub struct MapOperator {
    base: BaseOperator,
}

impl MapOperator {
    pub fn new() -> Self {
        Self {
            base: BaseOperator::new(),
        }
    }
}

#[async_trait]
impl Operator for MapOperator {
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
        Ok(DataBatch::new(None, result))
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }
}

pub struct JoinOperator {
    base: BaseOperator,
    left_buffer: Vec<DataBatch>,
    right_buffer: Vec<DataBatch>,
}

impl JoinOperator {
    pub fn new() -> Self {
        Self {
            base: BaseOperator::new(),
            left_buffer: Vec::new(),
            right_buffer: Vec::new(),
        }
    }
}

#[async_trait]
impl Operator for JoinOperator {
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

pub struct SinkOperator {
    base: BaseOperator,
    batches: Arc<Mutex<Vec<DataBatch>>>,
}

impl SinkOperator {
    pub fn new() -> Self {
        Self {
            base: BaseOperator::new(),
            batches: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_batches(&self) -> Arc<Mutex<Vec<DataBatch>>> {
        self.batches.clone()
    }
}

#[async_trait]
impl Operator for SinkOperator {
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
        Ok(batch)
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }
}

pub struct SourceOperator {
    base: BaseOperator,
    batches: Arc<Mutex<Vec<DataBatch>>>,
    current_index: usize,
    num_sent: usize,
}

impl SourceOperator {
    pub fn new(batches: Vec<DataBatch>) -> Self {
        Self {
            base: BaseOperator::new(),
            batches: Arc::new(Mutex::new(batches)),
            current_index: 0,
            num_sent: 0,
        }
    }

    pub fn get_num_sent(&self) -> usize {
        self.num_sent
    }
}

#[async_trait]
impl Operator for SourceOperator {
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
            self.num_sent += 1;
            self.current_index += 1;
            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }
}
