// Core Function traits
use crate::runtime::runtime_context::RuntimeContext;
use anyhow::Result; 
use std::sync::Arc;
use tokio::sync::Mutex;
use async_trait::async_trait;
use crate::common::data_batch::DataBatch;

#[async_trait]
pub trait Function: Send + Sync {
    async fn open(&mut self, runtime_context: RuntimeContext) -> Result<()> {
        Ok(())
    }
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

// Blocking functions that will be executed in thread pool
pub trait MapFunction: Function {
    fn map(&mut self, batch: &DataBatch) -> Result<DataBatch>;
}

pub trait FilterFunction: Function {
    fn filter(&mut self, batch: &DataBatch) -> Result<bool>;
}

pub trait JoinFunction: Function {
    fn join(&mut self, left: &DataBatch, right: &DataBatch) -> Result<DataBatch>;
}

// Async functions
#[async_trait]
pub trait SourceFunction: Function {
    async fn fetch(&mut self) -> Result<Option<DataBatch>>;
}

#[async_trait]
pub trait SinkFunction: Function {
    async fn sink(&mut self, batch: &DataBatch) -> Result<()>;
}

// Simple vector source implementation
pub struct VectorSourceFunction {
    batches: Arc<Mutex<Vec<DataBatch>>>,
    current_index: usize,
    num_sent: usize,
    batch_size: usize,
}

impl VectorSourceFunction {
    pub fn new(batches: Vec<DataBatch>, batch_size: usize) -> Self {
        Self {
            batches: Arc::new(Mutex::new(batches)),
            current_index: 0,
            num_sent: 0,
            batch_size,
        }
    }

    pub fn get_num_sent(&self) -> usize {
        self.num_sent
    }
}

#[async_trait]
impl Function for VectorSourceFunction {
    // Default implementations from Function trait are sufficient
}

#[async_trait]
impl SourceFunction for VectorSourceFunction {
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

// Simple vector sink implementation
pub struct VectorSinkFunction {
    batches: Arc<Mutex<Vec<DataBatch>>>,
}

impl VectorSinkFunction {
    pub fn new() -> Self {
        Self {
            batches: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_batches(&self) -> Arc<Mutex<Vec<DataBatch>>> {
        self.batches.clone()
    }
}

#[async_trait]
impl Function for VectorSinkFunction {
    // Default implementations from Function trait are sufficient
}

#[async_trait]
impl SinkFunction for VectorSinkFunction {
    async fn sink(&mut self, batch: &DataBatch) -> Result<()> {
        let mut batches = self.batches.lock().await;
        batches.push(batch.clone());
        Ok(())
    }
}