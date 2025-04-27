// Core Function traits
use async_trait::async_trait;
use crate::runtime::runtime_context::RuntimeContext;
use anyhow::Result; 
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::common::record::{Value, StreamRecord, Record, BaseRecord};
use crate::runtime::source::SourceContext;

#[async_trait]
pub trait Function: Send + Sync {
    async fn open(&mut self, runtime_context: RuntimeContext) -> Result<()> {
        Ok(())
    }
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait MapFunction: Function {
    async fn map(&mut self, value: &Value) -> Result<Value>;
}

#[async_trait]
pub trait FilterFunction: Function {
    async fn filter(&mut self, value: &Value) -> Result<bool>;
}

#[async_trait]
pub trait JoinFunction: Function {
    async fn join(&mut self, left: &Value, right: &Value) -> Result<Value>;
}

#[async_trait]
pub trait SourceFunction: Function {
    async fn fetch(&mut self, ctx: &mut dyn SourceContext) -> Result<()>;
}

#[async_trait]
pub trait SinkFunction: Function {
    async fn sink(&mut self, value: &Value) -> Result<()>;
}

// Simple vector source implementation
pub struct VectorSourceFunction {
    values: Arc<Mutex<Vec<Value>>>,
    current_index: usize,
    num_sent: usize,
    batch_size: usize,
}

impl VectorSourceFunction {
    pub fn new(values: Vec<Value>, batch_size: usize) -> Self {
        Self {
            values: Arc::new(Mutex::new(values)),
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
    async fn fetch(&mut self, ctx: &mut dyn SourceContext) -> Result<()> {
        let values = self.values.lock().await;
        let end_index = std::cmp::min(self.current_index + self.batch_size, values.len());
        
        if self.current_index < values.len() {
            let mut records = Vec::with_capacity(end_index - self.current_index);
            for i in self.current_index..end_index {
                records.push(StreamRecord::Record(Record {
                    base: BaseRecord {
                        value: values[i].clone(),
                        event_time: None,
                        source_emit_ts: None,
                        stream_name: None,
                    },
                }));
            }
            ctx.collect_batch(records).await?;
            self.num_sent += end_index - self.current_index;
            self.current_index = end_index;
        }
        Ok(())
    }
}

// Simple vector sink implementation
pub struct VectorSinkFunction {
    values: Arc<Mutex<Vec<Value>>>,
}

impl VectorSinkFunction {
    pub fn new() -> Self {
        Self {
            values: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_values(&self) -> Arc<Mutex<Vec<Value>>> {
        self.values.clone()
    }
}

#[async_trait]
impl Function for VectorSinkFunction {
    // Default implementations from Function trait are sufficient
}

#[async_trait]
impl SinkFunction for VectorSinkFunction {
    async fn sink(&mut self, value: &Value) -> Result<()> {
        let mut values = self.values.lock().await;
        values.push(value.clone());
        Ok(())
    }
}