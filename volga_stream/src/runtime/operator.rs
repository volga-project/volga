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

use super::{function::{Function, JoinFunction, MapFunction, SourceFunction, SinkFunction}, source::{SourceContextImpl, TimestampAssigner}};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorType {
    SOURCE,
    PROCESSOR,
}

#[async_trait]
pub trait Operator: Send + Sync {
    async fn open(&mut self, runtime_context: RuntimeContext) -> Result<()>;
    async fn fetch(&mut self) -> Result<Option<DataBatch>> {
        Err(anyhow::anyhow!("Not a source operator"))
    }
    async fn process_batch(&mut self, batch: DataBatch, stream_id: Option<usize>) -> Result<DataBatch>;
    async fn close(&mut self) -> Result<()>;
    async fn finish(&mut self) -> Result<()>;
    fn as_any(&self) -> &dyn Any;
    fn operator_type(&self) -> OperatorType;
}

// Base operator
pub struct BaseOperator<F: Function> {
    pub func: F,
    pub runtime_context: Option<RuntimeContext>,
}

impl<F: Function> BaseOperator<F> {
    pub fn new(func: F) -> Self {
        Self {
            func,
            runtime_context: None,
        }
    }
}

// MapOperator
pub struct MapOperator<F: MapFunction + Clone + 'static> {
    base: BaseOperator<F>,
}

impl<F: MapFunction + Clone + 'static> MapOperator<F> {
    pub fn new(func: F) -> Self {
        Self {
            base: BaseOperator::new(func),
        }
    }
}

#[async_trait]
impl<F: MapFunction + Clone + 'static> Operator for MapOperator<F> {
    async fn open(&mut self, runtime_context: RuntimeContext) -> Result<()> {
        self.base.runtime_context = Some(runtime_context.clone());
        self.base.func.open(runtime_context).await
    }

    async fn close(&mut self) -> Result<()> {
        self.base.func.close().await
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_batch(&mut self, batch: DataBatch, stream_id: Option<usize>) -> Result<DataBatch> {
        if stream_id != Some(0) {
            return Err(anyhow::anyhow!("MapOperator only accepts input from stream 0"));
        }

        self.base.func.map(&batch)
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// JoinOperator
pub struct JoinOperator<F: JoinFunction + Clone + 'static> {
    base: BaseOperator<F>,
    left_batches: HashMap<String, DataBatch>,
    right_batches: HashMap<String, DataBatch>,
}

impl<F: JoinFunction + Clone + 'static> JoinOperator<F> {
    pub fn new(func: F) -> Self {
        Self {
            base: BaseOperator::new(func),
            left_batches: HashMap::new(),
            right_batches: HashMap::new(),
        }
    }
}

#[async_trait]
impl<F: JoinFunction + Clone + 'static> Operator for JoinOperator<F> {
    async fn open(&mut self, runtime_context: RuntimeContext) -> Result<()> {
        self.base.runtime_context = Some(runtime_context.clone());
        self.base.func.open(runtime_context).await
    }

    async fn close(&mut self) -> Result<()> {
        self.base.func.close().await
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_batch(&mut self, batch: DataBatch, stream_id: Option<usize>) -> Result<DataBatch> {
        let key = batch.key()?;
        
        match stream_id {
            Some(0) => { // Left stream
                self.left_batches.insert(key.clone(), batch.clone());
                    
                if let Some(right_batch) = self.right_batches.get(&key) {
                    let mut func = self.base.func.clone();
                    let batch_clone = batch.clone();
                    let right_batch = right_batch.clone();
                    
                    if let Some(runtime_context) = &self.base.runtime_context {
                        if let Some(compute_pool) = runtime_context.compute_pool() {
                            // TODO: Use we should await this
                            compute_pool.spawn_fifo(move || {
                                func.join(&batch_clone, &right_batch).map(|_| ()).unwrap_or(())
                            });
                        }
                    }
                    Ok(batch)
                } else {
                    Ok(batch)
                }
            }
            Some(1) => { // Right stream
                self.right_batches.insert(key.clone(), batch.clone());
                
                if let Some(left_batch) = self.left_batches.get(&key) {
                    let mut func = self.base.func.clone();
                    let batch_clone = batch.clone();
                    let left_batch = left_batch.clone();
                    
                    if let Some(runtime_context) = &self.base.runtime_context {
                        if let Some(compute_pool) = runtime_context.compute_pool() {
                            // TODO: Use we should await this
                            compute_pool.spawn_fifo(move || {
                                func.join(&left_batch, &batch_clone).map(|_| ()).unwrap_or(())
                            });
                        }
                    }
                    Ok(batch)
                } else {
                    Ok(batch)
                }
            }
            _ => Err(anyhow::anyhow!("Invalid stream ID for JoinOperator")),
        }
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// SinkOperator
pub struct SinkOperator<F: SinkFunction + 'static> {
    base: BaseOperator<F>,
}

impl<F: SinkFunction + 'static> SinkOperator<F> {
    pub fn new(func: F) -> Self {
        Self {
            base: BaseOperator::new(func),
        }
    }
}

#[async_trait]
impl<F: SinkFunction + 'static> Operator for SinkOperator<F> {
    async fn open(&mut self, runtime_context: RuntimeContext) -> Result<()> {
        self.base.runtime_context = Some(runtime_context.clone());
        self.base.func.open(runtime_context).await
    }

    async fn close(&mut self) -> Result<()> {
        self.base.func.close().await
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_batch(&mut self, batch: DataBatch, stream_id: Option<usize>) -> Result<DataBatch> {
        if stream_id != Some(0) {
            return Err(anyhow::anyhow!("SinkOperator only accepts input from stream 0"));
        }

        self.base.func.sink(&batch).await?;
        Ok(batch)
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct SourceOperator<F: SourceFunction + 'static> {
    base: BaseOperator<F>,
    source_context: Option<SourceContextImpl>,
}

impl<F: SourceFunction + 'static> SourceOperator<F> {
    pub fn new(func: F) -> Self {
        Self {
            base: BaseOperator::new(func),
            source_context: None,
        }
    }

    pub fn set_timestamp_assigner(&mut self, assigner: Box<dyn TimestampAssigner>) {
        if let Some(ctx) = &mut self.source_context {
            ctx.timestamp_assigner = Some(assigner);
        }
    }
}

#[async_trait]
impl<F: SourceFunction + 'static> Operator for SourceOperator<F> {
    async fn open(&mut self, runtime_context: RuntimeContext) -> Result<()> {
        self.base.runtime_context = Some(runtime_context.clone());
        self.base.func.open(runtime_context.clone()).await?;

        self.source_context = Some(SourceContextImpl::new(
            vec![],
            runtime_context,
            None,
            None,
            None,
        ));

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.base.func.close().await
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_batch(&mut self, _batch: DataBatch, _stream_id: Option<usize>) -> Result<DataBatch> {
        Err(anyhow::anyhow!("SourceOperator does not process input records"))
    }

    async fn fetch(&mut self) -> Result<Option<DataBatch>> {
        self.base.func.fetch().await
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::SOURCE
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
