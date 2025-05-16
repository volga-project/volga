use async_trait::async_trait;
use crate::runtime::runtime_context::RuntimeContext;
use crate::common::data_batch::{DataBatch, KeyedDataBatch};
use anyhow::Result;
use tokio_rayon::rayon::{ThreadPool, ThreadPoolBuilder};
use std::fmt;
use crate::runtime::execution_graph::{SourceConfig, SinkConfig};
use crate::runtime::storage::in_memory_storage_actor::{InMemoryStorageActor, InMemoryStorageMessage};
use kameo::prelude::ActorRef;
use crate::runtime::sink_function::{SinkFunction, create_sink_function, SinkFunctionTrait};
use crate::runtime::map_function::MapFunction;
use crate::runtime::key_by_function::KeyByFunction;
use crate::runtime::key_by_function::KeyByFunctionTrait;

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
    async fn process_batch(&mut self, batch: DataBatch) -> Result<Option<Vec<DataBatch>>> {
        Err(anyhow::anyhow!("process_batch not implemented for this operator"))
    }
    fn operator_type(&self) -> OperatorType;
    async fn fetch(&mut self) -> Result<Option<DataBatch>> {
        Err(anyhow::anyhow!("fetch not implemented for this operator"))
    }
}

#[derive(Debug)]
pub enum Operator {
    Map(MapOperator),
    Join(JoinOperator),
    Sink(SinkOperator),
    Source(SourceOperator),
    KeyBy(KeyByOperator),
}

#[async_trait]
impl OperatorTrait for Operator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        match self {
            Operator::Map(op) => op.open(context).await,
            Operator::Join(op) => op.open(context).await,
            Operator::Sink(op) => op.open(context).await,
            Operator::Source(op) => op.open(context).await,
            Operator::KeyBy(op) => op.open(context).await,
        }
    }

    async fn close(&mut self) -> Result<()> {
        match self {
            Operator::Map(op) => op.close().await,
            Operator::Join(op) => op.close().await,
            Operator::Sink(op) => op.close().await,
            Operator::Source(op) => op.close().await,
            Operator::KeyBy(op) => op.close().await,
        }
    }

    async fn finish(&mut self) -> Result<()> {
        match self {
            Operator::Map(op) => op.finish().await,
            Operator::Join(op) => op.finish().await,
            Operator::Sink(op) => op.finish().await,
            Operator::Source(op) => op.finish().await,
            Operator::KeyBy(op) => op.finish().await,
        }
    }

    async fn process_batch(&mut self, batch: DataBatch) -> Result<Option<Vec<DataBatch>>> {
        match self {
            Operator::Map(op) => op.process_batch(batch).await,
            Operator::Join(op) => op.process_batch(batch).await,
            Operator::Sink(op) => op.process_batch(batch).await,
            Operator::Source(op) => op.process_batch(batch).await,
            Operator::KeyBy(op) => op.process_batch(batch).await,
        }
    }

    fn operator_type(&self) -> OperatorType {
        match self {
            Operator::Map(op) => op.operator_type(),
            Operator::Join(op) => op.operator_type(),
            Operator::Sink(op) => op.operator_type(),
            Operator::Source(op) => op.operator_type(),
            Operator::KeyBy(op) => op.operator_type(),
        }
    }

    async fn fetch(&mut self) -> Result<Option<DataBatch>> {
        match self {
            Operator::Map(op) => op.fetch().await,
            Operator::Join(op) => op.fetch().await,
            Operator::Sink(op) => op.fetch().await,
            Operator::Source(op) => op.fetch().await,
            Operator::KeyBy(op) => op.fetch().await,
        }
    }
}

#[derive(Debug)]
pub struct OperatorBase {
    runtime_context: Option<RuntimeContext>,
}

impl OperatorBase {
    pub fn new() -> Self {
        Self {
            runtime_context: None,
        }
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

#[derive(Debug)]
pub struct MapOperator {
    base: OperatorBase,
    map_function: MapFunction,
}

impl MapOperator {
    pub fn new(map_function: MapFunction) -> Self {
        Self { 
            base: OperatorBase::new(),
            map_function,
        }
    }
}

#[async_trait]
impl OperatorTrait for MapOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn process_batch(&mut self, batch: DataBatch) -> Result<Option<Vec<DataBatch>>> {
        let processed = self.map_function.map(batch).await?;
        Ok(Some(vec![processed]))
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }

    async fn fetch(&mut self) -> Result<Option<DataBatch>> {
        Err(anyhow::anyhow!("fetch not implemented for this operator"))
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    async fn finish(&mut self) -> Result<()> {
        self.base.finish().await
    }
}

#[derive(Debug)]
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

    async fn process_batch(&mut self, batch: DataBatch) -> Result<Option<Vec<DataBatch>>> {
        // TODO proper lookup for upstream_vertex_id position (left or right)
        if let Some(upstream_id) = batch.upstream_vertex_id() {
            if upstream_id.contains("left") {
                self.left_buffer.push(batch.clone());
            } else {
                self.right_buffer.push(batch.clone());
            }
        }
        Ok(Some(vec![batch]))
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }
}

#[derive(Debug)]
pub struct SinkOperator {
    base: OperatorBase,
    sink_function: SinkFunction,
}

impl SinkOperator {
    pub fn new(config: SinkConfig) -> Self {
        Self {
            base: OperatorBase::new(),
            sink_function: create_sink_function(config),
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

    async fn process_batch(&mut self, batch: DataBatch) -> Result<Option<Vec<DataBatch>>> {
        self.sink_function.sink(batch.clone()).await?;
        Ok(Some(vec![batch]))
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }
}

#[derive(Debug)]
pub struct SourceOperator {
    base: OperatorBase,
    batches: Vec<DataBatch>,
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
        if self.current_index < self.batches.len() {
            let batch = self.batches[self.current_index].clone();
            self.current_index += 1;
            println!("Source operator fetched batch: {:?}", batch);
            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug)]
pub struct KeyByOperator {
    base: OperatorBase,
    key_by_function: KeyByFunction,
}

impl KeyByOperator {
    pub fn new(key_by_function: KeyByFunction) -> Self {
        Self { 
            base: OperatorBase::new(),
            key_by_function,
        }
    }
}

#[async_trait]
impl OperatorTrait for KeyByOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn process_batch(&mut self, batch: DataBatch) -> Result<Option<Vec<DataBatch>>> {
        let keyed_batches = self.key_by_function.key_by(batch).await?;
        // Convert KeyedDataBatch to DataBatch::KeyedBatch
        let batches = keyed_batches.into_iter()
            .map(DataBatch::KeyedBatch)
            .collect();
        Ok(Some(batches))
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    async fn finish(&mut self) -> Result<()> {
        self.base.finish().await
    }
}
