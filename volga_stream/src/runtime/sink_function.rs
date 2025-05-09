use async_trait::async_trait;
use anyhow::Result;
use std::fmt;
use crate::common::data_batch::DataBatch;
use crate::runtime::storage::in_memory_storage_actor::{InMemoryStorageActor, InMemoryStorageMessage};
use kameo::prelude::ActorRef;

#[async_trait]
pub trait SinkFunctionTrait: Send + Sync + fmt::Debug {
    async fn sink(&mut self, batch: DataBatch) -> Result<()>;
}

#[derive(Debug)]
pub enum SinkFunction {
    Vector(VectorSinkFunction),
    InMemoryStorageActor(InMemoryStorageActorSinkFunction),
}

#[async_trait]
impl SinkFunctionTrait for SinkFunction {
    async fn sink(&mut self, batch: DataBatch) -> Result<()> {
        match self {
            SinkFunction::Vector(f) => f.sink(batch).await,
            SinkFunction::InMemoryStorageActor(f) => f.sink(batch).await,
        }
    }
}

#[derive(Debug)]
pub struct VectorSinkFunction {
    output: Vec<DataBatch>,
}

impl VectorSinkFunction {
    pub fn new(output: Vec<DataBatch>) -> Self {
        Self { output }
    }
}

#[async_trait]
impl SinkFunctionTrait for VectorSinkFunction {
    async fn sink(&mut self, batch: DataBatch) -> Result<()> {
        self.output.push(batch);
        Ok(())
    }
}

#[derive(Debug)]
pub struct InMemoryStorageActorSinkFunction {
    storage_actor: ActorRef<InMemoryStorageActor>,
}

impl InMemoryStorageActorSinkFunction {
    pub fn new(storage_actor: ActorRef<InMemoryStorageActor>) -> Self {
        Self { storage_actor }
    }
}

#[async_trait]
impl SinkFunctionTrait for InMemoryStorageActorSinkFunction {
    async fn sink(&mut self, batch: DataBatch) -> Result<()> {
        self.storage_actor.tell(InMemoryStorageMessage::Append { batch }).await?;
        Ok(())
    }
}

pub fn create_sink_function(config: crate::runtime::execution_graph::SinkConfig) -> SinkFunction {
    match config {
        crate::runtime::execution_graph::SinkConfig::VectorSinkConfig(output) => {
            SinkFunction::Vector(VectorSinkFunction::new(output))
        }
        crate::runtime::execution_graph::SinkConfig::InMemoryStorageActorSinkConfig(storage_actor) => {
            SinkFunction::InMemoryStorageActor(InMemoryStorageActorSinkFunction::new(storage_actor))
        }
    }
} 