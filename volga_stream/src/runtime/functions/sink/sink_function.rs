use async_trait::async_trait;
use anyhow::Result;
use std::fmt;
use crate::common::message::Message;
use crate::runtime::execution_graph::SinkConfig;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use std::any::Any;

use super::in_memory_storage_sink::InMemoryStorageActorSinkFunction;

#[async_trait]
pub trait SinkFunctionTrait: Send + Sync + fmt::Debug {
    async fn sink(&mut self, message: Message) -> Result<()>;
}

#[derive(Debug)]
pub enum SinkFunction {
    InMemoryStorageActor(InMemoryStorageActorSinkFunction),
}

#[async_trait]
impl SinkFunctionTrait for SinkFunction {
    async fn sink(&mut self, message: Message) -> Result<()> {
        match self {
            SinkFunction::InMemoryStorageActor(f) => f.sink(message).await,
        }
    }
}

#[async_trait]
impl FunctionTrait for SinkFunction {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        match self {
            SinkFunction::InMemoryStorageActor(f) => f.open(context).await,
        }
    }
    
    async fn close(&mut self) -> Result<()> {
        match self {
            SinkFunction::InMemoryStorageActor(f) => f.close().await,
        }
    }
    
    async fn finish(&mut self) -> Result<()> {
        match self {
            SinkFunction::InMemoryStorageActor(f) => f.finish().await,
        }
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
    
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub fn create_sink_function(config: SinkConfig) -> SinkFunction {
    match config {
        SinkConfig::InMemoryStorageActorSinkConfig(storage_actor) => {
            SinkFunction::InMemoryStorageActor(InMemoryStorageActorSinkFunction::new(storage_actor))
        }
    }
}