use async_trait::async_trait;
use anyhow::Result;
use std::fmt;
use crate::common::message::Message;
use crate::runtime::operators::sink::sink_operator::SinkConfig;
use crate::runtime::functions::sink::in_memory_storage_sink::InMemoryStorageSinkFunction;
use crate::runtime::functions::sink::request_sink::RequestSinkFunction;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use std::any::Any;

#[async_trait]
pub trait SinkFunctionTrait: Send + Sync + fmt::Debug {
    async fn sink(&mut self, message: Message) -> Result<()>;
}

#[derive(Debug)]
pub enum SinkFunction {
    InMemoryStorageGrpc(InMemoryStorageSinkFunction),
    Request(RequestSinkFunction),
}

impl fmt::Display for SinkFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SinkFunction::InMemoryStorageGrpc(_) => write!(f, "InMemoryStorageGrpc"),
            SinkFunction::Request(_) => write!(f, "Request"),
        }
    }
}

#[async_trait]
impl SinkFunctionTrait for SinkFunction {
    async fn sink(&mut self, message: Message) -> Result<()> {
        match self {
            SinkFunction::InMemoryStorageGrpc(f) => f.sink(message).await,
            SinkFunction::Request(f) => f.sink(message).await,
        }
    }
}

#[async_trait]
impl FunctionTrait for SinkFunction {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        match self {
            SinkFunction::InMemoryStorageGrpc(f) => f.open(context).await,
            SinkFunction::Request(f) => f.open(context).await,
        }
    }
    
    async fn close(&mut self) -> Result<()> {
        match self {
            SinkFunction::InMemoryStorageGrpc(f) => f.close().await,
            SinkFunction::Request(f) => f.close().await,
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
        SinkConfig::InMemoryStorageGrpcSinkConfig(server_addr) => {
            SinkFunction::InMemoryStorageGrpc(InMemoryStorageSinkFunction::new(server_addr))
        }
        SinkConfig::RequestSinkConfig(response_sender) => {
            SinkFunction::Request(RequestSinkFunction::new(response_sender))
        }
    }
}