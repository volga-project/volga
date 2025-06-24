use std::collections::HashMap;

use crate::runtime::execution_graph::ExecutionGraph;
use crate::transport::transport_client::TransportClientConfig;
use kameo::Actor;
use kameo::message::{Context, Message};
use anyhow::Result;
use tonic::async_trait;

#[async_trait]
pub trait TransportBackend: Send + Sync {
    async fn start(&mut self);
    async fn close(&mut self);
    fn init_channels(&mut self, execution_graph: &ExecutionGraph, vertex_ids: Vec<String>) -> HashMap<String, TransportClientConfig>;
}

#[derive(Debug)]
pub enum TransportBackendActorMessage {
    Start,
    Close
}

#[derive(Debug, Clone)]
pub enum TransportBackendType {
    Grpc,
    InMemory,
}

#[derive(Actor)]
pub struct TransportBackendActor {
    backend: Box<dyn TransportBackend + Send + 'static>,
}

impl TransportBackendActor {
    pub fn new(backend: Box<dyn TransportBackend + Send + 'static>) -> Self {
        Self {
            backend,
        }
    }
}

impl Message<TransportBackendActorMessage> for TransportBackendActor {
    type Reply = Result<()>;

    async fn handle(&mut self, msg: TransportBackendActorMessage, _ctx: &mut Context<TransportBackendActor, Result<()>>) -> Self::Reply {
        match msg {
            TransportBackendActorMessage::Start => {
                self.backend.start().await;
            }
            TransportBackendActorMessage::Close => {
                self.backend.close().await;
            }
        }
        Ok(())
    }
} 