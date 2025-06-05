use crate::transport::transport_backend::{TransportBackend, InMemoryTransportBackend};
use kameo::Actor;
use kameo::message::{Context, Message};
use anyhow::Result;

#[derive(Debug)]
pub enum TransportBackendActorMessage {
    Start,
    Close
}

#[derive(Actor)]
pub struct TransportBackendActor {
    backend: InMemoryTransportBackend,
}

impl TransportBackendActor {
    pub fn new(backend: InMemoryTransportBackend) -> Self {
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