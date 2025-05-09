use crate::transport::channel::Channel;
use crate::transport::transport_client_actor::TransportClientActorType;
use crate::transport::transport_backend::{TransportBackend, InMemoryTransportBackend};
use kameo::Actor;
use kameo::message::{Context, Message};
use anyhow::Result;

#[derive(Debug)]
pub enum TransportBackendActorMessage {
    Start,
    Close,
    RegisterChannel {
        vertex_id: String,
        channel: Channel,
        is_input: bool,
    },
    RegisterActor {
        vertex_id: String,
        actor: TransportClientActorType,
    },
}

#[derive(Actor)]
pub struct TransportBackendActor {
    backend: InMemoryTransportBackend,
}

impl TransportBackendActor {
    pub fn new() -> Self {
        Self {
            backend: InMemoryTransportBackend::new(),
        }
    }
}

impl Message<TransportBackendActorMessage> for TransportBackendActor {
    type Reply = Result<()>;

    async fn handle(&mut self, msg: TransportBackendActorMessage, _ctx: &mut Context<TransportBackendActor, Result<()>>) -> Self::Reply {
        match msg {
            TransportBackendActorMessage::Start => {
                self.backend.start().await?;
            }
            TransportBackendActorMessage::Close => {
                self.backend.close().await?;
            }
            TransportBackendActorMessage::RegisterChannel { vertex_id, channel, is_input } => {
                self.backend.register_channel(vertex_id, channel, is_input).await?;
            }
            TransportBackendActorMessage::RegisterActor { vertex_id, actor } => {
                self.backend.register_actor(vertex_id, actor).await?;
            }
        }
        Ok(())
    }
} 