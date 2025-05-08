use anyhow::Result;
use kameo::Actor;
use kameo::message::{Context, Message};
use crate::runtime::task::StreamTask;
use crate::transport::transport_backend::{TransportBackend, InMemoryTransportBackend};
use crate::transport::channel::Channel;
use crate::runtime::partition::PartitionType;
use crate::transport::transport_actor::{TransportActor, TransportActorMessage, TransportActorType};
use tokio::sync::mpsc;
use crate::common::data_batch::DataBatch;

// StreamTaskActor messages
#[derive(Debug)]
pub enum StreamTaskMessage {
    Open,
    Run,
    Close,
    CreateCollector {
        channel_id: String,
        partition_type: PartitionType,
        target_operator_id: String,
    },
}

#[derive(Actor)]
pub struct StreamTaskActor {
    task: StreamTask,
}

impl StreamTaskActor {
    pub fn new(task: StreamTask) -> Self {
        Self { task }
    }
}

impl Message<StreamTaskMessage> for StreamTaskActor {
    type Reply = Result<()>;

    async fn handle(&mut self, msg: StreamTaskMessage, _ctx: &mut Context<StreamTaskActor, Result<()>>) -> Self::Reply {
        match msg {
            StreamTaskMessage::Open => {
                self.task.open().await?;
            }
            StreamTaskMessage::Run => {
                self.task.run().await?;
            }
            StreamTaskMessage::Close => {
                self.task.close().await?;
            }
            StreamTaskMessage::CreateCollector { channel_id, partition_type, target_operator_id } => {
                self.task.create_or_update_collector(channel_id, partition_type, target_operator_id)?;
            }
        }
        Ok(())
    }
}

impl Message<TransportActorMessage> for StreamTaskActor {
    type Reply = Result<()>;

    async fn handle(&mut self, msg: TransportActorMessage, _ctx: &mut Context<StreamTaskActor, Result<()>>) -> Self::Reply {
        match msg {
            TransportActorMessage::RegisterReceiver { channel_id, receiver } => {
                if let Some(reader) = &mut self.task.transport_client_mut().reader {
                    reader.register_receiver(channel_id, receiver);
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Reader not initialized"))
                }
            }
            TransportActorMessage::RegisterSender { channel_id, sender } => {
                if let Some(writer) = &mut self.task.transport_client_mut().writer {
                    writer.register_sender(channel_id, sender);
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Writer not initialized"))
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl TransportActor for StreamTaskActor {
    async fn register_receiver(&mut self, channel_id: String, receiver: mpsc::Receiver<DataBatch>) -> Result<()> {
        if let Some(reader) = &mut self.task.transport_client_mut().reader {
            reader.register_receiver(channel_id, receiver);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Reader not initialized"))
        }
    }

    async fn register_sender(&mut self, channel_id: String, sender: mpsc::Sender<DataBatch>) -> Result<()> {
        if let Some(writer) = &mut self.task.transport_client_mut().writer {
            writer.register_sender(channel_id, sender);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Writer not initialized"))
        }
    }
}

// TransportBackendActor messages
#[derive(Debug)]
pub enum TransportBackendMessage {
    Start,
    Close,
    RegisterChannel {
        vertex_id: String,
        channel: Channel,
        is_input: bool,
    },
    RegisterActor {
        vertex_id: String,
        actor: TransportActorType,
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

impl Message<TransportBackendMessage> for TransportBackendActor {
    type Reply = Result<()>;

    async fn handle(&mut self, msg: TransportBackendMessage, _ctx: &mut Context<TransportBackendActor, Result<()>>) -> Self::Reply {
        match msg {
            TransportBackendMessage::Start => {
                self.backend.start().await?;
            }
            TransportBackendMessage::Close => {
                self.backend.close().await?;
            }
            TransportBackendMessage::RegisterChannel { vertex_id, channel, is_input } => {
                self.backend.register_channel(vertex_id, channel, is_input).await?;
            }
            TransportBackendMessage::RegisterActor { vertex_id, actor } => {
                self.backend.register_actor(vertex_id, actor).await?;
            }
        }
        Ok(())
    }
} 