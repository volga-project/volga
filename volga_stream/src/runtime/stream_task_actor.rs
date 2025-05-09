use anyhow::Result;
use kameo::Actor;
use kameo::message::{Context, Message};
use crate::runtime::stream_task::StreamTask;
use crate::transport::transport_backend::{TransportBackend, InMemoryTransportBackend};
use crate::transport::channel::Channel;
use crate::runtime::partition::PartitionType;
use crate::transport::transport_client_actor::{TransportClientActor, TransportClientActorMessage, TransportClientActorType};
use tokio::sync::mpsc;
use crate::common::data_batch::DataBatch;
use async_trait::async_trait;

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

impl Message<TransportClientActorMessage> for StreamTaskActor {
    type Reply = Result<()>;

    async fn handle(&mut self, msg: TransportClientActorMessage, _ctx: &mut Context<StreamTaskActor, Result<()>>) -> Self::Reply {
        match msg {
            TransportClientActorMessage::RegisterReceiver { channel_id, receiver } => {
                if let Some(reader) = &mut self.task.transport_client_mut().reader {
                    reader.register_receiver(channel_id, receiver);
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Reader not initialized"))
                }
            }
            TransportClientActorMessage::RegisterSender { channel_id, sender } => {
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

#[async_trait]
impl TransportClientActor for StreamTaskActor {
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