use anyhow::Result;
use kameo::Actor;
use kameo::message::Context;
use crate::runtime::stream_task::StreamTask;
use crate::runtime::partition::PartitionType;
use crate::transport::transport_client_actor::{TransportClientActor, TransportClientActorMessage, TransportClientActorType};
use tokio::sync::mpsc;
use crate::common::message::Message;
use async_trait::async_trait;

// StreamTaskActor messages
#[derive(Debug)]
pub enum StreamTaskMessage {
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

impl kameo::message::Message<StreamTaskMessage> for StreamTaskActor {
    type Reply = Result<()>;

    async fn handle(&mut self, msg: StreamTaskMessage, _ctx: &mut Context<StreamTaskActor, Result<()>>) -> Self::Reply {
        match msg {
            StreamTaskMessage::Run => {
                self.task.run().await?;
            }
            StreamTaskMessage::Close => {
                self.task.close().await?;
            }
            StreamTaskMessage::CreateCollector { channel_id, partition_type, target_operator_id } => {
                self.task.create_or_update_collector(channel_id, partition_type, target_operator_id).await?;
            }
        }
        Ok(())
    }
}

impl kameo::message::Message<TransportClientActorMessage> for StreamTaskActor {
    type Reply = Result<()>;

    async fn handle(&mut self, msg: TransportClientActorMessage, _ctx: &mut Context<StreamTaskActor, Result<()>>) -> Self::Reply {
        match msg {
            TransportClientActorMessage::RegisterReceiver { channel_id, receiver } => {
                self.task.register_reader_receiver(channel_id, receiver).await?;
                Ok(())
            }
            TransportClientActorMessage::RegisterSender { channel_id, sender } => {
                self.task.register_writer_sender(channel_id, sender).await?;
                Ok(())
            }
        }
    }
}

#[async_trait]
impl TransportClientActor for StreamTaskActor {
    async fn register_receiver(&mut self, channel_id: String, receiver: mpsc::Receiver<Message>) -> Result<()> {
        self.task.register_reader_receiver(channel_id, receiver).await?;
        Ok(())
    }

    async fn register_sender(&mut self, channel_id: String, sender: mpsc::Sender<Message>) -> Result<()> {
        self.task.register_writer_sender(channel_id, sender).await?;
        Ok(())
    }
}