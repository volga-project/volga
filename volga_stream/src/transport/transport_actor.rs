use anyhow::Result;
use tokio::sync::mpsc;
use crate::common::data_batch::DataBatch;
use async_trait::async_trait;
use kameo::Actor;
use kameo::prelude::ActorRef;
use crate::runtime::actors::StreamTaskActor;
use crate::transport::test_utils::{TestDataReaderActor, TestDataWriterActor};

#[derive(Debug)]
pub enum TransportActorType {
    StreamTask(ActorRef<StreamTaskActor>),
    TestReader(ActorRef<TestDataReaderActor>),
    TestWriter(ActorRef<TestDataWriterActor>),
}

impl TransportActorType {
    pub async fn register_receiver(&self, channel_id: String, receiver: mpsc::Receiver<DataBatch>) -> Result<()> {
        match self {
            TransportActorType::StreamTask(actor) => {
                actor.tell(crate::transport::transport_actor::TransportActorMessage::RegisterReceiver {
                    channel_id,
                    receiver,
                }).await.map_err(|e| anyhow::anyhow!("Failed to register receiver: {}", e))
            }
            TransportActorType::TestReader(actor) => {
                actor.tell(crate::transport::transport_actor::TransportActorMessage::RegisterReceiver {
                    channel_id,
                    receiver,
                }).await.map_err(|e| anyhow::anyhow!("Failed to register receiver: {}", e))
            }
            TransportActorType::TestWriter(_) => {
                Err(anyhow::anyhow!("Writer actor does not support receiver registration"))
            }
        }
    }

    pub async fn register_sender(&self, channel_id: String, sender: mpsc::Sender<DataBatch>) -> Result<()> {
        match self {
            TransportActorType::StreamTask(actor) => {
                actor.tell(crate::transport::transport_actor::TransportActorMessage::RegisterSender {
                    channel_id,
                    sender,
                }).await.map_err(|e| anyhow::anyhow!("Failed to register sender: {}", e))
            }
            TransportActorType::TestWriter(actor) => {
                actor.tell(crate::transport::transport_actor::TransportActorMessage::RegisterSender {
                    channel_id,
                    sender,
                }).await.map_err(|e| anyhow::anyhow!("Failed to register sender: {}", e))
            }
            TransportActorType::TestReader(_) => {
                Err(anyhow::anyhow!("Reader actor does not support sender registration"))
            }
        }
    }
}

#[async_trait]
pub trait TransportActor: Actor + Send + Sync {
    async fn register_receiver(&mut self, channel_id: String, receiver: mpsc::Receiver<DataBatch>) -> Result<()>;
    async fn register_sender(&mut self, channel_id: String, sender: mpsc::Sender<DataBatch>) -> Result<()>;
}

// Messages for transport actor registration
#[derive(Debug)]
pub enum TransportActorMessage {
    RegisterReceiver {
        channel_id: String,
        receiver: mpsc::Receiver<DataBatch>,
    },
    RegisterSender {
        channel_id: String,
        sender: mpsc::Sender<DataBatch>,
    },
} 