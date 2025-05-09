use anyhow::Result;
use kameo::Actor;
use kameo::message::{Context, Message};
use crate::common::data_batch::DataBatch;
use crate::transport::transport_client::{DataReader, DataWriter};
use crate::transport::transport_client_actor::{TransportClientActor, TransportClientActorMessage};
use tokio::sync::mpsc;
use async_trait::async_trait;

// TestDataReaderActor messages
#[derive(Debug)]
pub enum TestDataReaderMessage {
    RegisterReceiver {
        channel_id: String,
        receiver: mpsc::Receiver<DataBatch>,
    },
    ReadBatch,
}

#[derive(Actor)]
pub struct TestDataReaderActor {
    reader: DataReader,
}

impl TestDataReaderActor {
    pub fn new(vertex_id: String) -> Self {
        Self {
            reader: DataReader::new(vertex_id),
        }
    }
}

impl Message<TestDataReaderMessage> for TestDataReaderActor {
    type Reply = Result<Option<DataBatch>>;

    async fn handle(&mut self, msg: TestDataReaderMessage, _ctx: &mut Context<TestDataReaderActor, Result<Option<DataBatch>>>) -> Self::Reply {
        match msg {
            TestDataReaderMessage::RegisterReceiver { channel_id, receiver } => {
                self.reader.register_receiver(channel_id, receiver);
                Ok(None)
            }
            TestDataReaderMessage::ReadBatch => {
                self.reader.read_batch().await
            }
        }
    }
}

impl Message<TransportClientActorMessage> for TestDataReaderActor {
    type Reply = Result<()>;

    async fn handle(&mut self, msg: TransportClientActorMessage, _ctx: &mut Context<TestDataReaderActor, Result<()>>) -> Self::Reply {
        match msg {
            TransportClientActorMessage::RegisterReceiver { channel_id, receiver } => {
                self.reader.register_receiver(channel_id, receiver);
                Ok(())
            }
            TransportClientActorMessage::RegisterSender { .. } => {
                Err(anyhow::anyhow!("TestDataReaderActor does not support sender registration"))
            }
        }
    }
}

#[async_trait]
impl TransportClientActor for TestDataReaderActor {
    async fn register_receiver(&mut self, channel_id: String, receiver: mpsc::Receiver<DataBatch>) -> Result<()> {
        self.reader.register_receiver(channel_id, receiver);
        Ok(())
    }

    async fn register_sender(&mut self, _channel_id: String, _sender: mpsc::Sender<DataBatch>) -> Result<()> {
        Err(anyhow::anyhow!("TestDataReaderActor does not support sender registration"))
    }
}

// TestDataWriterActor messages
#[derive(Debug)]
pub enum TestDataWriterMessage {
    RegisterSender {
        channel_id: String,
        sender: mpsc::Sender<DataBatch>,
    },
    WriteBatch {
        channel_id: String,
        batch: DataBatch,
    },
}

#[derive(Actor)]
pub struct TestDataWriterActor {
    writer: DataWriter,
}

impl TestDataWriterActor {
    pub fn new(vertex_id: String) -> Self {
        Self {
            writer: DataWriter::new(vertex_id),
        }
    }
}

impl Message<TestDataWriterMessage> for TestDataWriterActor {
    type Reply = Result<()>;

    async fn handle(&mut self, msg: TestDataWriterMessage, _ctx: &mut Context<TestDataWriterActor, Result<()>>) -> Self::Reply {
        match msg {
            TestDataWriterMessage::RegisterSender { channel_id, sender } => {
                self.writer.register_sender(channel_id, sender);
                Ok(())
            }
            TestDataWriterMessage::WriteBatch { channel_id, batch } => {
                self.writer.write_batch(&channel_id, batch).await
            }
        }
    }
}

impl Message<TransportClientActorMessage> for TestDataWriterActor {
    type Reply = Result<()>;

    async fn handle(&mut self, msg: TransportClientActorMessage, _ctx: &mut Context<TestDataWriterActor, Result<()>>) -> Self::Reply {
        match msg {
            TransportClientActorMessage::RegisterSender { channel_id, sender } => {
                self.writer.register_sender(channel_id, sender);
                Ok(())
            }
            TransportClientActorMessage::RegisterReceiver { .. } => {
                Err(anyhow::anyhow!("TestDataWriterActor does not support receiver registration"))
            }
        }
    }
}

#[async_trait]
impl TransportClientActor for TestDataWriterActor {
    async fn register_receiver(&mut self, _channel_id: String, _receiver: mpsc::Receiver<DataBatch>) -> Result<()> {
        Err(anyhow::anyhow!("TestDataWriterActor does not support receiver registration"))
    }

    async fn register_sender(&mut self, channel_id: String, sender: mpsc::Sender<DataBatch>) -> Result<()> {
        self.writer.register_sender(channel_id, sender);
        Ok(())
    }
} 