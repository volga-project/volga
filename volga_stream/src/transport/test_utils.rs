use anyhow::Result;
use kameo::Actor;
use kameo::message::{Context, Message};
use crate::common::data_batch::DataBatch;
use crate::transport::transport_client::{DataReader, DataWriter};
use tokio::sync::mpsc;

// DataReaderActor messages
#[derive(Debug)]
pub enum DataReaderMessage {
    RegisterReceiver {
        channel_id: String,
        receiver: mpsc::Receiver<DataBatch>,
    },
    ReadBatch,
}

#[derive(Actor)]
pub struct DataReaderActor {
    reader: DataReader,
}

impl DataReaderActor {
    pub fn new(vertex_id: String) -> Self {
        Self {
            reader: DataReader::new(vertex_id),
        }
    }
}

impl Message<DataReaderMessage> for DataReaderActor {
    type Reply = Result<Option<DataBatch>>;

    async fn handle(&mut self, msg: DataReaderMessage, _ctx: &mut Context<DataReaderActor, Result<Option<DataBatch>>>) -> Self::Reply {
        match msg {
            DataReaderMessage::RegisterReceiver { channel_id, receiver } => {
                self.reader.register_receiver(channel_id, receiver);
                Ok(None)
            }
            DataReaderMessage::ReadBatch => {
                self.reader.read_batch().await
            }
        }
    }
}

// DataWriterActor messages
#[derive(Debug)]
pub enum DataWriterMessage {
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
pub struct DataWriterActor {
    writer: DataWriter,
}

impl DataWriterActor {
    pub fn new(vertex_id: String) -> Self {
        Self {
            writer: DataWriter::new(vertex_id),
        }
    }
}

impl Message<DataWriterMessage> for DataWriterActor {
    type Reply = Result<()>;

    async fn handle(&mut self, msg: DataWriterMessage, _ctx: &mut Context<DataWriterActor, Result<()>>) -> Self::Reply {
        match msg {
            DataWriterMessage::RegisterSender { channel_id, sender } => {
                self.writer.register_sender(channel_id, sender);
                Ok(())
            }
            DataWriterMessage::WriteBatch { channel_id, batch } => {
                self.writer.write_batch(&channel_id, batch).await
            }
        }
    }
} 