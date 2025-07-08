use anyhow::{Ok, Result};
use kameo::Actor;
use kameo::message::Context;
use crate::common::message::Message;
use crate::transport::transport_client::{DataReader, DataWriter};

use super::transport_client::TransportClientConfig;

#[derive(Debug)]
pub enum TestDataReaderMessage {
    ReadMessage,
}

#[derive(Actor)]
pub struct TestDataReaderActor {
    reader: DataReader,
}

impl TestDataReaderActor {
    pub fn new(vertex_id: String, transport_client_config: TransportClientConfig) -> Self {
        Self {
            reader: DataReader::new(vertex_id, transport_client_config.reader_receivers.unwrap()),
        }
    }
}

impl kameo::message::Message<TestDataReaderMessage> for TestDataReaderActor {
    type Reply = Result<Option<Message>>;

    async fn handle(&mut self, msg: TestDataReaderMessage, _ctx: &mut Context<TestDataReaderActor, Result<Option<Message>>>) -> Self::Reply {
        match msg {
            TestDataReaderMessage::ReadMessage => {
                self.reader.read_message().await
            }
        }
    }
}

#[derive(Debug)]
pub enum TestDataWriterMessage {
    WriteMessage {
        channel_id: String,
        message: Message,
    },
    Start,
    FlushAndClose
}

#[derive(Actor)]
pub struct TestDataWriterActor {
    writer: DataWriter,
}

impl TestDataWriterActor {
    pub fn new(vertex_id: String, transport_client_config: TransportClientConfig) -> Self {
        Self {
            writer: DataWriter::new(vertex_id, transport_client_config.writer_senders.unwrap()),
        }
    }
}

impl kameo::message::Message<TestDataWriterMessage> for TestDataWriterActor {
    type Reply = Result<()>;

    async fn handle(&mut self, msg: TestDataWriterMessage, _ctx: &mut Context<TestDataWriterActor, Result<()>>) -> Self::Reply {
        match msg {
            TestDataWriterMessage::WriteMessage { channel_id, message } => {
                let (success, _) = self.writer.write_message(&channel_id, &message).await;
                if success {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Failed to write message"))
                }
            },
            TestDataWriterMessage::Start => {
                self.writer.start().await;
                Ok(())
            },
            TestDataWriterMessage::FlushAndClose => {
                self.writer.flush_and_close().await.unwrap();
                Ok(())
            }
        }
    }
}
