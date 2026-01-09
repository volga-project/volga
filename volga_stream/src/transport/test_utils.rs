use anyhow::{Ok, Result};
use futures::StreamExt;
use kameo::Actor;
use kameo::message::Context;
use crate::common::message::Message;
use crate::runtime::VertexId;
use crate::runtime::operators::operator::MessageStream;
use crate::transport::channel::Channel;
use crate::transport::transport_client::{DataReader, DataWriter};

use super::transport_client::TransportClientConfig;

#[derive(Debug)]
pub enum TestDataReaderMessage {
    ReadMessage,
}

#[derive(Actor)]
pub struct TestDataReaderActor {
    reader_message_stream: MessageStream,
}

impl TestDataReaderActor {
    pub fn new(vertex_id: VertexId, transport_client_config: TransportClientConfig) -> Self {
        let reader = DataReader::new(vertex_id, transport_client_config.reader_receivers.unwrap());
        let (reader_message_stream, _control) = reader.message_stream_with_control();
        Self {
            reader_message_stream,
        }
    }
}

impl kameo::message::Message<TestDataReaderMessage> for TestDataReaderActor {
    type Reply = Result<Option<Message>>;

    async fn handle(&mut self, msg: TestDataReaderMessage, _ctx: &mut Context<TestDataReaderActor, Result<Option<Message>>>) -> Self::Reply {
        match msg {
            TestDataReaderMessage::ReadMessage => {
                Ok(self.reader_message_stream.next().await)
            }
        }
    }
}

#[derive(Debug)]
pub enum TestDataWriterMessage {
    WriteMessage {
        channel: Channel,
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
    pub fn new(vertex_id: VertexId, transport_client_config: TransportClientConfig) -> Self {
        Self {
            writer: DataWriter::new(
                vertex_id,
                transport_client_config.writer_senders.unwrap(),
                transport_client_config.metrics_labels,
            ),
        }
    }
}

impl kameo::message::Message<TestDataWriterMessage> for TestDataWriterActor {
    type Reply = Result<()>;

    async fn handle(&mut self, msg: TestDataWriterMessage, _ctx: &mut Context<TestDataWriterActor, Result<()>>) -> Self::Reply {
        match msg {
            TestDataWriterMessage::WriteMessage { channel, message } => {
                let (success, _) = self.writer.write_message(&channel, &message).await;
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
