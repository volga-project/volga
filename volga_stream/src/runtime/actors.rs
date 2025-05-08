use anyhow::Result;
use kameo::Actor;
use kameo::message::{Context, Message};
use crate::runtime::task::StreamTask;
use crate::transport::transport_backend::{TransportBackend, InMemoryTransportBackend};
use crate::common::data_batch::DataBatch;
use crate::transport::channel::Channel;
use crate::transport::transport_client::{TransportClient, DataReader, DataWriter};
use std::collections::HashMap;
use crate::runtime::partition::PartitionType;

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
    RegisterClient {
        vertex_id: String,
        client: TransportClient,
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
            TransportBackendMessage::RegisterClient { vertex_id, client } => {
                self.backend.register_client(vertex_id, client).await?;
            }
        }
        Ok(())
    }
} 