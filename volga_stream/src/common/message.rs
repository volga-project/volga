use arrow::record_batch::RecordBatch;
use anyhow::Result;

use super::Key;

#[derive(Debug, Clone)]
pub struct BaseMessage {
    pub upstream_vertex_id: Option<String>,
    pub record_batch: RecordBatch,
}

impl BaseMessage {
    pub fn new(upstream_vertex_id: Option<String>, record_batch: RecordBatch) -> Self {
        Self {
            upstream_vertex_id,
            record_batch,
        }
    }
}

#[derive(Debug, Clone)]
pub struct KeyedMessage {
    pub base: BaseMessage,
    pub key: Key,
}

impl KeyedMessage {
    pub fn new(base: BaseMessage, key: Key) -> Self {
        Self {
            base,
            key,
        }
    }

    pub fn key(&self) -> &Key {
        &self.key
    }
}

#[derive(Clone, Debug)]
pub enum Message {
    Regular(BaseMessage),
    Keyed(KeyedMessage),
}

impl Message {
    pub fn new(upstream_vertex_id: Option<String>, record_batch: RecordBatch) -> Self {
        Message::Regular(BaseMessage::new(upstream_vertex_id, record_batch))
    }

    pub fn new_keyed(upstream_vertex_id: Option<String>, record_batch: RecordBatch, key: Key) -> Self {
        Message::Keyed(KeyedMessage::new(
            BaseMessage::new(upstream_vertex_id, record_batch),
            key,
        ))
    }

    pub fn upstream_vertex_id(&self) -> Option<String> {
        match self {
            Message::Regular(message) => message.upstream_vertex_id.clone(),
            Message::Keyed(message) => message.base.upstream_vertex_id.clone(),
        }
    }

    pub fn record_batch(&self) -> &RecordBatch {
        match self {
            Message::Regular(message) => &message.record_batch,
            Message::Keyed(message) => &message.base.record_batch,
        }
    }

    pub fn key(&self) -> Result<&Key> {
        match self {
            Message::Regular(_) => Err(anyhow::anyhow!("Regular message does not have a key")),
            Message::Keyed(message) => Ok(&message.key),
        }
    }
} 