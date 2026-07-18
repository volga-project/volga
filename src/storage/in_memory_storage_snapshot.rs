use std::collections::HashMap;

use arrow::record_batch::RecordBatch;

use crate::common::message::Message;

#[derive(Debug, Clone, Default)]
pub struct InMemoryStorageSnapshot {
    vector_messages: Vec<Message>,
    keyed_messages: HashMap<String, Message>,
}

impl InMemoryStorageSnapshot {
    pub fn new(
        vector_messages: Vec<Message>,
        keyed_messages: HashMap<String, Message>,
    ) -> Self {
        Self {
            vector_messages,
            keyed_messages,
        }
    }

    pub fn messages(&self) -> &[Message] {
        &self.vector_messages
    }

    pub fn keyed_messages(&self) -> &HashMap<String, Message> {
        &self.keyed_messages
    }

    pub fn record_batches(&self) -> impl Iterator<Item = &RecordBatch> {
        self.vector_messages
            .iter()
            .chain(self.keyed_messages.values())
            .filter_map(|message| match message {
                Message::Regular(message) => Some(&message.record_batch),
                Message::Keyed(message) => Some(&message.base.record_batch),
                Message::Watermark(_) | Message::CheckpointBarrier(_) => None,
            })
    }

    pub fn row_count(&self) -> usize {
        self.record_batches().map(RecordBatch::num_rows).sum()
    }
}
