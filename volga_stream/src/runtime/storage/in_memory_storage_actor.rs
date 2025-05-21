use anyhow::Result;
use kameo::{Actor, message::{Context, Message}};
use crate::common::data_batch::DataBatch;
use std::collections::HashMap;

#[derive(Debug)]
pub enum InMemoryStorageMessage {
    Append {
        batch: DataBatch,
    },
    AppendBatches {
        batches: Vec<DataBatch>,
    },
    Insert {
        key: String,
        batch: DataBatch,
    },
    InsertKeyedBatches {
        keyed_batches: HashMap<String, DataBatch>,
    },
    GetByIndex {
        index: usize,
    },
    GetByKey {
        key: String,
    },
    GetVector,
    GetMap,
}

#[derive(Debug)]
pub enum InMemoryStorageReply {
    None,
    SingleBatch(DataBatch),
    Vector(Vec<DataBatch>),
    Map(HashMap<String, DataBatch>),
}

#[derive(Actor)]
pub struct InMemoryStorageActor {
    vector_storage: Vec<DataBatch>,
    map_storage: HashMap<String, DataBatch>,
}

impl InMemoryStorageActor {
    pub fn new() -> Self {
        Self {
            vector_storage: Vec::new(),
            map_storage: HashMap::new(),
        }
    }
}

impl Message<InMemoryStorageMessage> for InMemoryStorageActor {
    type Reply = Result<InMemoryStorageReply>;

    async fn handle(&mut self, msg: InMemoryStorageMessage, _ctx: &mut Context<InMemoryStorageActor, Result<InMemoryStorageReply>>) -> Self::Reply {
        match msg {
            InMemoryStorageMessage::Append { batch } => {
                self.vector_storage.push(batch);
                Ok(InMemoryStorageReply::None)
            }
            InMemoryStorageMessage::AppendBatches { batches } => {
                self.vector_storage.extend(batches);
                Ok(InMemoryStorageReply::None)
            }
            InMemoryStorageMessage::Insert { key, batch } => {
                self.map_storage.insert(key, batch);
                Ok(InMemoryStorageReply::None)
            }
            InMemoryStorageMessage::InsertKeyedBatches { keyed_batches } => {
                self.map_storage.extend(keyed_batches);
                Ok(InMemoryStorageReply::None)
            }
            InMemoryStorageMessage::GetByIndex { index } => {
                Ok(self.vector_storage.get(index)
                    .map(|batch| InMemoryStorageReply::SingleBatch(batch.clone()))
                    .unwrap_or(InMemoryStorageReply::None))
            }
            InMemoryStorageMessage::GetByKey { key } => {
                Ok(self.map_storage.get(&key)
                    .map(|batch| InMemoryStorageReply::SingleBatch(batch.clone()))
                    .unwrap_or(InMemoryStorageReply::None))
            }
            InMemoryStorageMessage::GetVector => {
                Ok(InMemoryStorageReply::Vector(self.vector_storage.clone()))
            }
            InMemoryStorageMessage::GetMap => {
                Ok(InMemoryStorageReply::Map(self.map_storage.clone()))
            }
        }
    }
} 