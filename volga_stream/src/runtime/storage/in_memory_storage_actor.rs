use anyhow::Result;
use kameo::{Actor, message::{Context, Message}};
use crate::common::data_batch::DataBatch;
use std::collections::HashMap;

#[derive(Debug)]
pub enum InMemoryStorageMessage {
    Append {
        batch: DataBatch,
    },
    Insert {
        key: String,
        batch: DataBatch,
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
    type Reply = Result<Option<DataBatch>>;

    async fn handle(&mut self, msg: InMemoryStorageMessage, _ctx: &mut Context<InMemoryStorageActor, Result<Option<DataBatch>>>) -> Self::Reply {
        match msg {
            InMemoryStorageMessage::Append { batch } => {
                self.vector_storage.push(batch);
                Ok(None)
            }
            InMemoryStorageMessage::Insert { key, batch } => {
                self.map_storage.insert(key, batch);
                Ok(None)
            }
            InMemoryStorageMessage::GetByIndex { index } => {
                Ok(self.vector_storage.get(index).cloned())
            }
            InMemoryStorageMessage::GetByKey { key } => {
                Ok(self.map_storage.get(&key).cloned())
            }
            InMemoryStorageMessage::GetVector => {
                // Return a copy of the vector storage
                Ok(Some(DataBatch::new(
                    None,
                    self.vector_storage.iter()
                        .flat_map(|batch| batch.record_batch().clone())
                        .collect()
                )))
            }
            InMemoryStorageMessage::GetMap => {
                // Return a copy of the map storage as a single batch
                Ok(Some(DataBatch::new(
                    None,
                    self.map_storage.values()
                        .flat_map(|batch| batch.record_batch().clone())
                        .collect()
                )))
            }
        }
    }
} 