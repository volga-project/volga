use anyhow::Result;
use kameo::{Actor, message::Context};
use crate::common::message::Message;
use std::collections::HashMap;

#[derive(Debug)]
pub enum InMemoryStorageMessage {
    Append {
        message: Message,
    },
    AppendMany {
        messages: Vec<Message>,
    },
    Insert {
        key: String,
        message: Message,
    },
    InsertKeyedMany {
        keyed_messages: HashMap<String, Message>,
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
    Single(Message),
    Vector(Vec<Message>),
    Map(HashMap<String, Message>),
}

#[derive(Actor)]
pub struct InMemoryStorageActor {
    vector_storage: Vec<Message>,
    map_storage: HashMap<String, Message>,
}

impl InMemoryStorageActor {
    pub fn new() -> Self {
        Self {
            vector_storage: Vec::new(),
            map_storage: HashMap::new(),
        }
    }
}

impl kameo::message::Message<InMemoryStorageMessage> for InMemoryStorageActor {
    type Reply = Result<InMemoryStorageReply>;

    async fn handle(&mut self, msg: InMemoryStorageMessage, _ctx: &mut Context<InMemoryStorageActor, Result<InMemoryStorageReply>>) -> Self::Reply {
        match msg {
            InMemoryStorageMessage::Append { message } => {
                self.vector_storage.push(message);
                Ok(InMemoryStorageReply::None)
            }
            InMemoryStorageMessage::AppendMany { messages } => {
                self.vector_storage.extend(messages);
                Ok(InMemoryStorageReply::None)
            }
            InMemoryStorageMessage::Insert { key, message } => {
                self.map_storage.insert(key, message);
                Ok(InMemoryStorageReply::None)
            }
            InMemoryStorageMessage::InsertKeyedMany { keyed_messages } => {
                self.map_storage.extend(keyed_messages);
                Ok(InMemoryStorageReply::None)
            }
            InMemoryStorageMessage::GetByIndex { index } => {
                Ok(self.vector_storage.get(index)
                    .map(|message| InMemoryStorageReply::Single(message.clone()))
                    .unwrap_or(InMemoryStorageReply::None))
            }
            InMemoryStorageMessage::GetByKey { key } => {
                Ok(self.map_storage.get(&key)
                    .map(|message| InMemoryStorageReply::Single(message.clone()))
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