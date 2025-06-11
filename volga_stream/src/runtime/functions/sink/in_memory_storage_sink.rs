use arrow::array::{Float64Array, StringArray};
use async_trait::async_trait;
use anyhow::Result;
use std::fmt;
use crate::common::message::{Message, KeyedMessage, BaseMessage};
use crate::runtime::storage::in_memory_storage_actor::{InMemoryStorageActor, InMemoryStorageMessage, InMemoryStorageReply};
use kameo::prelude::ActorRef;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use std::any::Any;
use std::collections::HashMap;
use tokio::sync::Mutex;
use tokio::time::{Duration, interval};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

const BUFFER_FLUSH_INTERVAL_MS: u64 = 100;

#[derive(Debug)]
pub struct InMemoryStorageActorSinkFunction {
    storage_actor: ActorRef<InMemoryStorageActor>,
    buffer: Arc<Mutex<Vec<Message>>>,
    keyed_buffer: Arc<Mutex<HashMap<u64, Message>>>,
    flush_handle: Option<tokio::task::JoinHandle<()>>,
    running: Arc<AtomicBool>,
    runtime_context: Option<RuntimeContext>,
}

impl InMemoryStorageActorSinkFunction {
    pub fn new(storage_actor: ActorRef<InMemoryStorageActor>) -> Self {
        Self { 
            storage_actor,
            buffer: Arc::new(Mutex::new(Vec::new())),
            keyed_buffer: Arc::new(Mutex::new(HashMap::new())),
            flush_handle: None,
            running: Arc::new(AtomicBool::new(false)),
            runtime_context: None,
        }
    }

    async fn flush_buffers(
        storage_actor: &ActorRef<InMemoryStorageActor>,
        buffer: &Arc<Mutex<Vec<Message>>>,
        keyed_buffer: &Arc<Mutex<HashMap<u64, Message>>>,
        vertex_id: Option<&str>,
    ) -> Result<()> {
        // Flush regular batches
        let mut regular_batches = buffer.lock().await;
        if !regular_batches.is_empty() {
            let batches: Vec<Message> = regular_batches.drain(..).collect();
            storage_actor.ask(InMemoryStorageMessage::AppendMany { messages: batches.clone() }).await?;
        }

        // Flush keyed batches
        let mut keyed_batches = keyed_buffer.lock().await;
        if !keyed_batches.is_empty() {
            let batches: HashMap<String, Message> = keyed_batches.drain()
                .map(|(key, batch)| (key.to_string(), batch))
                .collect();
            storage_actor.ask(InMemoryStorageMessage::InsertKeyedMany { keyed_messages: batches.clone() }).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl crate::runtime::functions::sink::sink_function::SinkFunctionTrait for InMemoryStorageActorSinkFunction {
    async fn sink(&mut self, message: Message) -> Result<()> {
        let r = message.record_batch();
        let v = r.column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0);
        let vertex_id = self.runtime_context.as_ref().map(|ctx| ctx.vertex_id()).unwrap();
        println!("{:?} sink rcvd, value {:?}", vertex_id, v);

        match &message {
            Message::Keyed(keyed_message) => {
                let key = keyed_message.key().hash();
                let mut keyed_buffer = self.keyed_buffer.lock().await;
                keyed_buffer.insert(key, message);
            }
            _ => {
                let mut buffer = self.buffer.lock().await;
                buffer.push(message);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl FunctionTrait for InMemoryStorageActorSinkFunction {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.runtime_context = Some(context.clone());
        self.running.store(true, Ordering::SeqCst);
        
        // Start periodic flush task
        let storage_actor = self.storage_actor.clone();
        let buffer = self.buffer.clone();
        let keyed_buffer = self.keyed_buffer.clone();
        let running = self.running.clone();
        let vertex_id = context.vertex_id().to_string();
        
        self.flush_handle = Some(tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(BUFFER_FLUSH_INTERVAL_MS));
            while running.load(Ordering::SeqCst) {
                interval.tick().await;
                if let Err(e) = InMemoryStorageActorSinkFunction::flush_buffers(&storage_actor, &buffer, &keyed_buffer, Some(&vertex_id)).await {
                    panic!("Error flushing buffers: {:?}", e);
                }
            }
        }));
        
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);
        
        // Wait for flush task to complete
        if let Some(handle) = self.flush_handle.take() {
            handle.await?;
        }
        
        // Final flush
        let vertex_id = self.runtime_context.as_ref().map(|ctx| ctx.vertex_id());
        Self::flush_buffers(&self.storage_actor, &self.buffer, &self.keyed_buffer, vertex_id.as_deref()).await?;
    
        Ok(())
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
    
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::test_utils::create_test_string_batch;
    use crate::common::Key;
    use tokio::runtime::Runtime;
    use kameo::spawn;
    use arrow::array::StringArray;
    use crate::common::message::{KeyedMessage, BaseMessage};
    use crate::runtime::storage::in_memory_storage_actor::{InMemoryStorageMessage, InMemoryStorageReply};
    use crate::runtime::functions::sink::sink_function::SinkFunctionTrait;

    #[test]
    fn test_in_memory_storage_actor_sink_function() -> Result<()> {
        // Create runtime for async operations
        let runtime = Runtime::new()?;

        // Create test data
        let regular_messages = vec![
            Message::new(None, create_test_string_batch(vec!["regular1".to_string()]), None),
            Message::new(None, create_test_string_batch(vec!["regular2".to_string()]), None),
            Message::new(None, create_test_string_batch(vec!["regular3".to_string()]), None),
        ];

        // Create keyed messages
        let key1_batch = create_test_string_batch(vec!["key1".to_string()]);
        let key2_batch = create_test_string_batch(vec!["key2".to_string()]);
        let key1 = Key::new(key1_batch.clone())?;
        let key2 = Key::new(key2_batch.clone())?;
        let keyed_messages = vec![
            Message::Keyed(KeyedMessage::new(
                BaseMessage::new(None, create_test_string_batch(vec!["value1".to_string()]), None),
                key1.clone(),
            )),
            Message::Keyed(KeyedMessage::new(
                BaseMessage::new(None, create_test_string_batch(vec!["value2".to_string()]), None),
                key2.clone(),
            )),
        ];

        // Create storage actor
        let storage_actor = InMemoryStorageActor::new();
        let storage_ref = runtime.block_on(async {
            spawn(storage_actor)
        });
        // Create sink function
        let mut sink_function = InMemoryStorageActorSinkFunction::new(storage_ref.clone());

        // Open sink function
        runtime.block_on(async {
            let context = RuntimeContext::new(
                "test_sink".to_string(),
                0,
                1,
                None,
            );
            sink_function.open(&context).await?;

            // Send regular messages
            for message in regular_messages {
                sink_function.sink(message).await?;
            }

            // Send keyed messages
            for message in keyed_messages {
                sink_function.sink(message).await?;
            }

            // Wait for data to be flushed
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

            // Close sink function
            sink_function.close().await?;

            // Verify results
            let vector_reply = storage_ref.ask(InMemoryStorageMessage::GetVector).await?;
            let map_reply = storage_ref.ask(InMemoryStorageMessage::GetMap).await?;

            match (vector_reply, map_reply) {
                (InMemoryStorageReply::Vector(vector), InMemoryStorageReply::Map(map)) => {
                    // Check regular messages
                    assert_eq!(vector.len(), 3);
                    assert_eq!(vector[0].record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "regular1");
                    assert_eq!(vector[1].record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "regular2");
                    assert_eq!(vector[2].record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "regular3");

                    // Check keyed messages
                    assert_eq!(map.len(), 2);
                    let key1_hash = key1.hash();
                    let key2_hash = key2.hash();
                    assert_eq!(map.get(&key1_hash.to_string()).unwrap().record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "value1");
                    assert_eq!(map.get(&key2_hash.to_string()).unwrap().record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "value2");
                }
                _ => panic!("Unexpected reply types"),
            }

            Ok::<_, anyhow::Error>(())
        })?;

        Ok(())
    }
} 