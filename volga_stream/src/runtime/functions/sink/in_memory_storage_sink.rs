use async_trait::async_trait;
use anyhow::Result;
use std::fmt;
use crate::common::data_batch::{DataBatch, KeyedDataBatch, BaseDataBatch};
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
const MAX_BUFFER_SIZE: usize = 1000;

#[derive(Debug)]
pub struct InMemoryStorageActorSinkFunction {
    storage_actor: ActorRef<InMemoryStorageActor>,
    buffer: Arc<Mutex<Vec<DataBatch>>>,
    keyed_buffer: Arc<Mutex<HashMap<u64, DataBatch>>>,
    flush_handle: Option<tokio::task::JoinHandle<()>>,
    running: Arc<AtomicBool>,
}

impl InMemoryStorageActorSinkFunction {
    pub fn new(storage_actor: ActorRef<InMemoryStorageActor>) -> Self {
        Self { 
            storage_actor,
            buffer: Arc::new(Mutex::new(Vec::new())),
            keyed_buffer: Arc::new(Mutex::new(HashMap::new())),
            flush_handle: None,
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn flush_buffers(&self) -> Result<()> {
        // Flush regular batches
        let mut regular_batches = self.buffer.lock().await;
        if !regular_batches.is_empty() {
            let batches = regular_batches.drain(..).collect();
            self.storage_actor.ask(InMemoryStorageMessage::AppendBatches { batches }).await?;
        }

        // Flush keyed batches
        let mut keyed_batches = self.keyed_buffer.lock().await;
        if !keyed_batches.is_empty() {
            let batches = keyed_batches.drain()
                .map(|(key, batch)| (key.to_string(), batch))
                .collect();
            self.storage_actor.ask(InMemoryStorageMessage::InsertKeyedBatches { keyed_batches: batches }).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl crate::runtime::functions::sink::sink_function::SinkFunctionTrait for InMemoryStorageActorSinkFunction {
    async fn sink(&mut self, batch: DataBatch) -> Result<()> {
        match &batch {
            DataBatch::KeyedBatch(keyed_batch) => {
                let key = keyed_batch.key().hash();
                let mut keyed_buffer = self.keyed_buffer.lock().await;
                keyed_buffer.insert(key, batch);
                
                // If buffer is full, flush it
                if keyed_buffer.len() >= MAX_BUFFER_SIZE {
                    let batches = keyed_buffer.drain()
                        .map(|(key, batch)| (key.to_string(), batch))
                        .collect();
                    self.storage_actor.ask(InMemoryStorageMessage::InsertKeyedBatches { keyed_batches: batches }).await?;
                }
            }
            _ => {
                let mut buffer = self.buffer.lock().await;
                buffer.push(batch);
                
                // If buffer is full, flush it
                if buffer.len() >= MAX_BUFFER_SIZE {
                    let batches = buffer.drain(..).collect();
                    self.storage_actor.ask(InMemoryStorageMessage::AppendBatches { batches }).await?;
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl FunctionTrait for InMemoryStorageActorSinkFunction {
    async fn open(&mut self, _context: &RuntimeContext) -> Result<()> {
        self.running.store(true, Ordering::SeqCst);
        
        // Start periodic flush task
        let buffer = self.buffer.clone();
        let keyed_buffer = self.keyed_buffer.clone();
        let storage_actor = self.storage_actor.clone();
        let running = self.running.clone();
        
        let handle = tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(BUFFER_FLUSH_INTERVAL_MS));
            
            while running.load(Ordering::SeqCst) {
                interval.tick().await;
                
                // Flush regular batches
                let mut regular_batches = buffer.lock().await;
                if !regular_batches.is_empty() {
                    let batches = regular_batches.drain(..).collect();
                    if let Err(e) = storage_actor.ask(InMemoryStorageMessage::AppendBatches { batches }).await {
                        eprintln!("Error flushing regular batches: {}", e);
                    }
                }

                // Flush keyed batches
                let mut keyed_batches = keyed_buffer.lock().await;
                if !keyed_batches.is_empty() {
                    let batches = keyed_batches.drain()
                        .map(|(key, batch)| (key.to_string(), batch))
                        .collect();
                    if let Err(e) = storage_actor.ask(InMemoryStorageMessage::InsertKeyedBatches { keyed_batches: batches }).await {
                        eprintln!("Error flushing keyed batches: {}", e);
                    }
                }
            }
        });
        
        self.flush_handle = Some(handle);
        Ok(())
    }
    
    async fn close(&mut self) -> Result<()> {
        // Stop the flush task
        self.running.store(false, Ordering::SeqCst);
        
        // Wait for the task to finish
        if let Some(handle) = self.flush_handle.take() {
            if let Err(e) = handle.await {
                eprintln!("Error waiting for flush task to finish: {}", e);
            }
        }
        
        // Flush any remaining batches
        self.flush_buffers().await?;
        
        Ok(())
    }
    
    async fn finish(&mut self) -> Result<()> {
        // Same as close for this implementation
        self.close().await
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
    use crate::common::data_batch::{KeyedDataBatch, BaseDataBatch};
    use crate::runtime::storage::in_memory_storage_actor::{InMemoryStorageMessage, InMemoryStorageReply};
    use crate::runtime::functions::sink::sink_function::SinkFunctionTrait;

    #[test]
    fn test_in_memory_storage_actor_sink_function() -> Result<()> {
        // Create runtime for async operations
        let runtime = Runtime::new()?;

        // Create test data
        let regular_batches = vec![
            DataBatch::new(None, create_test_string_batch(vec!["regular1".to_string()])?),
            DataBatch::new(None, create_test_string_batch(vec!["regular2".to_string()])?),
            DataBatch::new(None, create_test_string_batch(vec!["regular3".to_string()])?),
        ];

        // Create keyed batches
        let key1_batch = create_test_string_batch(vec!["key1".to_string()])?;
        let key2_batch = create_test_string_batch(vec!["key2".to_string()])?;
        let key1 = Key::new(key1_batch.clone())?;
        let key2 = Key::new(key2_batch.clone())?;
        let keyed_batches = vec![
            DataBatch::KeyedBatch(KeyedDataBatch::new(
                BaseDataBatch::new(None, create_test_string_batch(vec!["value1".to_string()])?),
                key1.clone(),
            )),
            DataBatch::KeyedBatch(KeyedDataBatch::new(
                BaseDataBatch::new(None, create_test_string_batch(vec!["value2".to_string()])?),
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

            // Send regular batches
            for batch in regular_batches {
                sink_function.sink(batch).await?;
            }

            // Send keyed batches
            for batch in keyed_batches {
                sink_function.sink(batch).await?;
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
                    // Check regular batches
                    assert_eq!(vector.len(), 3);
                    assert_eq!(vector[0].record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "regular1");
                    assert_eq!(vector[1].record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "regular2");
                    assert_eq!(vector[2].record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "regular3");

                    // Check keyed batches
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