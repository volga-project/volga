use arrow::array::StringArray;
use async_trait::async_trait;
use anyhow::Result;
use crate::common::message::Message;
use crate::runtime::functions::sink::SinkFunctionTrait;
use crate::storage::in_memory_storage_grpc_client::InMemoryStorageClient;
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
pub struct InMemoryStorageSinkFunction {
    storage_client: Option<Arc<Mutex<InMemoryStorageClient>>>,
    buffer: Arc<Mutex<Vec<Message>>>,
    keyed_buffer: Arc<Mutex<HashMap<u64, Message>>>,
    flush_handle: Option<tokio::task::JoinHandle<()>>,
    running: Arc<AtomicBool>,
    runtime_context: Option<RuntimeContext>,
    server_addr: String,
}

impl InMemoryStorageSinkFunction {
    pub fn new(server_addr: String) -> Self {
        Self { 
            storage_client: None,
            buffer: Arc::new(Mutex::new(Vec::new())),
            keyed_buffer: Arc::new(Mutex::new(HashMap::new())),
            flush_handle: None,
            running: Arc::new(AtomicBool::new(false)),
            runtime_context: None,
            server_addr,
        }
    }

    async fn flush_buffers(
        storage_client: &Arc<Mutex<InMemoryStorageClient>>,
        buffer: &Arc<Mutex<Vec<Message>>>,
        keyed_buffer: &Arc<Mutex<HashMap<u64, Message>>>,
        vertex_id: Option<&str>,
    ) -> Result<()> {
        // Flush regular batches
        let mut regular_batches = buffer.lock().await;
        if !regular_batches.is_empty() {
            let batches: Vec<Message> = regular_batches.drain(..).collect();
            let mut client = storage_client.lock().await;
            client.append_many(batches).await?;
        }

        // Flush keyed batches
        let mut keyed_batches = keyed_buffer.lock().await;
        if !keyed_batches.is_empty() {
            let batches: HashMap<String, Message> = keyed_batches.drain()
                .map(|(key, batch)| (key.to_string(), batch))
                .collect();

            let mut client = storage_client.lock().await;
            client.insert_keyed_many(batches).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl SinkFunctionTrait for InMemoryStorageSinkFunction {
    async fn sink(&mut self, message: Message) -> Result<()> {
        // let r = message.record_batch();
        // let v = r.column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0);
        // let vertex_id = self.runtime_context.as_ref().map(|ctx| ctx.vertex_id()).unwrap();
        // println!("{:?} sink rcvd, value {:?}", vertex_id, v);

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
impl FunctionTrait for InMemoryStorageSinkFunction {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.runtime_context = Some(context.clone());
        
        // Create gRPC client connection
        let client = InMemoryStorageClient::new(self.server_addr.clone()).await?;
        let shared_client = Arc::new(Mutex::new(client));
        self.storage_client = Some(shared_client.clone());
        
        self.running.store(true, Ordering::SeqCst);
        
        // Start periodic flush task
        let buffer = self.buffer.clone();
        let keyed_buffer = self.keyed_buffer.clone();
        let running = self.running.clone();
        let vertex_id = context.vertex_id().to_string();
        
        self.flush_handle = Some(tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(BUFFER_FLUSH_INTERVAL_MS));
            
            while running.load(Ordering::SeqCst) {
                interval.tick().await;
                if let Err(e) = InMemoryStorageSinkFunction::flush_buffers(&shared_client, &buffer, &keyed_buffer, Some(&vertex_id)).await {
                    eprintln!("Error flushing buffers: {:?}", e);
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
        if let Some(ref client) = self.storage_client {
            let vertex_id = self.runtime_context.as_ref().map(|ctx| ctx.vertex_id());
            Self::flush_buffers(client, &self.buffer, &self.keyed_buffer, vertex_id.as_deref()).await?;
        }
    
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
    use crate::common::test_utils::{create_test_string_batch, gen_unique_grpc_port};
    use crate::common::Key;
    use tokio::runtime::Runtime;
    use arrow::array::StringArray;
    use crate::common::message::{KeyedMessage, BaseMessage};
    use crate::storage::in_memory_storage_grpc_server::InMemoryStorageServer;

    #[test]
    fn test_in_memory_storage_grpc_sink_function() -> Result<()> {
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

        runtime.block_on(async {
            // Start storage server
            let mut storage_server = InMemoryStorageServer::new();
            let port = gen_unique_grpc_port();
            let server_addr = format!("127.0.0.1:{}", port);

            storage_server.start(&server_addr).await?;
            
            // Wait a bit for server to start
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            // Create sink function
            let mut sink_function = InMemoryStorageSinkFunction::new(format!("http://{}", server_addr));

            // Open sink function
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

            // Verify results by querying the server
            let mut client = InMemoryStorageClient::new(format!("http://{}", server_addr)).await?;
            let vector_messages = client.get_vector().await?;
            let map_messages = client.get_map().await?;

            // Check regular messages
            assert_eq!(vector_messages.len(), 3);
            assert_eq!(vector_messages[0].record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "regular1");
            assert_eq!(vector_messages[1].record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "regular2");
            assert_eq!(vector_messages[2].record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "regular3");
            println!("Vector messages ok");

            // Check keyed messages
            assert_eq!(map_messages.len(), 2);
            let key1_hash = key1.hash();
            let key2_hash = key2.hash();
            assert_eq!(map_messages.get(&key1_hash.to_string()).unwrap().record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "value1");
            assert_eq!(map_messages.get(&key2_hash.to_string()).unwrap().record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "value2");
            println!("Map messages ok");

            // Stop storage server
            storage_server.stop().await;

            Ok::<_, anyhow::Error>(())
        })?;

        Ok(())
    }
} 