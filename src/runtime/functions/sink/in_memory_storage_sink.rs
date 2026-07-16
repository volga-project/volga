use async_trait::async_trait;
use anyhow::Result;
use arrow::array::{Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::DataType;
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
    dedup_buffer: Arc<Mutex<HashMap<String, Message>>>,
    flush_handle: Option<tokio::task::JoinHandle<()>>,
    running: Arc<AtomicBool>,
    runtime_context: Option<RuntimeContext>,
    server_addr: String,
    dedup: bool,
}

impl InMemoryStorageSinkFunction {
    pub fn new(server_addr: String, dedup: bool) -> Self {
        Self {
            storage_client: None,
            buffer: Arc::new(Mutex::new(Vec::new())),
            keyed_buffer: Arc::new(Mutex::new(HashMap::new())),
            dedup_buffer: Arc::new(Mutex::new(HashMap::new())),
            flush_handle: None,
            running: Arc::new(AtomicBool::new(false)),
            runtime_context: None,
            server_addr,
            dedup,
        }
    }

    fn extract_dedup_rows(message: &Message) -> Result<Vec<(String, Message)>> {
        let batch = message.record_batch();
        let schema = batch.schema();
        let key_idx = ["key", "partition_key"]
            .iter()
            .find_map(|name| schema.index_of(name).ok())
            .ok_or_else(|| anyhow::anyhow!("dedup sink requires key or partition_key column"))?;
        let ts_idx = ["timestamp", "ts"]
            .iter()
            .find_map(|name| schema.index_of(name).ok())
            .ok_or_else(|| anyhow::anyhow!("dedup sink requires timestamp or ts column"))?;

        let key_col = batch.column(key_idx);
        let ts_col = batch.column(ts_idx);
        let mut rows = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            let key = match key_col.data_type() {
                DataType::Utf8 => key_col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row_idx)
                    .to_string(),
                other => {
                    return Err(anyhow::anyhow!(
                        "dedup key column must be Utf8, got {:?}",
                        other
                    ))
                }
            };
            let ts = match ts_col.data_type() {
                DataType::Timestamp(_, _) => ts_col
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .value(row_idx),
                DataType::Int64 => {
                    use arrow::array::Int64Array;
                    ts_col
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row_idx)
                }
                other => {
                    return Err(anyhow::anyhow!(
                        "dedup timestamp column must be Timestamp or Int64, got {:?}",
                        other
                    ))
                }
            };
            let single = batch.slice(row_idx, 1);
            let row_message = Message::new(
                message.upstream_vertex_id(),
                single,
                message.ingest_timestamp(),
                None,
            );
            rows.push((format!("{key}|{ts}"), row_message));
        }
        Ok(rows)
    }

    async fn flush_buffers(
        storage_client: &Arc<Mutex<InMemoryStorageClient>>,
        buffer: &Arc<Mutex<Vec<Message>>>,
        keyed_buffer: &Arc<Mutex<HashMap<u64, Message>>>,
        dedup_buffer: &Arc<Mutex<HashMap<String, Message>>>,
        dedup: bool,
    ) -> Result<()> {
        if dedup {
            let mut rows = dedup_buffer.lock().await;
            if !rows.is_empty() {
                let payload: HashMap<String, Message> = rows.drain().collect();
                let mut client = storage_client.lock().await;
                client.upsert_dedup_rows(payload).await?;
            }
            return Ok(());
        }

        let mut regular_batches = buffer.lock().await;
        if !regular_batches.is_empty() {
            let batches: Vec<Message> = regular_batches.drain(..).collect();
            let mut client = storage_client.lock().await;
            client.append_many(batches).await?;
        }

        let mut keyed_batches = keyed_buffer.lock().await;
        if !keyed_batches.is_empty() {
            let batches: HashMap<String, Message> = keyed_batches
                .drain()
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
        if self.dedup {
            let rows = Self::extract_dedup_rows(&message)?;
            let mut dedup_buffer = self.dedup_buffer.lock().await;
            for (key, row) in rows {
                dedup_buffer.insert(key, row);
            }
            return Ok(());
        }

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

        let client = InMemoryStorageClient::new(self.server_addr.clone()).await?;
        let shared_client = Arc::new(Mutex::new(client));
        self.storage_client = Some(shared_client.clone());

        self.running.store(true, Ordering::SeqCst);

        let buffer = self.buffer.clone();
        let keyed_buffer = self.keyed_buffer.clone();
        let dedup_buffer = self.dedup_buffer.clone();
        let running = self.running.clone();
        let dedup = self.dedup;
        self.flush_handle = Some(tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(BUFFER_FLUSH_INTERVAL_MS));
            while running.load(Ordering::SeqCst) {
                ticker.tick().await;
                if let Err(e) = InMemoryStorageSinkFunction::flush_buffers(
                    &shared_client,
                    &buffer,
                    &keyed_buffer,
                    &dedup_buffer,
                    dedup,
                )
                .await
                {
                    eprintln!("[IN_MEMORY_SINK] flush error: {e}");
                }
            }
        }));

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.flush_handle.take() {
            let _ = handle.await;
        }
        if let Some(client) = &self.storage_client {
            Self::flush_buffers(
                client,
                &self.buffer,
                &self.keyed_buffer,
                &self.dedup_buffer,
                self.dedup,
            )
            .await?;
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
