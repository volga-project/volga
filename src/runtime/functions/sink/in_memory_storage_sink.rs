use async_trait::async_trait;
use anyhow::Result;
use arrow::array::{Array, Int64Array, StringArray, TimestampMillisecondArray};
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
    keyed_buffer: Arc<Mutex<HashMap<String, Message>>>,
    flush_handle: Option<tokio::task::JoinHandle<()>>,
    running: Arc<AtomicBool>,
    runtime_context: Option<RuntimeContext>,
    server_addr: String,
    /// When non-empty, explode rows and upsert into the keyed map using these columns.
    upsert_key_columns: Vec<String>,
}

impl InMemoryStorageSinkFunction {
    pub fn new(server_addr: String, upsert_key_columns: Vec<String>) -> Self {
        Self {
            storage_client: None,
            buffer: Arc::new(Mutex::new(Vec::new())),
            keyed_buffer: Arc::new(Mutex::new(HashMap::new())),
            flush_handle: None,
            running: Arc::new(AtomicBool::new(false)),
            runtime_context: None,
            server_addr,
            upsert_key_columns,
        }
    }

    fn column_value_as_string(column: &dyn Array, row_idx: usize) -> Result<String> {
        match column.data_type() {
            DataType::Utf8 => Ok(column
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row_idx)
                .to_string()),
            DataType::Timestamp(_, _) => Ok(column
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(row_idx)
                .to_string()),
            DataType::Int64 => Ok(column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row_idx)
                .to_string()),
            other => Err(anyhow::anyhow!(
                "upsert key column type {:?} is not supported (Utf8/Timestamp/Int64)",
                other
            )),
        }
    }

    fn extract_upsert_rows(
        message: &Message,
        key_columns: &[String],
    ) -> Result<Vec<(String, Message)>> {
        if key_columns.is_empty() {
            return Err(anyhow::anyhow!("upsert_key_columns must be non-empty"));
        }
        let batch = message.record_batch();
        let schema = batch.schema();
        let col_indexes = key_columns
            .iter()
            .map(|name| {
                schema.index_of(name).map_err(|_| {
                    anyhow::anyhow!("upsert key column '{name}' not found in sink batch schema")
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let mut rows = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            let mut parts = Vec::with_capacity(col_indexes.len());
            for &col_idx in &col_indexes {
                parts.push(Self::column_value_as_string(
                    batch.column(col_idx).as_ref(),
                    row_idx,
                )?);
            }
            let map_key = parts.join("|");
            let single = batch.slice(row_idx, 1);
            let row_message = Message::new(
                message.upstream_vertex_id(),
                single,
                message.ingest_timestamp(),
                None,
            );
            rows.push((map_key, row_message));
        }
        Ok(rows)
    }

    async fn flush_buffers(
        storage_client: &Arc<Mutex<InMemoryStorageClient>>,
        buffer: &Arc<Mutex<Vec<Message>>>,
        keyed_buffer: &Arc<Mutex<HashMap<String, Message>>>,
    ) -> Result<()> {
        let mut regular_batches = buffer.lock().await;
        if !regular_batches.is_empty() {
            let batches: Vec<Message> = regular_batches.drain(..).collect();
            let mut client = storage_client.lock().await;
            client.append_many(batches).await?;
        }

        let mut keyed_batches = keyed_buffer.lock().await;
        if !keyed_batches.is_empty() {
            let batches: HashMap<String, Message> = keyed_batches.drain().collect();
            let mut client = storage_client.lock().await;
            client.insert_keyed_many(batches).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl SinkFunctionTrait for InMemoryStorageSinkFunction {
    async fn sink(&mut self, message: Message) -> Result<()> {
        if !self.upsert_key_columns.is_empty() {
            let rows = Self::extract_upsert_rows(&message, &self.upsert_key_columns)?;
            let mut keyed_buffer = self.keyed_buffer.lock().await;
            for (key, row) in rows {
                keyed_buffer.insert(key, row);
            }
            return Ok(());
        }

        match &message {
            Message::Keyed(keyed_message) => {
                let key = keyed_message.key().hash().to_string();
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
        let running = self.running.clone();
        self.flush_handle = Some(tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(BUFFER_FLUSH_INTERVAL_MS));
            while running.load(Ordering::SeqCst) {
                ticker.tick().await;
                if let Err(e) =
                    InMemoryStorageSinkFunction::flush_buffers(&shared_client, &buffer, &keyed_buffer)
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
            Self::flush_buffers(client, &self.buffer, &self.keyed_buffer).await?;
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
