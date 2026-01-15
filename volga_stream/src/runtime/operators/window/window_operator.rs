use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::fmt;
use std::time::Duration;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::future;
use tokio_rayon::rayon::ThreadPool;

use datafusion::physical_plan::windows::BoundedWindowAggExec;
use crate::common::message::Message;
use crate::common::Key;
use crate::runtime::operators::operator::{MessageStream, OperatorBase, OperatorConfig, OperatorPollResult, OperatorTrait, OperatorType};
use crate::runtime::operators::window::window_operator_state::{WindowOperatorState, WindowId};
use crate::runtime::operators::window::TileConfig;
use crate::runtime::operators::window::Cursor;
use crate::runtime::operators::window::shared::{WindowConfig, build_window_operator_parts};
use crate::runtime::runtime_context::RuntimeContext;
use crate::storage::StorageStatsSnapshot;
use crate::common::MAX_WATERMARK_VALUE;

#[path = "exec/advance.rs"]
mod advance;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionMode {
    Regular, // operator produces messages
    Request, // operator only updates state, produces no messages
}

#[derive(Debug, Clone, Copy)]
pub enum RequestAdvancePolicy {
    /// Advance windows only when watermark is received (default, supports complex DAGs).
    OnWatermark,
    /// Advance windows on every keyed message (for simple ingest+request topologies; max freshness).
    OnIngest,
}

// Advance pipeline is in `window/exec/advance.rs` (wired as `window_operator::advance`)

#[derive(Debug, Clone)]
pub struct WindowOperatorConfig {
    pub window_exec: Arc<BoundedWindowAggExec>,
    pub execution_mode: ExecutionMode,
    pub parallelize: bool,
    pub tiling_configs: Vec<Option<TileConfig>>,
    pub lateness: Option<i64>,
    pub request_advance_policy: RequestAdvancePolicy,
    pub compaction_interval_ms: u64,
    pub dump_interval_ms: u64,
    pub dump_hot_bucket_count: usize,
    pub in_mem_dump_parallelism: usize,
}

impl WindowOperatorConfig {
    pub fn new(window_exec: Arc<BoundedWindowAggExec>) -> Self {    
        Self {
            window_exec,
            execution_mode: ExecutionMode::Regular,
            parallelize: false,
            tiling_configs: Vec::new(),
            lateness: None,
            request_advance_policy: RequestAdvancePolicy::OnWatermark,
            compaction_interval_ms: 250,
            dump_interval_ms: 1000,
            dump_hot_bucket_count: 2,
            in_mem_dump_parallelism: 4,
        }
    }
}

pub struct WindowOperator {
    base: OperatorBase,
    window_configs: Arc<BTreeMap<WindowId, WindowConfig>>,
    state: Option<Arc<WindowOperatorState>>,
    buffered_keys: HashSet<Key>,
    execution_mode: ExecutionMode,
    request_advance_policy: RequestAdvancePolicy,
    parallelize: bool,
    thread_pool: Option<ThreadPool>,
    output_schema: SchemaRef,
    input_schema: SchemaRef,
    current_watermark: Option<u64>,
    compaction_interval: Duration,
    dump_interval: Duration,
    ts_column_index: usize,
    tiling_configs: Vec<Option<TileConfig>>,
    lateness: Option<i64>,
    dump_hot_bucket_count: usize,
    in_mem_dump_parallelism: usize,
}

impl fmt::Debug for WindowOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WindowOperator")
            .field("base", &self.base)
            .field("windows", &self.window_configs)
            .field("state", &self.state)
            .field("buffered_keys", &self.buffered_keys)
            .field("execution_mode", &self.execution_mode)
            .field("parallelize", &self.parallelize)
            .field("thread_pool", &self.thread_pool)
            .field("output_schema", &self.output_schema)
            .field("input_schema", &self.input_schema)
            .finish()
    }
}

// shared init lives in `window/init.rs`

impl WindowOperator {
    pub fn new(config: OperatorConfig) -> Self {
        let window_operator_config = match config.clone() {
            OperatorConfig::WindowConfig(window_config) => window_config,
            _ => panic!("Expected WindowConfig, got {:?}", config),
        };

        if matches!(
            window_operator_config.request_advance_policy,
            RequestAdvancePolicy::OnIngest
        ) && window_operator_config.execution_mode != ExecutionMode::Request
        {
            panic!(
                "RequestAdvancePolicy::OnIngest is only valid for ExecutionMode::Request"
            );
        }

        let (ts_column_index, windows, input_schema, output_schema, thread_pool) =
            build_window_operator_parts(
            false, &window_operator_config.window_exec, &window_operator_config.tiling_configs, window_operator_config.parallelize
        );

        let window_configs = Arc::new(windows);

        Self {
            base: OperatorBase::new(config),
            window_configs,
            state: None,
            buffered_keys: HashSet::new(),
            execution_mode: window_operator_config.execution_mode,
            request_advance_policy: window_operator_config.request_advance_policy,
            parallelize: window_operator_config.parallelize,
            thread_pool,
            output_schema,
            input_schema,
            current_watermark: None,
            compaction_interval: Duration::from_millis(window_operator_config.compaction_interval_ms.max(1)),
            dump_interval: Duration::from_millis(window_operator_config.dump_interval_ms.max(1)),
            ts_column_index,
            tiling_configs: window_operator_config.tiling_configs.clone(),
            lateness: window_operator_config.lateness,
            dump_hot_bucket_count: window_operator_config.dump_hot_bucket_count,
            in_mem_dump_parallelism: window_operator_config.in_mem_dump_parallelism,
        }
    }

    fn state_ref(&self) -> &Arc<WindowOperatorState> {
        self.state.as_ref().expect("WindowOperator must be opened first")
    }

    pub fn storage_stats_snapshot(&self) -> StorageStatsSnapshot {
        self.state_ref().storage_stats_snapshot()
    }

    async fn process_key(
        &self,
        key: &Key,
        advance_to: Option<Cursor>,
    ) -> (RecordBatch, bool) {
        let _permit = self
            .state_ref()
            .storage()
            .inflight_keys
            .clone()
            .acquire_owned()
            .await
            .expect("inflight_keys semaphore");

        let (result, still_pending) = self.advance_windows(key, advance_to).await;

        self.state_ref().prune_if_needed(key).await;

        (result, still_pending)
    }

    pub fn get_state(&self) -> &WindowOperatorState {
        self.state_ref()
    }

    async fn process_buffered(
        &self,
        keys: Vec<Key>,
        advance_to: Option<Cursor>,
    ) -> (RecordBatch, HashSet<Key>) {
        if keys.is_empty() {
            return (RecordBatch::new_empty(self.output_schema.clone()), HashSet::new());
        }

        // Parallelize across keys to overlap storage IO.
        let futures: Vec<_> = keys
            .iter()
            .map(|k| async move { (k.clone(), self.process_key(k, advance_to).await) })
            .collect();
        let results = future::join_all(futures).await;

        let mut batches: Vec<RecordBatch> = Vec::with_capacity(results.len());
        let mut pending_keys: HashSet<Key> = HashSet::new();
        for (k, (batch, pending)) in results {
            batches.push(batch);
            if pending {
                pending_keys.insert(k);
            }
        }

        // TOOO we should return vec instead of concating - separate batch per key
        let out = arrow::compute::concat_batches(&self.output_schema, &batches)
            .expect("Should be able to concat result batches");
        (out, pending_keys)
    }
}


#[async_trait]
impl OperatorTrait for WindowOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await?;
        
        let vertex_id = context.vertex_id_arc();
        let operator_states = context.operator_states();

        if self.state.is_none() {
            let storage = context
                .worker_storage_context()
                .expect("WorkerStorageContext must be provided by RuntimeContext");
            let in_mem_low_watermark_per_mille = storage.budgets.in_mem_low_watermark_per_mille;

            let task_id: crate::runtime::TaskId = context.vertex_id_arc();
            self.state = Some(Arc::new(WindowOperatorState::new(
                storage,
                task_id,
                self.ts_column_index,
                self.window_configs.clone(),
                self.tiling_configs.clone(),
                self.lateness,
                self.dump_hot_bucket_count,
                in_mem_low_watermark_per_mille,
                self.in_mem_dump_parallelism,
            )));
        }

        operator_states.insert_operator_state(vertex_id, self.state_ref().clone());
        // Start background compaction + dump (separate intervals), plus retirement cleanup.
        self.state_ref().start_background_tasks(
            self.compaction_interval,
            self.dump_interval,
        );
        
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(state) = &self.state {
            state.stop_background_tasks().await;
        }
        self.base.close().await
    }

    fn set_input(&mut self, input: Option<MessageStream>) {
        self.base.set_input(input);
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }

    fn operator_config(&self) -> &OperatorConfig {
        self.base.operator_config()
    }

    async fn poll_next(&mut self) -> OperatorPollResult {
        // First, return any buffered messages
        if let Some(msg) = self.base.pop_pending_output() {
            return OperatorPollResult::Ready(msg);
        }

        match self.base.next_input().await {
            Some(message) => {
                
                let _ingest_ts = message.ingest_timestamp();
                let _extras = message.get_extras();
                match message {
                    Message::Keyed(keyed_message) => {
                        let key = keyed_message.key();

                        // NOTE: we drop rows at/before the per-key processed cutoff:
                        // cutoff_ts = max(processed_pos.ts, watermark_ts - lateness).
                        // This allows late events within lateness without requiring retractions.
                        let watermark_ts = self.current_watermark.map(|v| {
                            if v == MAX_WATERMARK_VALUE {
                                i64::MAX
                            } else {
                                v as i64
                            }
                        });
                        let input_rows = keyed_message.base.record_batch.num_rows();
                        let dropped_rows = self
                            .state_ref()
                            .insert_batch(
                                key,
                                watermark_ts,
                                keyed_message.base.record_batch.clone(),
                            )
                            .await;
                        if dropped_rows < input_rows {
                            if self.execution_mode == ExecutionMode::Request
                                && matches!(self.request_advance_policy, RequestAdvancePolicy::OnIngest)
                            {
                                // Request mode, ingest-driven: advance state immediately, produce no outputs.
                                let _ = self.process_key(key, None).await;
                            } else {
                                // Default: buffer and wait for watermark to emit.
                                self.buffered_keys.insert(key.clone());
                            }
                        }
                        return OperatorPollResult::Continue;
                    }
                    Message::Watermark(watermark) => {
                        let vertex_id = self.base.runtime_context.as_ref().unwrap().vertex_id().to_string();
                        println!("[{}] Window operator received watermark: {:?}", vertex_id, watermark);

                        self.current_watermark = Some(watermark.watermark_value);
                        let wm_ts = if watermark.watermark_value == MAX_WATERMARK_VALUE {
                            i64::MAX
                        } else {
                            watermark.watermark_value as i64
                        };

                        // respect lateness when computing advance_to
                        let advance_to_ts = match (wm_ts, self.lateness) {
                            (ts, Some(lateness_ms)) if ts != i64::MAX => ts.saturating_sub(lateness_ms),
                            (ts, _) => ts,
                        };
                        let advance_to = Some(Cursor::new(advance_to_ts, u64::MAX));

                        let keys: Vec<Key> = self.buffered_keys.iter().cloned().collect();
                        let (result, pending_keys) = self.process_buffered(keys, advance_to).await;
                        if watermark.watermark_value == MAX_WATERMARK_VALUE {
                            self.buffered_keys.clear();
                        } else {
                            self.buffered_keys = pending_keys;
                        }

                        // vertex_id will be set by stream task
                        // TODO ingest timestamp and extras?
                        
                        // Buffer the watermark to be returned on next poll
                        self.base.pending_messages.push(Message::Watermark(watermark));

                        if self.execution_mode == ExecutionMode::Request {
                            // request mode produces no output, just updates state
                            return OperatorPollResult::Continue;
                        } else {
                            // vertex_id will be set by stream task
                            return OperatorPollResult::Ready(Message::new(None, result, None, None));
                        }
                    }
                    Message::CheckpointBarrier(barrier) => {
                        // pass through (StreamTask intercepts and checkpoints synchronously)
                        return OperatorPollResult::Ready(Message::CheckpointBarrier(barrier));
                    }
                    _ => {
                        panic!("Window operator expects keyed messages or watermarks");
                    }
                }
            }
            None => return OperatorPollResult::None,
        }
    }

    async fn checkpoint(&mut self, _checkpoint_id: u64) -> Result<Vec<(String, Vec<u8>)>> {
        // Ensure checkpoint depends only on the store: flush hot in-memory runs to `BatchStore`
        // and update the bucket index to reference `BatchRef::Stored`.
        self.state_ref().flush_in_mem_runs_to_store().await?;
        self.state_ref().await_store_persisted().await?;

        let state_cp = self.state_ref().to_checkpoint();
        let batch_store_cp = self
            .state_ref()
            .get_batch_store()
            .to_checkpoint(self.state_ref().task_id());

        Ok(vec![
            ("window_operator_state".to_string(), bincode::serialize(&state_cp)?),
            ("batch_store".to_string(), bincode::serialize(&batch_store_cp)?),
        ])
    }

    async fn restore(&mut self, blobs: &[(String, Vec<u8>)]) -> Result<()> {
        let state_bytes = blobs
            .iter()
            .find(|(name, _)| name == "window_operator_state")
            .map(|(_, b)| b.as_slice());
        let batch_store_bytes = blobs
            .iter()
            .find(|(name, _)| name == "batch_store")
            .map(|(_, b)| b.as_slice());

        if state_bytes.is_none() && batch_store_bytes.is_none() {
            return Ok(());
        }

        if let Some(bytes) = batch_store_bytes {
            let cp: crate::storage::batch_store::BatchStoreCheckpoint = bincode::deserialize(bytes)?;
            self.state_ref()
                .get_batch_store()
                .apply_checkpoint(self.state_ref().task_id(), cp);
        } else {
            panic!("Batch store bytes are missing");
        }

        if let Some(bytes) = state_bytes {
            let cp: crate::runtime::operators::window::window_operator_state::WindowOperatorStateCheckpoint =
                bincode::deserialize(bytes)?;
            // Important: do not replace the state Arc (it is registered in OperatorStates in open()).
            self.state_ref().apply_checkpoint(cp);
        } else {
            panic!("Window operator state bytes are missing");
        }

        Ok(())
    }
}

// output helpers live in `window/output.rs`
