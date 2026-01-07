use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::fmt;
use std::time::Duration;

use anyhow::Result;
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Schema, SchemaBuilder, SchemaRef};
use async_trait::async_trait;
use futures::future;
use indexmap::IndexMap;
use tokio_rayon::rayon::{ThreadPool, ThreadPoolBuilder};

use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use crate::common::message::Message;
use crate::common::Key;
use crate::runtime::operators::operator::{MessageStream, OperatorBase, OperatorConfig, OperatorPollResult, OperatorTrait, OperatorType};
use crate::runtime::operators::window::aggregates::{get_aggregate_type, Aggregation, BucketRange};
use crate::runtime::operators::window::aggregates::plain::PlainAggregation;
use crate::runtime::operators::window::aggregates::retractable::RetractableAggregation;
use crate::runtime::operators::window::state::sorted_range_view_loader::{load_sorted_ranges_views, RangesLoadPlan};
use crate::runtime::operators::window::window_operator_state::{AccumulatorState, WindowOperatorState, WindowId};
use crate::runtime::operators::window::{AggregatorType, TileConfig};
use crate::runtime::operators::window::Cursor;
use crate::runtime::operators::window::state::window_logic;
use crate::storage::index::SortedRangeIndex;
use crate::storage::index::SortedRangeView;
use crate::runtime::runtime_context::RuntimeContext;
use crate::storage::StorageBudgetConfig;
use crate::storage::StorageStatsSnapshot;

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

#[derive(Debug, Clone)]
pub struct WindowConfig {
    pub window_id: WindowId,
    pub window_expr: Arc<dyn WindowExpr>,
    pub tiling: Option<TileConfig>,
    pub aggregator_type: AggregatorType,
    pub exclude_current_row: Option<bool>, // for request mode only
}

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
    pub in_mem_limit_bytes: usize,
    pub in_mem_low_watermark_per_mille: u32,
    pub in_mem_dump_parallelism: usize,
    pub max_inflight_keys: usize,
    pub load_io_parallelism: usize,
    pub work_limit_bytes: usize,
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
            in_mem_limit_bytes: 0,
            in_mem_low_watermark_per_mille: 700,
            in_mem_dump_parallelism: 4,
            max_inflight_keys: 1,
            load_io_parallelism: 16,
            work_limit_bytes: 256 * 1024 * 1024,
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
    storage_budgets: StorageBudgetConfig,
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

pub fn init(
    is_request_operator: bool,
    window_exec: &Arc<BoundedWindowAggExec>,
    tiling_configs: &Vec<Option<TileConfig>>,
    parallelize: bool
) -> (usize, BTreeMap<WindowId, WindowConfig>, SchemaRef, SchemaRef, Option<ThreadPool>) {
        let ts_column_index = window_exec.window_expr()[0].order_by()[0].expr.as_any().downcast_ref::<Column>().expect("Expected Column expression in ORDER BY").index();

        let mut windows = BTreeMap::new();
        for (window_id, window_expr) in window_exec.window_expr().iter().enumerate() {
            let exclude_current_row = if is_request_operator {
                // TODO get from SQL, only for request operator 
                Some(false)
            } else {
                None
            };
            windows.insert(window_id, WindowConfig {
                window_id,
                window_expr: window_expr.clone(),
                tiling: tiling_configs.get(window_id).and_then(|config| config.clone()),
                aggregator_type: get_aggregate_type(window_expr),
                exclude_current_row: exclude_current_row,
            });
        }

        let input_schema = window_exec.input().schema();
        let output_schema = create_output_schema(&input_schema, &window_exec.window_expr());

        let thread_pool = if parallelize {
            Some(ThreadPoolBuilder::new()
                .num_threads(std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
                .build()
                .expect("Failed to create thread pool"))
        } else {
            None
        };

    (ts_column_index, windows, input_schema, output_schema, thread_pool)
}

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

        let (ts_column_index, windows, input_schema, output_schema, thread_pool) = init(
            false, &window_operator_config.window_exec, &window_operator_config.tiling_configs, window_operator_config.parallelize
        );

        let window_configs = Arc::new(windows);
        let storage_budgets = StorageBudgetConfig {
            // Keep these in operator config until we move this into worker-level config plumbing.
            in_mem_limit_bytes: window_operator_config.in_mem_limit_bytes.max(1),
            in_mem_low_watermark_per_mille: window_operator_config.in_mem_low_watermark_per_mille,
            work_limit_bytes: window_operator_config.work_limit_bytes.max(1),
            max_inflight_keys: window_operator_config.max_inflight_keys.max(1),
            load_io_parallelism: window_operator_config.load_io_parallelism.max(1),
        };

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
            storage_budgets,
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
        process_until: Option<Cursor>,
    ) -> RecordBatch {
        let _permit = self
            .state_ref()
            .storage()
            .inflight_keys
            .clone()
            .acquire_owned()
            .await
            .expect("inflight_keys semaphore");

        let result = self.advance_windows(key, process_until).await;

        self.state_ref().prune_if_needed(key).await;
        result
    }

    pub fn get_state(&self) -> &WindowOperatorState {
        self.state_ref()
    }

    #[cfg(test)]
    fn storage_ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(self.get_state().storage(), other.get_state().storage())
    }

    async fn process_buffered(&self, process_until: Option<Cursor>) -> RecordBatch {
        let keys: Vec<Key> = self.buffered_keys.iter().cloned().collect();
        let futures: Vec<_> = keys
            .iter()
            .map(|key| async move { self.process_key(key, process_until).await })
            .collect();
        
        let results = future::join_all(futures).await;

        if results.is_empty() {
            return RecordBatch::new_empty(self.output_schema.clone());
        }

        // TOOO we should return vec instead of concating - separate batch per key
        arrow::compute::concat_batches(&self.output_schema, &results)
            .expect("Should be able to concat result batches")
    }

    async fn configure_aggregations(
        &self, 
        key: &Key,
        process_until: Option<Cursor>,
    ) -> (
        IndexMap<WindowId, Vec<Box<dyn Aggregation>>>,
        HashMap<WindowId, Cursor>,
        Option<(Option<Cursor>, Cursor, BucketRange)>,
    ) {
        let windows_state_guard = self.state_ref().get_windows_state(key).await
            .expect("Windows state should exist - insert_batch should have created it");
        let windows_state = windows_state_guard.value();
        
        let batch_index = &windows_state.bucket_index;

        let mut aggregations: IndexMap<WindowId, Vec<Box<dyn Aggregation>>> = IndexMap::new();
        let mut new_processed_until = HashMap::new();
        let ref_window_id = *self
            .window_configs
            .keys()
            .next()
            .expect("window_configs must be non-empty");
        let mut entry_delta: Option<(Option<Cursor>, Cursor, BucketRange)> = None;
        
        for (window_id, _window_config) in self.window_configs.iter() {
            let window_frame = _window_config.window_expr.get_window_frame();
            let window_state = windows_state.window_states.get(window_id).expect("Window state should exist");
            let aggregator_type = _window_config.aggregator_type;
            let tiles_owned = window_state.tiles.clone();
            let accumulator_state_owned = window_state.accumulator_state.clone();
            let prev_processed_until = window_state.processed_until;

            // Determine progress cap for this step.
            let until = process_until.unwrap_or_else(|| batch_index.max_pos_seen());
            let new_pos = until.min(batch_index.max_pos_seen());
            
            new_processed_until.insert(*window_id, new_pos);

            if *window_id == ref_window_id {
                if let Some(update_range) = batch_index.delta_span(prev_processed_until, new_pos) {
                    entry_delta = Some((prev_processed_until, new_pos, update_range));
                }
            }

            let mut aggs: Vec<Box<dyn Aggregation>> = Vec::new();

            if self.execution_mode == ExecutionMode::Regular {
                match aggregator_type {
                    AggregatorType::RetractableAccumulator => {
                        // Retractable aggregates only advance in-order; out-of-order rows
                        // are dropped on ingest (see WindowOperatorState::insert_batch).
                        if batch_index.delta_span(prev_processed_until, new_pos).is_some() {
                            aggs.push(Box::new(RetractableAggregation::from_range(
                                *window_id,
                                prev_processed_until,
                                new_pos,
                                batch_index,
                                _window_config.window_expr.clone(),
                                accumulator_state_owned.clone(),
                                self.state_ref().ts_column_index(),
                                batch_index.bucket_granularity(),
                            )));
                        }
                    }
                    AggregatorType::PlainAccumulator => {
                        if batch_index.delta_span(prev_processed_until, new_pos).is_some() {
                            aggs.push(Box::new(PlainAggregation::for_range(
                                batch_index,
                                _window_config.window_expr.clone(),
                                tiles_owned.clone(),
                                crate::runtime::operators::window::aggregates::plain::CursorBounds {
                                    prev: prev_processed_until,
                                    new: new_pos,
                                },
                            )));
                        }
                    }
                    AggregatorType::Evaluator => {
                        panic!("Evaluator is not supported yet");
                    }
                }
            } else {
                // Request mode: update state (processed_until + retractable accumulator_state),
                // but produce no output messages (handled in `poll_next`).
                match aggregator_type {
                    AggregatorType::RetractableAccumulator => {
                        if batch_index.delta_span(prev_processed_until, new_pos).is_some() {
                            aggs.push(Box::new(RetractableAggregation::from_range(
                                *window_id,
                                prev_processed_until,
                                new_pos,
                                batch_index,
                                _window_config.window_expr.clone(),
                                accumulator_state_owned.clone(),
                                self.state_ref().ts_column_index(),
                                batch_index.bucket_granularity(),
                            )));
                        }
                    }
                    // Plain accumulators are not materialized in request mode; their tiles are updated on ingest.
                    _ => {
                        let _ = window_frame;
                        let _ = tiles_owned;
                    }
                }
            }
            if !aggs.is_empty() {
                aggregations.insert(*window_id, aggs);
            }
        }

        drop(windows_state_guard);
        (aggregations, new_processed_until, entry_delta)
    }

    async fn advance_windows(
        &self, 
        key: &Key, 
        process_until: Option<Cursor>,
    ) -> RecordBatch {

        let (aggregations, new_processed_until, entry_delta) =
            self.configure_aggregations(key, process_until).await;
        
        if aggregations.is_empty() {
            return RecordBatch::new_empty(self.output_schema.clone());
        }
        
        // TODO: in the bucket-view design we will derive input values from bucket views.
        
        struct Exec<'a> {
            window_id: WindowId,
            agg: &'a dyn Aggregation,
        }

        let mut execs: Vec<Exec<'_>> = Vec::new();
        let mut load_plans: Vec<RangesLoadPlan> = Vec::new();
        for (window_id, aggs) in &aggregations {
            for agg in aggs {
                let requests = agg.get_data_requests();
                load_plans.push(RangesLoadPlan {
                    requests,
                    window_expr_for_args: agg.window_expr().clone(),
                });
                execs.push(Exec {
                    window_id: *window_id,
                    agg: agg.as_ref(),
                });
            }
        }

        let views_by_agg = load_sorted_ranges_views(self.state_ref(), key, &load_plans).await;

        let futures: Vec<_> = execs
            .iter()
            .zip(views_by_agg.iter())
            .map(|(exec, views)| async move {
                let result = exec
                    .agg
                    .produce_aggregates_from_ranges(
                        views,
                        self.thread_pool.as_ref(),
                    )
                    .await;
                (exec.window_id, result)
            })
            .collect();

        let results = future::join_all(futures).await;
        let mut aggregation_results: IndexMap<
            WindowId,
            Vec<(Vec<ScalarValue>, Option<AccumulatorState>)>,
        > = IndexMap::new();
        for (window_id, r) in results {
            aggregation_results.entry(window_id).or_default().push(r);
        }
        
        // Collect accumulator states
        let accumulator_states: HashMap<WindowId, Option<AccumulatorState>> = aggregation_results
            .iter()
            .map(|(window_id, results)| {
                let accumulator_state = results.last().and_then(|(_, state)| state.clone());
                (*window_id, accumulator_state)
            })
            .collect();
        
        drop(aggregations);
        
        // Update window state positions and accumulator states
        self.state_ref()
            .update_window_positions_and_accumulators(key, &new_processed_until, &accumulator_states)
            .await;

        if self.execution_mode == ExecutionMode::Request {
            return RecordBatch::new_empty(self.output_schema.clone());
        }

        let input_values = match entry_delta {
            None => Vec::new(),
            Some(d) => extract_input_values_for_output(d, &views_by_agg, &self.input_schema),
        };

        if input_values.is_empty() {
            return RecordBatch::new_empty(self.output_schema.clone());
        }

        // Collect aggregated values
        let aggregated_values: Vec<Vec<ScalarValue>> = aggregation_results
            .values()
            .map(|results| {
                results
                    .iter()
                    .flat_map(|(aggregates, _)| aggregates.clone())
                    .collect()
            })
            .collect();

        debug_assert!(
            aggregated_values
                .iter()
                .all(|col| col.len() == input_values.len()),
            "All window result columns must match input row count"
        );
        
        stack_concat_results(input_values, aggregated_values, &self.output_schema, &self.input_schema)
    }
}

fn extract_input_values_for_output(
    (prev, new, entry_range): (Option<Cursor>, Cursor, BucketRange),
    views_by_agg: &[Vec<SortedRangeView>],
    input_schema: &SchemaRef,
) -> Vec<Vec<ScalarValue>> {
    let view_for_entries = views_by_agg
        .iter()
        .flat_map(|g| g.iter())
        .find(|v| {
            let r = v.bucket_range();
            r.start <= entry_range.start && r.end >= entry_range.end
        });
    let Some(view) = view_for_entries else {
        return Vec::new();
    };

    let idx = SortedRangeIndex::new(view);
    let Some(mut pos) = window_logic::first_update_pos(&idx, prev) else {
        return Vec::new();
    };

    let end = match idx.seek_rowpos_gt(new) {
        Some(after) => match idx.prev_pos(after) {
            Some(p) => p,
            None => return Vec::new(),
        },
        None => idx.last_pos(),
    };
    if end < pos {
        return Vec::new();
    }

    let input_cols = input_schema.fields().len();
    let mut out: Vec<Vec<ScalarValue>> = Vec::new();
    loop {
        let bucket_ts = idx.bucket_ts(&pos);
        if bucket_ts >= entry_range.start && bucket_ts <= entry_range.end {
            let b = &view.segments()[pos.segment];
            let batch = b.batch();
            let mut row_vals = Vec::with_capacity(input_cols);
            for col_idx in 0..input_cols {
                row_vals.push(
                    ScalarValue::try_from_array(batch.column(col_idx), pos.row)
                        .expect("Should be able to extract scalar value"),
                );
            }
            out.push(row_vals);
        }
        if pos == end {
            break;
        }
        pos = idx.next_pos(pos).expect("must have next pos until end");
    }
    out
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

            let task_id: crate::runtime::TaskId = context.vertex_id_arc();
            self.state = Some(Arc::new(WindowOperatorState::new(
                storage,
                task_id,
                self.ts_column_index,
                self.window_configs.clone(),
                self.tiling_configs.clone(),
                self.lateness,
                self.dump_hot_bucket_count,
                self.storage_budgets.in_mem_low_watermark_per_mille,
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

                        let watermark_ts = self.current_watermark.map(|v| v as i64);
                        let input_rows = keyed_message.base.record_batch.num_rows();
                        let _dropped_rows = self
                            .state_ref()
                            .insert_batch(
                                key,
                                watermark_ts,
                                keyed_message.base.record_batch.clone(),
                            )
                            .await;
                        if _dropped_rows < input_rows {
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
                        let process_until = Some(Cursor::new(watermark.watermark_value as i64, u64::MAX));

                        let result = self.process_buffered(process_until).await;
                        self.buffered_keys.clear();

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


// copied from private DataFusion function
pub fn create_output_schema(
    input_schema: &Schema,
    window_exprs: &[Arc<dyn WindowExpr>],
) -> Arc<Schema> {
    let capacity = input_schema.fields().len() + window_exprs.len();
    let mut builder = SchemaBuilder::with_capacity(capacity);
    builder.extend(input_schema.fields().iter().cloned());
    // append results to the schema
    for expr in window_exprs {
        builder.push(expr.field().expect("Should be able to get field"));
    }
    Arc::new(builder
        .finish()
        .with_metadata(input_schema.metadata().clone())
    )
}

// creates a record batch by vertically stacking input_values and aggregated_values
pub fn stack_concat_results(
    input_values: Vec<Vec<ScalarValue>>, // values from input that were not part of aggregation (e.g keys)
    aggregated_values: Vec<Vec<ScalarValue>>, // produces aggregates
    output_schema: &SchemaRef,
    input_schema: &SchemaRef
) -> RecordBatch {
    let num_rows = input_values.len();
    
    // Assert: if a row has at least one Null, all windows should have Null for that row
    // (This is guaranteed by the current logic where lateness check happens once per entry)
    // Filter out rows where any aggregated value is Null (too-late entries)
    let keep_indices: Vec<usize> = (0..num_rows)
        .filter(|&row_idx| {
            // Check if this row has any Null values
            let has_null = aggregated_values.iter().any(|window_results| {
                matches!(window_results.get(row_idx), Some(ScalarValue::Null))
            });
            
            if has_null {
                // Assert that all windows have Null for this row
                for window_results in &aggregated_values {
                    assert!(
                        matches!(window_results.get(row_idx), Some(ScalarValue::Null)),
                        "If any window has Null for a row, all windows must have Null (row_idx: {})",
                        row_idx
                    );
                }
                // Remove this row (return false)
                false
            } else {
                // Keep this row (return true)
                true
            }
        })
        .collect();
    
    if keep_indices.is_empty() {
        // All rows were filtered out, return empty batch
        return RecordBatch::new_empty(output_schema.clone());
    }
    
    // Filter input_values and aggregated_values to keep only non-Null rows
    let filtered_input_values: Vec<Vec<ScalarValue>> = keep_indices.iter()
        .map(|&idx| input_values[idx].clone())
        .collect();
    
    let filtered_aggregated_values: Vec<Vec<ScalarValue>> = aggregated_values.iter()
        .map(|window_results| {
            keep_indices.iter()
                .map(|&idx| window_results[idx].clone())
                .collect()
        })
        .collect();
    
    let mut columns: Vec<ArrayRef> = Vec::new();
    
    // Create input columns (first N columns in output schema)
    for col_idx in 0..input_schema.fields().len() {
        let column_values: Vec<ScalarValue> = filtered_input_values.iter()
            .map(|row| row[col_idx].clone())
            .collect();
        
        let array = ScalarValue::iter_to_array(column_values.into_iter())
            .expect("Should be able to convert input values to array");
        columns.push(array);
    }
    
    // Add window result columns (remaining columns in output schema)
    for window_results in filtered_aggregated_values.iter() {
        let array = ScalarValue::iter_to_array(window_results.iter().cloned())
            .expect("Should be able to convert scalar values to array");
        columns.push(array);
    }

    // Ensure we have the right number of columns for the schema
    if columns.len() != output_schema.fields().len() {
        panic!("Mismatch between number of result columns ({}) and schema fields ({})", 
               columns.len(), output_schema.fields().len());
    }

    RecordBatch::try_new(output_schema.clone(), columns)
        .expect("Should be able to create RecordBatch from window results")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::{Float64Array, Int64Array, TimestampMillisecondArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use crate::api::planner::{Planner, PlanningContext};
    use crate::runtime::functions::key_by::key_by_function::extract_datafusion_window_exec;
    use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
    use crate::runtime::operators::operator::OperatorConfig;
    use crate::runtime::operators::operator::OperatorTrait;
    use crate::common::message::Message;
    use crate::runtime::runtime_context::RuntimeContext;
    use crate::runtime::state::OperatorStates;
    use crate::common::Key;
    use crate::storage::{StorageBudgetConfig, WorkerStorageContext};
    use crate::storage::batch_store::{BatchStore, InMemBatchStore};
    use crate::runtime::operators::window::TimeGranularity;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("value", DataType::Float64, false),
            Field::new("partition_key", DataType::Utf8, false),
        ]))
    }

    fn create_test_batch(timestamps: Vec<i64>, values: Vec<f64>, partition_keys: Vec<&str>) -> RecordBatch {
        let schema = create_test_schema();
        let timestamp_array = Arc::new(TimestampMillisecondArray::from(timestamps));
        let value_array = Arc::new(Float64Array::from(values));
        let partition_array = Arc::new(StringArray::from(partition_keys));
        
        RecordBatch::try_new(schema, vec![timestamp_array, value_array, partition_array])
            .expect("Should be able to create test batch")
    }

    fn generate_keyed_messages(num_keys: usize, rows_per_key: usize, batch_size: usize) -> Vec<Message> {
        assert!(num_keys > 0);
        assert!(rows_per_key > 0);
        assert!(batch_size > 0);

        let key_names: Vec<String> = (0..num_keys).map(|i| format!("K{}", i)).collect();

        let mut per_key_next_ts: Vec<i64> = (0..num_keys).map(|k| 1000 + (k as i64) * 7).collect();
        let mut per_key_rows_emitted: Vec<usize> = vec![0; num_keys];

        let mut out = Vec::new();
        loop {
            let mut any = false;
            for k in 0..num_keys {
                if per_key_rows_emitted[k] >= rows_per_key {
                    continue;
                }
                any = true;

                let remaining = rows_per_key - per_key_rows_emitted[k];
                let n = remaining.min(batch_size);

                let mut ts = Vec::with_capacity(n);
                let mut vals = Vec::with_capacity(n);
                let mut keys = Vec::with_capacity(n);

                for i in 0..n {
                    let t = per_key_next_ts[k];
                    per_key_next_ts[k] += 1;
                    ts.push(t);

                    let pos = per_key_rows_emitted[k] + i;
                    let v = ((k as i64 * 31 + pos as i64 * 17) % 5) as f64 + 1.0;
                    vals.push(v);

                    keys.push(key_names[k].as_str());
                }

                per_key_rows_emitted[k] += n;
                let batch = create_test_batch(ts, vals, keys);
                out.push(create_keyed_message(batch, &key_names[k]));
            }

            if !any {
                break;
            }
        }

        out
    }

    fn create_test_key(partition_name: &str) -> Key {
        let schema = Arc::new(Schema::new(vec![
            Field::new("partition", DataType::Utf8, false),
        ]));
        
        let partition_array = StringArray::from(vec![partition_name]);
        let key_batch = RecordBatch::try_new(schema, vec![Arc::new(partition_array)])
            .expect("Failed to create key batch");
        
        Key::new(key_batch).expect("Failed to create key")
    }

    async fn extract_window_exec_from_sql(sql: &str) -> Arc<BoundedWindowAggExec> {
        let ctx = SessionContext::new();
        let mut planner = Planner::new(PlanningContext::new(ctx));
        let schema = create_test_schema();
        
        planner.register_source(
            "test_table".to_string(), 
            SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])), 
            schema.clone()
        );

        extract_datafusion_window_exec(sql, &mut planner).await
    }

    #[tokio::test]
    async fn worker_storage_is_shared_via_runtime_context() {
        let sql = "SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
        FROM test_table
        WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW)";

        let window_exec = extract_window_exec_from_sql(sql).await;
        let mut cfg1 = WindowOperatorConfig::new(window_exec.clone());
        cfg1.in_mem_limit_bytes = 64 * 1024 * 1024;
        cfg1.work_limit_bytes = 64 * 1024 * 1024;
        cfg1.max_inflight_keys = 2;

        let cfg2 = cfg1.clone();

        let budgets = StorageBudgetConfig {
            in_mem_limit_bytes: 64 * 1024 * 1024,
            in_mem_low_watermark_per_mille: 700,
            work_limit_bytes: 64 * 1024 * 1024,
            max_inflight_keys: 2,
            load_io_parallelism: 4,
        };
        let store =
            Arc::new(InMemBatchStore::new(64, TimeGranularity::Seconds(1), 128)) as Arc<dyn BatchStore>;
        let shared = WorkerStorageContext::new(store, budgets).expect("ctx");

        let operator_states = Arc::new(OperatorStates::new());
        let mut ctx1 = RuntimeContext::new(
            "w1".to_string().into(),
            0,
            1,
            None,
            Some(operator_states.clone()),
            None,
        );
        ctx1.set_worker_storage_context(shared.clone());
        let mut ctx2 = RuntimeContext::new(
            "w2".to_string().into(),
            0,
            1,
            None,
            Some(operator_states.clone()),
            None,
        );
        ctx2.set_worker_storage_context(shared.clone());

        let mut op1 = WindowOperator::new(OperatorConfig::WindowConfig(cfg1));
        let mut op2 = WindowOperator::new(OperatorConfig::WindowConfig(cfg2));
        op1.open(&ctx1).await.expect("open op1");
        op2.open(&ctx2).await.expect("open op2");

        assert!(op1.storage_ptr_eq(&op2), "operators should share worker storage context");
        assert!(Arc::ptr_eq(op1.get_state().storage(), &shared));
        assert!(Arc::ptr_eq(op2.get_state().storage(), &shared));
    }

    fn create_keyed_message(batch: RecordBatch, partition_key: &str) -> Message {
        let key = create_test_key(partition_key);
        Message::new_keyed(None, batch, key, None, None)
    }

    fn create_watermark_message(watermark_value: u64) -> Message {
        Message::Watermark(crate::common::WatermarkMessage::new(
            "test".to_string(),
            watermark_value,
            Some(0),
        ))
    }

    async fn drain_one_pending_message(window_operator: &mut WindowOperator) {
        // Watermarks are buffered to be returned on the next poll.
        let _ = window_operator.poll_next().await;
    }

    fn create_test_runtime_context() -> RuntimeContext {
        use crate::storage::{StorageBudgetConfig, WorkerStorageContext};
        use crate::storage::batch_store::{BatchStore, InMemBatchStore};
        use crate::runtime::operators::window::TimeGranularity;

        let store =
            Arc::new(InMemBatchStore::new(64, TimeGranularity::Seconds(1), 128)) as Arc<dyn BatchStore>;
        let shared = WorkerStorageContext::new(store, StorageBudgetConfig::default()).expect("ctx");

        let mut ctx = RuntimeContext::new(
            "test_vertex".to_string().into(),
            0,
            1,
            None,
            Some(Arc::new(OperatorStates::new())),
            None
        );
        ctx.set_worker_storage_context(shared);
        ctx
    }

    async fn collect_regular_output_rows(window_operator: &mut WindowOperator) -> Vec<(i64, f64, String, f64, i64, f64)> {
        let mut rows = Vec::new();
        loop {
            match window_operator.poll_next().await {
                OperatorPollResult::Ready(msg) => match msg {
                    Message::Regular(base) => {
                        let batch = base.record_batch;
                        if batch.num_rows() == 0 {
                            continue;
                        }

                        // Columns: timestamp, value, partition_key, sum_val, cnt_val, avg_val
                        let ts = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .expect("timestamp col");
                        let val = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .expect("value col");
                        let key = batch
                            .column(2)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .expect("partition_key col");
                        let sum = batch
                            .column(3)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .expect("sum col");
                        let cnt = batch
                            .column(4)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("count col");
                        let avg = batch
                            .column(5)
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .expect("avg col");

                        for i in 0..batch.num_rows() {
                            rows.push((
                                ts.value(i),
                                val.value(i),
                                key.value(i).to_string(),
                                sum.value(i),
                                cnt.value(i),
                                avg.value(i),
                            ));
                        }
                    }
                    Message::Watermark(_) | Message::CheckpointBarrier(_) => {
                        // Not expected in these tests; if it happens it's fine to ignore.
                    }
                    _ => panic!("Unexpected output message from window operator: {:?}", msg),
                },
                OperatorPollResult::Continue => continue,
                OperatorPollResult::None => break,
            }
        }
        rows
    }

    #[tokio::test]
    async fn test_window_operator_checkpoint_consistency() {
        // Same query shape as the end-to-end checkpoint recovery test.
        let sql = "SELECT timestamp, value, partition_key, \
                   SUM(value) OVER w as sum_val, \
                   COUNT(value) OVER w as cnt_val, \
                   AVG(value) OVER w as avg_val \
                   FROM test_table \
                   WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp \
                   RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW)";

        let window_exec = extract_window_exec_from_sql(sql).await;
        let mut window_config = WindowOperatorConfig::new(window_exec);
        window_config.parallelize = false;

        let operator_config = OperatorConfig::WindowConfig(window_config.clone());

        let msgs: Vec<Message> = generate_keyed_messages(16, 250, 32);

        // Baseline: run uninterrupted.
        let mut baseline = WindowOperator::new(operator_config.clone());
        let baseline_ctx = create_test_runtime_context();
        baseline.open(&baseline_ctx).await.expect("open baseline");
        baseline.set_input(Some(Box::pin(futures::stream::iter(msgs.clone()))));
        let baseline_rows = collect_regular_output_rows(&mut baseline).await;

        // With checkpoint+restore in the middle.
        let split_at = msgs.len() / 2;
        let mut first = WindowOperator::new(operator_config.clone());
        let first_ctx = create_test_runtime_context();
        first.open(&first_ctx).await.expect("open first");
        first.set_input(Some(Box::pin(futures::stream::iter(msgs[..split_at].to_vec()))));
        let first_rows = collect_regular_output_rows(&mut first).await;
        let blobs = first.checkpoint(1).await.expect("checkpoint");

        let mut second = WindowOperator::new(operator_config);
        let second_ctx = create_test_runtime_context();
        second.open(&second_ctx).await.expect("open second");
        second.restore(&blobs).await.expect("restore");
        second.set_input(Some(Box::pin(futures::stream::iter(msgs[split_at..].to_vec()))));
        let second_rows = collect_regular_output_rows(&mut second).await;

        let mut restored_rows = first_rows;
        restored_rows.extend(second_rows);

        assert_eq!(
            baseline_rows.len(),
            restored_rows.len(),
            "row count mismatch baseline={} restored={}",
            baseline_rows.len(),
            restored_rows.len()
        );

        for (i, (b, r)) in baseline_rows.iter().zip(restored_rows.iter()).enumerate() {
            assert_eq!(b.0, r.0, "timestamp mismatch at row {}", i);
            assert_eq!(b.2, r.2, "partition_key mismatch at row {}", i);
            assert_eq!(b.4, r.4, "count mismatch at row {}", i);
            assert!((b.1 - r.1).abs() < 1e-9, "value mismatch at row {}: {} vs {}", i, b.1, r.1);
            assert!((b.3 - r.3).abs() < 1e-9, "sum mismatch at row {}: {} vs {}", i, b.3, r.3);
            assert!((b.5 - r.5).abs() < 1e-9, "avg mismatch at row {}: {} vs {}", i, b.5, r.5);
        }
    }

    #[tokio::test]
    async fn test_range_window_watermark_emission() {
        // Single window definition with alias and multiple aggregates
        let sql = "SELECT 
            timestamp,
            value,
            partition_key,
            SUM(value) OVER w as sum_val,
            COUNT(value) OVER w as count_val,
            AVG(value) OVER w as avg_val,
            MIN(value) OVER w as min_val,
            MAX(value) OVER w as max_val
        FROM test_table
        WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW)";
        
        let window_exec = extract_window_exec_from_sql(sql).await;
        let mut window_config = WindowOperatorConfig::new(window_exec);
        window_config.parallelize = true;

        let operator_config = OperatorConfig::WindowConfig(window_config);
        
        let mut window_operator = WindowOperator::new(operator_config);
        let runtime_context = create_test_runtime_context();
        window_operator.open(&runtime_context).await.expect("Should be able to open operator");
        
        // Create all test batches (we will wrap them into messages per step).
        let batch1 = create_test_batch(vec![1000], vec![10.0], vec!["A"]);
        let batch2 = create_test_batch(vec![1500, 2000], vec![30.0, 20.0], vec!["A", "A"]);
        let batch3 = create_test_batch(vec![3200], vec![5.0], vec!["A"]);
        // Partition B data must not be late relative to the current watermark (wm3=3200),
        // otherwise it will be dropped by strict watermark semantics.
        let batch4 = create_test_batch(vec![3500, 4000], vec![100.0, 200.0], vec!["B", "B"]);

        // Message 1: buffer
        let message1 = create_keyed_message(batch1.clone(), "A");
        let input_stream = Box::pin(futures::stream::iter(vec![message1]));
        window_operator.set_input(Some(input_stream));
        
        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue));

        // Watermark @1000 emits results for message1.
        let wm1 = create_watermark_message(1000);
        let input_stream = Box::pin(futures::stream::iter(vec![wm1]));
        window_operator.set_input(Some(input_stream));
        let result1 = window_operator.poll_next().await;
        let message1_result = result1.get_result_message();
        let result_batch1 = message1_result.record_batch();
        assert_eq!(result_batch1.num_rows(), 1, "Should have 1 result row");
        assert_eq!(result_batch1.num_columns(), 8, "Should have 8 columns (timestamp, value, partition_key, + 5 aggregates)");
        
        // Verify first row results: SUM=10.0, COUNT=1, AVG=10.0, MIN=10.0, MAX=10.0
        // Columns: [0=timestamp, 1=value, 2=partition_key, 3=sum_val, 4=count_val, 5=avg_val, 6=min_val, 7=max_val]
        let sum_column = result_batch1.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let count_column = result_batch1.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        
        assert_eq!(sum_column.value(0), 10.0, "SUM should be 10.0");
        assert_eq!(count_column.value(0), 1, "COUNT should be 1");

        // Drain the buffered watermark message.
        drain_one_pending_message(&mut window_operator).await;
        
        // Message 2: buffer
        let message2 = create_keyed_message(batch2.clone(), "A");
        let input_stream = Box::pin(futures::stream::iter(vec![message2]));
        window_operator.set_input(Some(input_stream));
        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue));

        // Watermark @2000 emits results for message2.
        let wm2 = create_watermark_message(2000);
        let input_stream = Box::pin(futures::stream::iter(vec![wm2]));
        window_operator.set_input(Some(input_stream));
        let result2 = window_operator.poll_next().await;
        let message2_result = result2.get_result_message();
        let result_batch2 = message2_result.record_batch();
        assert_eq!(result_batch2.num_rows(), 2, "Should have 2 result rows");
        
        let sum_column2 = result_batch2.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let count_column2 = result_batch2.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        
        // Row 1 (t=1500): includes t=1000,1500 -> SUM=40.0, COUNT=2
        assert_eq!(sum_column2.value(0), 40.0, "SUM at t=1500 should be 40.0 (10.0+30.0)");
        assert_eq!(count_column2.value(0), 2, "COUNT at t=1500 should be 2");
        
        // Row 2 (t=2000): includes t=1000,1500,2000 -> SUM=60.0, COUNT=3  
        assert_eq!(sum_column2.value(1), 60.0, "SUM at t=2000 should be 60.0 (10.0+30.0+20.0)");
        assert_eq!(count_column2.value(1), 3, "COUNT at t=2000 should be 3");

        drain_one_pending_message(&mut window_operator).await;
        
        // Message 3: buffer
        let message3 = create_keyed_message(batch3.clone(), "A");
        let input_stream = Box::pin(futures::stream::iter(vec![message3]));
        window_operator.set_input(Some(input_stream));
        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue));

        // Watermark @3200 emits results for message3.
        let wm3 = create_watermark_message(3200);
        let input_stream = Box::pin(futures::stream::iter(vec![wm3]));
        window_operator.set_input(Some(input_stream));
        let result3 = window_operator.poll_next().await;
        let message3_result = result3.get_result_message();
        let result_batch3 = message3_result.record_batch();
        assert_eq!(result_batch3.num_rows(), 1, "Should have 1 result row");
        
        let sum_column3 = result_batch3.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let count_column3 = result_batch3.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        
        // Window now includes t=1500,2000,3200 (t=1000 excluded) -> SUM=55.0, COUNT=3
        assert_eq!(sum_column3.value(0), 55.0, "SUM at t=3200 should be 55.0 (30.0+20.0+5.0)");
        assert_eq!(count_column3.value(0), 3, "COUNT at t=3200 should be 3");

        drain_one_pending_message(&mut window_operator).await;
        
        // Message 4: buffer
        let message4 = create_keyed_message(batch4.clone(), "B");
        let input_stream = Box::pin(futures::stream::iter(vec![message4]));
        window_operator.set_input(Some(input_stream));
        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue));

        // Watermark @4000 emits results for message4.
        let wm4 = create_watermark_message(4000);
        let input_stream = Box::pin(futures::stream::iter(vec![wm4]));
        window_operator.set_input(Some(input_stream));
        let result4 = window_operator.poll_next().await;
        let message4_result = result4.get_result_message();
        let result_batch4 = message4_result.record_batch();
        assert_eq!(result_batch4.num_rows(), 2, "Should have 2 result rows for partition B");
        
        let sum_column4 = result_batch4.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // Partition B should have independent window state
        assert_eq!(sum_column4.value(0), 100.0, "SUM for partition B at t=3500 should be 100.0");
        assert_eq!(sum_column4.value(1), 300.0, "SUM for partition B at t=4000 should be 300.0 (100.0+200.0)");

        drain_one_pending_message(&mut window_operator).await;
        
        window_operator.close().await.expect("Should be able to close operator");
    }

    #[tokio::test]
    async fn test_rows_window_watermark_emission() {
        // Test ROWS-based window function with alias
        let sql = "SELECT 
            timestamp,
            value,
            partition_key,
            SUM(value) OVER w as sum_3_rows
        FROM test_table
        WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)";
        
        let window_exec = extract_window_exec_from_sql(sql).await;
        let mut window_config = WindowOperatorConfig::new(window_exec);
        window_config.parallelize = true;

        let operator_config = OperatorConfig::WindowConfig(window_config);
        
        let mut window_operator = WindowOperator::new(operator_config);
        let runtime_context = create_test_runtime_context();
        window_operator.open(&runtime_context).await.expect("Should be able to open operator");
        
        // Test data: 5 rows with increasing values
        let batch = create_test_batch(
            vec![1000, 2000, 3000, 4000, 5000], 
            vec![10.0, 20.0, 30.0, 40.0, 50.0], 
            vec!["test", "test", "test", "test", "test"]
        );
        let message = create_keyed_message(batch, "test");

        let watermark = create_watermark_message(6000);
        let input_stream = Box::pin(futures::stream::iter(vec![message, watermark]));
        window_operator.set_input(Some(input_stream));

        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue));
        let result = window_operator.poll_next().await;
        let message_result = result.get_result_message();
        let result_batch = message_result.record_batch();
        assert_eq!(result_batch.num_rows(), 5, "Should have 5 result rows");
        
        let sum_column = result_batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // ROWS window calculations (always exactly 3 rows: 2 PRECEDING + CURRENT):
        // Row 1: window=[row1]           -> includes: 10.0                -> sum=10.0
        // Row 2: window=[row1,row2]      -> includes: 10.0,20.0           -> sum=30.0  
        // Row 3: window=[row1,row2,row3] -> includes: 10.0,20.0,30.0      -> sum=60.0
        // Row 4: window=[row2,row3,row4] -> includes: 20.0,30.0,40.0      -> sum=90.0
        // Row 5: window=[row3,row4,row5] -> includes: 30.0,40.0,50.0      -> sum=120.0
        
        assert_eq!(sum_column.value(0), 10.0, "SUM at row1 should be 10.0");
        assert_eq!(sum_column.value(1), 30.0, "SUM at row2 should be 30.0 (10.0+20.0)");
        assert_eq!(sum_column.value(2), 60.0, "SUM at row3 should be 60.0 (10.0+20.0+30.0)");
        assert_eq!(sum_column.value(3), 90.0, "SUM at row4 should be 90.0 (20.0+30.0+40.0)");
        assert_eq!(sum_column.value(4), 120.0, "SUM at row5 should be 120.0 (30.0+40.0+50.0)");

        // Drain watermark passthrough.
        drain_one_pending_message(&mut window_operator).await;
        
        window_operator.close().await.expect("Should be able to close operator");
    }

    #[tokio::test]
    async fn test_different_window_sizes() {
        // Test with different RANGE window sizes: SUM for small window, AVG for large window
        let sql = "SELECT 
            timestamp,
            value,
            partition_key,
            SUM(value) OVER w1 as sum_small,
            AVG(value) OVER w2 as avg_large
        FROM test_table 
        WINDOW 
            w1 AS (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW),
            w2 AS (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '3000' MILLISECOND PRECEDING AND CURRENT ROW)";
        
        let window_exec = extract_window_exec_from_sql(sql).await;
        let mut window_config = WindowOperatorConfig::new(window_exec);
        window_config.parallelize = true;
        
        let operator_config = OperatorConfig::WindowConfig(window_config);
        
        let mut window_operator = WindowOperator::new(operator_config);
        let runtime_context = create_test_runtime_context();
        window_operator.open(&runtime_context).await.expect("Should be able to open operator");

        // Create all test messages
        let batch1 = create_test_batch(vec![1000, 2000, 3000, 4000, 5000], vec![10.0, 20.0, 30.0, 40.0, 50.0], vec!["A", "A", "A", "A", "A"]);
        let message1 = create_keyed_message(batch1, "A");
        
        let batch2 = create_test_batch(vec![1500, 6000, 2500], vec![15.0, 60.0, 25.0], vec!["A", "A", "A"]);
        let message2 = create_keyed_message(batch2, "A");
        
        let batch3 = create_test_batch(vec![7000, 8000], vec![70.0, 80.0], vec!["A", "A"]);
        let message3 = create_keyed_message(batch3, "A");
        
        // Step 1: buffer message1, then emit via watermark.
        let wm1 = create_watermark_message(5000);
        let input_stream = Box::pin(futures::stream::iter(vec![message1, wm1]));
        window_operator.set_input(Some(input_stream));

        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue));
        let result1 = window_operator.poll_next().await;
        let message1_result = result1.get_result_message();
        let result_batch1 = message1_result.record_batch();
        assert_eq!(result_batch1.num_rows(), 5, "Should have 5 result rows");
        
        let sum_small_col = result_batch1.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let avg_large_col = result_batch1.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // Verify small window (RANGE 1000ms PRECEDING - SUM aggregate)
        // t=1000: window=[1000] -> SUM=10.0
        // t=2000: window=[1000,2000] -> SUM=30.0 (both within 1000ms range)
        // t=3000: window=[2000,3000] -> SUM=50.0 (1000 outside range)
        // t=4000: window=[3000,4000] -> SUM=70.0 (2000 outside range)
        // t=5000: window=[4000,5000] -> SUM=90.0 (3000 outside range)
        assert_eq!(sum_small_col.value(0), 10.0, "t=1000 small window SUM should be 10.0");
        assert_eq!(sum_small_col.value(1), 30.0, "t=2000 small window SUM should be 30.0");
        assert_eq!(sum_small_col.value(2), 50.0, "t=3000 small window SUM should be 50.0");
        assert_eq!(sum_small_col.value(3), 70.0, "t=4000 small window SUM should be 70.0");
        assert_eq!(sum_small_col.value(4), 90.0, "t=5000 small window SUM should be 90.0");
        
        // Verify large window (RANGE 3000ms PRECEDING - AVG aggregate)
        // t=1000: window=[1000] -> AVG=10.0
        // t=2000: window=[1000,2000] -> AVG=15.0 (30.0/2)
        // t=3000: window=[1000,2000,3000] -> AVG=20.0 (60.0/3)
        // t=4000: window=[1000,2000,3000,4000] -> AVG=25.0 (100.0/4)
        // t=5000: window=[2000,3000,4000,5000] -> AVG=35.0 (140.0/4, 1000 outside range)
        assert_eq!(avg_large_col.value(0), 10.0, "t=1000 large window AVG should be 10.0");
        assert_eq!(avg_large_col.value(1), 15.0, "t=2000 large window AVG should be 15.0");
        assert_eq!(avg_large_col.value(2), 20.0, "t=3000 large window AVG should be 20.0");
        assert_eq!(avg_large_col.value(3), 25.0, "t=4000 large window AVG should be 25.0");
        assert_eq!(avg_large_col.value(4), 35.0, "t=5000 large window AVG should be 35.0");

        drain_one_pending_message(&mut window_operator).await;

        // Step 2: buffer message2, then emit via watermark.
        let wm2 = create_watermark_message(6000);
        let input_stream = Box::pin(futures::stream::iter(vec![message2, wm2]));
        window_operator.set_input(Some(input_stream));

        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue));
        let result2 = window_operator.poll_next().await;
        let message2_result = result2.get_result_message();
        let result_batch2 = message2_result.record_batch();
        assert_eq!(result_batch2.num_rows(), 1, "Should have 1 result row (late rows dropped)");
        
        let sum_small_col2 = result_batch2.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let avg_large_col2 = result_batch2.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // Rows with ts <= previously emitted watermark(5000) are dropped; only t=6000 remains.
        // t=6000: small window=[5000,6000] -> SUM=110.0, large window=[3000,4000,5000,6000] -> AVG=45.0
        assert_eq!(sum_small_col2.value(0), 110.0, "t=6000 small window SUM should be 110.0");
        assert_eq!(avg_large_col2.value(0), 45.0, "t=6000 large window AVG should be 45.0");

        drain_one_pending_message(&mut window_operator).await;

        // Step 3: buffer message3, then emit via watermark.
        let wm3 = create_watermark_message(8000);
        let input_stream = Box::pin(futures::stream::iter(vec![message3, wm3]));
        window_operator.set_input(Some(input_stream));

        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue));
        let result3 = window_operator.poll_next().await;
        let message3_result = result3.get_result_message();
        let result_batch3 = message3_result.record_batch();
        assert_eq!(result_batch3.num_rows(), 2, "Should have 2 result rows");
        
        let sum_small_col3 = result_batch3.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let avg_large_col3 = result_batch3.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // Verify subsequent calculations (late rows were dropped).
        // t=7000: small window=[6000,7000] -> SUM=130.0, large window=[4000,5000,6000,7000] -> AVG=55.0 (220.0/4)
        // t=8000: small window=[7000,8000] -> SUM=150.0, large window=[5000,6000,7000,8000] -> AVG=65.0 (260.0/4)
        assert_eq!(sum_small_col3.value(0), 130.0, "t=7000 small window SUM should be 130.0");
        assert_eq!(sum_small_col3.value(1), 150.0, "t=8000 small window SUM should be 150.0");
        
        assert_eq!(avg_large_col3.value(0), 55.0, "t=7000 large window AVG should be 55.0");
        assert_eq!(avg_large_col3.value(1), 65.0, "t=8000 large window AVG should be 65.0");

        drain_one_pending_message(&mut window_operator).await;

        window_operator.close().await.expect("Should be able to close operator");
    }

    #[tokio::test]
    async fn test_tiled_aggregates() {
        // Test tiled aggregates (MIN, MAX), with late entries
        let sql = "SELECT 
            timestamp,
            value,
            partition_key,
            MIN(value) OVER w as min_val,
            MAX(value) OVER w as max_val,
            AVG(value) OVER w as avg_val
        FROM test_table
        WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '5000' MILLISECOND PRECEDING AND CURRENT ROW)";
        
        let window_exec = extract_window_exec_from_sql(sql).await;
        let mut window_config = WindowOperatorConfig::new(window_exec);
        window_config.parallelize = true;
        
        // Set up tiling configs for all three aggregates
        use crate::runtime::operators::window::state::tiles::{TileConfig, TimeGranularity};
        let tile_config = TileConfig::new(vec![
            TimeGranularity::Minutes(1),
            TimeGranularity::Minutes(5),
        ]).expect("Should create tile config");
        
        // Apply tiling to all three window functions (MIN, MAX, AVG)
        window_config.tiling_configs = vec![
            Some(tile_config.clone()), // MIN
            Some(tile_config.clone()), // MAX  
            Some(tile_config.clone()), // AVG
        ];
        
        let operator_config = OperatorConfig::WindowConfig(window_config);
        let runtime_context = create_test_runtime_context();
        
        let mut window_operator = WindowOperator::new(operator_config);
        window_operator.open(&runtime_context).await.expect("Should be able to open operator");
        
        // Create all test messages
        let batch1 = create_test_batch(
            vec![60000, 180000, 300000, 420000], 
            vec![10.0, 30.0, 50.0, 70.0], 
            vec!["A", "A", "A", "A"]
        );
        let message1 = create_keyed_message(batch1, "A");
        
        let batch2 = create_test_batch(
            vec![120000, 240000, 360000, 480000], 
            vec![20.0, 40.0, 60.0, 80.0], 
            vec!["A", "A", "A", "A"]
        );
        let message2 = create_keyed_message(batch2, "A");
        
        let batch3 = create_test_batch(
            vec![540000, 541000, 542000], 
            vec![90.0, 100.0, 110.0], 
            vec!["A", "A", "A"]
        );
        let message3 = create_keyed_message(batch3, "A");
        
        let batch4 = create_test_batch(
            vec![60000, 120000, 180000], 
            vec![5.0, 15.0, 25.0], 
            vec!["B", "B", "B"]
        );
        let message4 = create_keyed_message(batch4, "B");
        
        // Step 1: buffer message1, then emit via watermark.
        let wm1 = create_watermark_message(420000);
        let input_stream = Box::pin(futures::stream::iter(vec![message1, wm1]));
        window_operator.set_input(Some(input_stream));

        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue));
        let result1 = window_operator.poll_next().await;
        let message1_result = result1.get_result_message();
        let result_batch1 = message1_result.record_batch();
        assert_eq!(result_batch1.num_rows(), 4, "Should have 4 result rows");
        
        // Verify initial results (5000ms window)
        let min_column1 = result_batch1.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let max_column1 = result_batch1.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        let avg_column1 = result_batch1.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // Expected results for 5000ms window:
        // t=60000: window=[60000] -> MIN=10.0, MAX=10.0, AVG=10.0
        // t=180000: window=[180000] -> MIN=30.0, MAX=30.0, AVG=30.0 (60000 outside 5s window)
        // t=300000: window=[300000] -> MIN=50.0, MAX=50.0, AVG=50.0 (180000 outside 5s window)
        // t=420000: window=[420000] -> MIN=70.0, MAX=70.0, AVG=70.0 (300000 outside 5s window)
        
        assert_eq!(min_column1.value(0), 10.0, "t=60000 MIN should be 10.0");
        assert_eq!(max_column1.value(0), 10.0, "t=60000 MAX should be 10.0");
        assert_eq!(avg_column1.value(0), 10.0, "t=60000 AVG should be 10.0");
        
        assert_eq!(min_column1.value(1), 30.0, "t=180000 MIN should be 30.0");
        assert_eq!(max_column1.value(1), 30.0, "t=180000 MAX should be 30.0");
        assert_eq!(avg_column1.value(1), 30.0, "t=180000 AVG should be 30.0");
        
        assert_eq!(min_column1.value(2), 50.0, "t=300000 MIN should be 50.0");
        assert_eq!(max_column1.value(2), 50.0, "t=300000 MAX should be 50.0");
        assert_eq!(avg_column1.value(2), 50.0, "t=300000 AVG should be 50.0");
        
        assert_eq!(min_column1.value(3), 70.0, "t=420000 MIN should be 70.0");
        assert_eq!(max_column1.value(3), 70.0, "t=420000 MAX should be 70.0");
        assert_eq!(avg_column1.value(3), 70.0, "t=420000 AVG should be 70.0");

        drain_one_pending_message(&mut window_operator).await;
        
        // Step 2: buffer message2, then emit via watermark.
        let wm2 = create_watermark_message(480000);
        let input_stream = Box::pin(futures::stream::iter(vec![message2, wm2]));
        window_operator.set_input(Some(input_stream));

        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue));
        let result2 = window_operator.poll_next().await;
        let message2_result = result2.get_result_message();
        let result_batch2 = message2_result.record_batch();
        assert_eq!(result_batch2.num_rows(), 1, "Should have 1 result row (late rows dropped)");
        
        let min_column2 = result_batch2.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let max_column2 = result_batch2.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        let avg_column2 = result_batch2.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // Rows with ts <= previously emitted watermark(420000) are dropped; only t=480000 remains.
        assert_eq!(min_column2.value(0), 80.0, "t=480000 MIN should be 80.0");
        assert_eq!(max_column2.value(0), 80.0, "t=480000 MAX should be 80.0");
        assert_eq!(avg_column2.value(0), 80.0, "t=480000 AVG should be 80.0");

        drain_one_pending_message(&mut window_operator).await;
        
        // Step 3: buffer message3, then emit via watermark.
        let wm3 = create_watermark_message(542000);
        let input_stream = Box::pin(futures::stream::iter(vec![message3, wm3]));
        window_operator.set_input(Some(input_stream));

        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue));
        let result3 = window_operator.poll_next().await;
        let message3_result = result3.get_result_message();
        let result_batch3 = message3_result.record_batch();
        assert_eq!(result_batch3.num_rows(), 3, "Should have 3 result rows");
        
        let min_column3 = result_batch3.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let max_column3 = result_batch3.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        let avg_column3 = result_batch3.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // Expected results (should use tiles for efficiency with overlapping windows):
        // t=540000: window=[540000] -> MIN=90.0, MAX=90.0, AVG=90.0
        // t=541000: window=[540000,541000] -> MIN=90.0, MAX=100.0, AVG=95.0 (within 5s window)
        // t=542000: window=[540000,541000,542000] -> MIN=90.0, MAX=110.0, AVG=100.0 (all within 5s window)
        
        assert_eq!(min_column3.value(0), 90.0, "t=540000 MIN should be 90.0");
        assert_eq!(max_column3.value(0), 90.0, "t=540000 MAX should be 90.0");
        assert_eq!(avg_column3.value(0), 90.0, "t=540000 AVG should be 90.0");
        
        assert_eq!(min_column3.value(1), 90.0, "t=541000 MIN should be 90.0");
        assert_eq!(max_column3.value(1), 100.0, "t=541000 MAX should be 100.0");
        assert_eq!(avg_column3.value(1), 95.0, "t=541000 AVG should be 95.0");
        
        assert_eq!(min_column3.value(2), 90.0, "t=542000 MIN should be 90.0");
        assert_eq!(max_column3.value(2), 110.0, "t=542000 MAX should be 110.0");
        assert_eq!(avg_column3.value(2), 100.0, "t=542000 AVG should be 100.0");

        drain_one_pending_message(&mut window_operator).await;
        
        // Step 4: buffer message4, then emit via watermark.
        let wm4 = create_watermark_message(542000);
        let input_stream = Box::pin(futures::stream::iter(vec![message4, wm4]));
        window_operator.set_input(Some(input_stream));

        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue));
        let result4 = window_operator.poll_next().await;
        let message4_result = result4.get_result_message();
        let result_batch4 = message4_result.record_batch();
        assert_eq!(result_batch4.num_rows(), 0, "Should have 0 result rows for partition B (late rows dropped)");

        drain_one_pending_message(&mut window_operator).await;
        
        window_operator.close().await.expect("Should be able to close operator");
    }

    #[tokio::test]
    async fn test_pruning_with_lateness() {
        // Test pruning with 5 different aggregates over 3 window types and lateness configuration
        let sql = "SELECT 
            timestamp,
            value,
            partition_key,
            SUM(value) OVER w1 as sum_2s,
            AVG(value) OVER w2 as avg_5s,
            MIN(value) OVER w2 as min_5s,
            COUNT(value) OVER w3 as count_3rows,
            MAX(value) OVER w3 as max_3rows
        FROM test_table
        WINDOW 
            w1 AS (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW),
            w2 AS (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '5000' MILLISECOND PRECEDING AND CURRENT ROW),
            w3 AS (PARTITION BY partition_key ORDER BY timestamp ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)";
        
        let window_exec = extract_window_exec_from_sql(sql).await;
        let mut window_config = WindowOperatorConfig::new(window_exec);
        window_config.parallelize = true;
        window_config.lateness = Some(3000); // 3 seconds lateness
        
        let operator_config = OperatorConfig::WindowConfig(window_config);
        let runtime_context = create_test_runtime_context();
        
        let mut window_operator = WindowOperator::new(operator_config);
        window_operator.open(&runtime_context).await.expect("Should be able to open operator");

        let partition_key = create_test_key("A");
        
        // Create all test messages
        let batch1 = create_test_batch(
            vec![10000, 15000, 20000], // 10s, 15s, 20s
            vec![100.0, 150.0, 200.0], 
            vec!["A", "A", "A"]
        );
        let message1 = create_keyed_message(batch1, "A");
        
        let batch2 = create_test_batch(
            vec![16000, 18000, 25000], // 16s/18s will be dropped (<= watermark), 25s on time
            vec![160.0, 180.0, 250.0], 
            vec!["A", "A", "A"]
        );
        let message2 = create_keyed_message(batch2, "A");
        
        let batch3 = create_test_batch(
            vec![26000, 27000, 28000, 29000, 30000], // Close to 25000ms - clustered events
            vec![260.0, 270.0, 280.0, 290.0, 300.0], 
            vec!["A", "A", "A", "A", "A"]
        );
        let message3 = create_keyed_message(batch3, "A");
        
        // Step 1: buffer message1, then emit via watermark.
        let wm1 = create_watermark_message(20000);
        let input_stream = Box::pin(futures::stream::iter(vec![message1, wm1]));
        window_operator.set_input(Some(input_stream));

        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue));
        let result1 = window_operator.poll_next().await;
        let message1_result = result1.get_result_message();
        let result_batch1 = message1_result.record_batch();
        assert_eq!(result_batch1.num_rows(), 3, "Should have 3 result rows");
        
        // Verify aggregate calculations for the last event (20000ms)
        let sum_column1 = result_batch1.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let avg_column1 = result_batch1.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        let min_column1 = result_batch1.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        let count_column1 = result_batch1.column(6).as_any().downcast_ref::<Int64Array>().unwrap();
        let max_column1 = result_batch1.column(7).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // For event at 20000ms:
        // w1 (2s range): includes 20000ms only -> sum = 200.0
        // w2 (5s range): includes 15000ms, 20000ms -> avg = (150.0 + 200.0) / 2 = 175.0, min = 150.0
        // w3 (3 rows): includes all 3 events: 10000ms, 15000ms, 20000ms -> count = 3, max = 200.0
        assert_eq!(sum_column1.value(2), 200.0, "2s window should include only current event");
        assert_eq!(avg_column1.value(2), 175.0, "5s window should include 15000ms and 20000ms");
        assert_eq!(min_column1.value(2), 150.0, "5s window min should be 150.0");
        assert_eq!(count_column1.value(2), 3, "3-row window should include all 3 events");
        assert_eq!(max_column1.value(2), 200.0, "3-row window max should be 200.0");
        
        // Verify state after step 1 - pruning should occur
        // w1 (2s range): cutoff = 20000 - (3000 + 2000) = 15000ms
        // w2 (5s range): cutoff = 20000 - (3000 + 5000) = 12000ms  
        // w3 (3 rows): Search timestamp = 20000 - 3000 = 17000ms
        //              Last entry <= 17000ms is 15000ms
        //              For 3-row window ending at 15000ms, window start is 10000ms (last 3: [10000, 15000] - only 2 entries, so cutoff is 0)
        //              So w3 cutoff = 10ms
        // Minimal cutoff = min(15000, 12000, 0) = 0 - no actual pruning
        window_operator.get_state().verify_pruning_for_testing(&partition_key, 0).await;

        drain_one_pending_message(&mut window_operator).await;

        // Step 2: buffer message2, then emit via watermark.
        let wm2 = create_watermark_message(25000);
        let input_stream = Box::pin(futures::stream::iter(vec![message2, wm2]));
        window_operator.set_input(Some(input_stream));

        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue));
        let result2 = window_operator.poll_next().await;
        let message2_result = result2.get_result_message();
        let result_batch2 = message2_result.record_batch();
        // Rows with ts <= previously emitted watermark(20000) are dropped; only t=25000 remains.
        assert_eq!(result_batch2.num_rows(), 1, "Should have 1 result row (2 events dropped)");
        
        // Verify the window calculations for the last event (25000ms)
        let sum_column = result_batch2.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let avg_column = result_batch2.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        let min_column = result_batch2.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        let count_column = result_batch2.column(6).as_any().downcast_ref::<Int64Array>().unwrap();
        let max_column = result_batch2.column(7).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // For event at 25000ms:
        // w1 (2s range): includes 25000ms only -> sum = 250.0
        // w2 (5s range): includes 20000ms, 25000ms -> avg = (200.0 + 250.0) / 2 = 225.0, min = 200.0
        // w3 (3 rows): includes last 3 events: 15000ms, 20000ms, 25000ms -> count = 3, max = 250.0
        assert_eq!(sum_column.value(0), 250.0, "2s window should include only current event");
        assert_eq!(avg_column.value(0), 225.0, "5s window should include 20000ms and 25000ms");
        assert_eq!(min_column.value(0), 200.0, "5s window min should be 200.0");
        assert_eq!(count_column.value(0), 3, "3-row window should include last 3 events");
        assert_eq!(max_column.value(0), 250.0, "3-row window max should be 250.0");
        
        // Verify state after step 2 - additional pruning should occur
        // After step 2, we have entries: [10000, 15000, 20000, 25000] (16000/18000 dropped)
        // w1 (2s range): cutoff = 25000 - (3000 + 2000) = 20000ms
        // w2 (5s range): cutoff = 25000 - (3000 + 5000) = 17000ms
        // w3 (3 rows): Search timestamp = 25000 - 3000 = 22000ms
        //              Last entry <= 22000ms is 20000ms
        //              For 3-row window ending at 20000ms, window start is 10000ms (last 3: [10000, 15000, 20000])
        //              So w3 cutoff = 15000ms  
        // Minimal cutoff = min(20000, 17000, 10000) = 10000ms
        window_operator.get_state().verify_pruning_for_testing(&partition_key, 10000).await;

        drain_one_pending_message(&mut window_operator).await;

        // Step 3: buffer message3, then emit via watermark.
        let wm3 = create_watermark_message(30000);
        let input_stream = Box::pin(futures::stream::iter(vec![message3, wm3]));
        window_operator.set_input(Some(input_stream));

        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue));
        let result3 = window_operator.poll_next().await;
        let message3_result = result3.get_result_message();
        let result_batch3 = message3_result.record_batch();
        assert_eq!(result_batch3.num_rows(), 5, "Should have 5 result rows");
        
        // Verify aggregate calculations for the last event (30000ms)
        let sum_column3 = result_batch3.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let avg_column3 = result_batch3.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        let min_column3 = result_batch3.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        let count_column3 = result_batch3.column(6).as_any().downcast_ref::<Int64Array>().unwrap();
        let max_column3 = result_batch3.column(7).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // For event at 30000ms:
        // w1 (2s range): window [28000, 30000] includes 28000ms, 29000ms, 30000ms -> sum = 280.0 + 290.0 + 300.0 = 870.0
        // w2 (5s range): window [25000, 30000] includes 25000ms, 26000ms, 27000ms, 28000ms, 29000ms, 30000ms -> avg = (250+260+270+280+290+300)/6 = 275.0, min = 250.0
        // w3 (3 rows): includes last 3 events: 28000ms, 29000ms, 30000ms -> count = 3, max = 300.0
        assert_eq!(sum_column3.value(4), 870.0, "2s window should include 28000ms, 29000ms and 30000ms (exact window boundary)");
        assert_eq!(avg_column3.value(4), 275.0, "5s window should include events from 25000ms to 30000ms (exact window boundary)");
        assert_eq!(min_column3.value(4), 250.0, "5s window min should be 250.0 (from 25000ms at exact boundary)");
        assert_eq!(count_column3.value(4), 3, "3-row window should include last 3 events");
        assert_eq!(max_column3.value(4), 300.0, "3-row window max should be 300.0");
        
        // Verify state after step 3 - range window should now be the limiting factor
        // After step 3, we have entries: [15000, 20000, 25000, 26000, 27000, 28000, 29000, 30000]
        // w1 (2s range): cutoff = 30000 - (3000 + 2000) = 25000ms
        // w2 (5s range): cutoff = 30000 - (3000 + 5000) = 22000ms
        // w3 (3 rows): Search timestamp = 30000 - 3000 = 27000ms
        //              Last entry <= 27000ms is 27000ms
        //              For 3-row window ending at 27000ms, window start is 25000ms (last 3: [25000, 26000, 27000])
        //              So w3 cutoff = 25000ms
        // Minimal cutoff = min(25000, 22000, 25000) = 22000ms
        window_operator.get_state().verify_pruning_for_testing(&partition_key, 22000).await;

        drain_one_pending_message(&mut window_operator).await;
        
        window_operator.close().await.expect("Should be able to close operator");
    }

    #[tokio::test]
    async fn test_request_mode() {
        // Test ExecutionMode::Request - operator should update state but produce no messages
        // Use both plain (MIN, MAX) and retractable (SUM, COUNT, AVG) aggregates with tiling
        let sql = "SELECT 
            timestamp,
            value,
            partition_key,
            SUM(value) OVER w as sum_val,
            COUNT(value) OVER w as count_val,
            AVG(value) OVER w as avg_val,
            MIN(value) OVER w as min_val,
            MAX(value) OVER w as max_val
        FROM test_table
        WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '3000' MILLISECOND PRECEDING AND CURRENT ROW)";
        
        let window_exec = extract_window_exec_from_sql(sql).await;
        let mut window_config = WindowOperatorConfig::new(window_exec);
        window_config.execution_mode = ExecutionMode::Request;
        window_config.parallelize = true;
        
        // Set up tiling configs for MIN and MAX (plain aggregates)
        use crate::runtime::operators::window::state::tiles::{TileConfig, TimeGranularity};
        let tile_config = TileConfig::new(vec![
            TimeGranularity::Minutes(1),
            TimeGranularity::Minutes(5),
        ]).expect("Should create tile config");
        
        // Apply tiling to MIN and MAX (plain aggregates), not to retractable ones
        window_config.tiling_configs = vec![
            None, // SUM - retractable, no tiling
            None, // COUNT - retractable, no tiling
            None, // AVG - retractable, no tiling
            Some(tile_config.clone()), // MIN - plain, with tiling
            Some(tile_config.clone()), // MAX - plain, with tiling
        ];
        
        let operator_config = OperatorConfig::WindowConfig(window_config);
        let runtime_context = create_test_runtime_context();
        
        let mut window_operator = WindowOperator::new(operator_config);
        window_operator.open(&runtime_context).await.expect("Should be able to open operator");
        
        let partition_key = create_test_key("A");
        
        // Step 1: Process initial batch [1000, 2000, 3000]
        let batch1 = create_test_batch(vec![1000, 2000, 3000], vec![10.0, 20.0, 30.0], vec!["A", "A", "A"]);
        let message1 = create_keyed_message(batch1, "A");
        
        let watermark1 = create_watermark_message(3000);
        let input_stream = Box::pin(futures::stream::iter(vec![message1, watermark1]));
        window_operator.set_input(Some(input_stream));
        
        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue), "Request mode should buffer");
        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue), "Request mode should produce no output on watermark");
        drain_one_pending_message(&mut window_operator).await;
    
        // Verify state was updated
        let state1_guard = window_operator
            .get_state()
            .get_windows_state(&partition_key)
            .await
            .expect("State should exist");
        let state1 = state1_guard.value();
        assert_eq!(state1.bucket_index.total_rows(), 3, "Should have 3 rows");
        
        // Verify accumulator states were updated for retractable aggregates
        let window_state_w1 = state1.window_states.get(&0).expect("Window state should exist");
        assert!(window_state_w1.accumulator_state.is_some(), "Accumulator state should be updated");
        let window_state_w2 = state1.window_states.get(&1).expect("Window state should exist");
        assert!(window_state_w2.accumulator_state.is_some(), "Accumulator state should be updated");
        let window_state_w3 = state1.window_states.get(&2).expect("Window state should exist");
        assert!(window_state_w3.accumulator_state.is_some(), "Accumulator state should be updated");
        
        // Drop guard before proceeding to next step
        drop(state1_guard);
        
        // Step 2: Process late entry [1500] - should be dropped (ts <= watermark)
        let batch2 = create_test_batch(vec![1500], vec![15.0], vec!["A"]);
        let message2 = create_keyed_message(batch2, "A");
        
        let watermark2 = create_watermark_message(3000);
        let input_stream2 = Box::pin(futures::stream::iter(vec![message2, watermark2]));
        window_operator.set_input(Some(input_stream2));
        
        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue));
        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue));
        drain_one_pending_message(&mut window_operator).await;
        
        // Verify late entry was dropped
        let state2_guard = window_operator
            .get_state()
            .get_windows_state(&partition_key)
            .await
            .expect("State should exist");
        let state2 = state2_guard.value();
        assert_eq!(state2.bucket_index.total_rows(), 3, "Should still have 3 rows after dropping late entry");
        
        // Verify accumulator state is still present
        let window_state2 = state2.window_states.get(&0).expect("Window state should exist");
        assert!(window_state2.accumulator_state.is_some(), "Accumulator state should still exist");
        
        // Drop guard before proceeding to next step
        drop(state2_guard);
        
        // Step 3: Process more entries [4000, 5000]
        let batch3 = create_test_batch(vec![4000, 5000], vec![40.0, 50.0], vec!["A", "A"]);
        let message3 = create_keyed_message(batch3, "A");
        
        let watermark3 = create_watermark_message(5000);
        let input_stream3 = Box::pin(futures::stream::iter(vec![message3, watermark3]));
        window_operator.set_input(Some(input_stream3));
        
        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue));
        assert!(matches!(window_operator.poll_next().await, OperatorPollResult::Continue));
        drain_one_pending_message(&mut window_operator).await;
        
        // Verify state was updated
        let state3_guard = window_operator
            .get_state()
            .get_windows_state(&partition_key)
            .await
            .expect("State should exist");
        let state3 = state3_guard.value();
        assert_eq!(state3.bucket_index.total_rows(), 5, "Should have 5 rows");
        
        // Verify window positions advanced
        let window_state3_w3 = state3.window_states.get(&3).expect("Window state should exist");
        assert_eq!(
            window_state3_w3.processed_until.map(|p| p.ts).unwrap_or(i64::MIN),
            5000,
            "Window processed_until.ts should advance to latest entry"
        );
        let window_state3_w4 = state3.window_states.get(&4).expect("Window state should exist");
        
        // Verify tiles were updated (for MIN and MAX)
        assert!(window_state3_w3.tiles.is_some(), "Tiles should exist for MIN aggregates");
        assert!(window_state3_w4.tiles.is_some(), "Tiles should exist for MAX aggregates");
        
        window_operator.close().await.expect("Should be able to close operator");
    }

}