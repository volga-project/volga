use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::fmt;

use anyhow::Result;
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Schema, SchemaBuilder, SchemaRef};
use async_trait::async_trait;
use futures::future;
use indexmap::{IndexMap, IndexSet};
use tokio_rayon::rayon::{ThreadPool, ThreadPoolBuilder};

use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use crate::common::message::Message;
use crate::common::Key;
use crate::runtime::operators::operator::{MessageStream, OperatorBase, OperatorConfig, OperatorPollResult, OperatorTrait, OperatorType};
use crate::runtime::operators::window::aggregates::{get_aggregate_type, split_entries_for_parallelism, Aggregation};
use crate::runtime::operators::window::aggregates::plain::PlainAggregation;
use crate::runtime::operators::window::aggregates::retractable::RetractableAggregation;
use crate::runtime::operators::window::window_operator_state::{AccumulatorState, WindowOperatorState, WindowId};
use crate::runtime::operators::window::time_entries::TimeIdx;
use crate::runtime::operators::window::{AggregatorType, TileConfig};
use crate::runtime::runtime_context::RuntimeContext;
use crate::storage::batch_store::{BatchId, BatchStore};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionMode {
    Regular, // operator produces messages
    Request, // operator only updates state, produces no messages
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateMode {
    PerMessage, // all events are processed immediately, late events are handled with best effort
    PerWatermark, // events are buffered until watermark is reached, late events are dropped
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
    pub update_mode: UpdateMode,
    pub execution_mode: ExecutionMode,
    pub parallelize: bool,
    pub tiling_configs: Vec<Option<TileConfig>>,
    pub lateness: Option<i64>,
}

impl WindowOperatorConfig {
    pub fn new(window_exec: Arc<BoundedWindowAggExec>) -> Self {    
        Self {
            window_exec,
            update_mode: UpdateMode::PerMessage,
            execution_mode: ExecutionMode::Regular,
            parallelize: false,
            tiling_configs: Vec::new(),
            lateness: None
        }
    }
}

pub struct WindowOperator {
    base: OperatorBase,
    window_configs: BTreeMap<WindowId, WindowConfig>,
    state: Arc<WindowOperatorState>,
    ts_column_index: usize,
    buffered_keys: HashSet<Key>,
    update_mode: UpdateMode,
    execution_mode: ExecutionMode,
    parallelize: bool,
    thread_pool: Option<ThreadPool>,
    output_schema: SchemaRef,
    input_schema: SchemaRef,
    tiling_configs: Vec<Option<TileConfig>>,
    lateness: Option<i64>,
}

impl fmt::Debug for WindowOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WindowOperator")
            .field("base", &self.base)
            .field("windows", &self.window_configs)
            .field("state", &self.state)
            .field("ts_column_index", &self.ts_column_index)
            .field("buffered_keys", &self.buffered_keys)
            .field("update_mode", &self.update_mode)
            .field("execution_mode", &self.execution_mode)
            .field("parallelize", &self.parallelize)
            .field("thread_pool", &self.thread_pool)
            .field("output_schema", &self.output_schema)
            .field("input_schema", &self.input_schema)
            .field("tiling_configs", &self.tiling_configs)
            .field("lateness", &self.lateness)
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

        let (ts_column_index, windows, input_schema, output_schema, thread_pool) = init(
            false, &window_operator_config.window_exec, &window_operator_config.tiling_configs, window_operator_config.parallelize
        );

        Self {
            base: OperatorBase::new(config),
            window_configs: windows,
            state: Arc::new(WindowOperatorState::new(Arc::new(BatchStore::default()))), // Temporary, will be replaced in open()
            ts_column_index,
            buffered_keys: HashSet::new(),
            update_mode: window_operator_config.update_mode,
            execution_mode: window_operator_config.execution_mode,
            parallelize: window_operator_config.parallelize,
            thread_pool,
            output_schema,
            input_schema,
            tiling_configs: window_operator_config.tiling_configs,
            lateness: window_operator_config.lateness,
        }
    }

    async fn process_key(&self, key: &Key, new_entries: Option<Vec<TimeIdx>>) -> RecordBatch {
        
        let result = self.advance_windows(key, new_entries).await;

        // TODO should pruning be done with updating state, while same lock is held
        if self.lateness.is_some() {
            // prune if lateness is configured
            self.state.prune(key, self.lateness.unwrap(), &self.window_configs).await;
        }
        result
    }

    pub fn get_state(&self) -> &WindowOperatorState {
        &self.state
    }


    async fn process_buffered(&self) -> RecordBatch {
        // TODO limit number of concurrent keys to process
        let futures: Vec<_> = self.buffered_keys.iter()
            .map(|key| async move {
                self.process_key(key, None).await
            })
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
        new_entries: Option<Vec<TimeIdx>>,
    ) -> (IndexMap<WindowId, Vec<Box<dyn Aggregation>>>, HashMap<WindowId, (TimeIdx, TimeIdx)>) {
        // State should exist because insert_batch creates it if needed
        let windows_state_guard = self.state.get_windows_state(key).await
            .expect("Windows state should exist - insert_batch should have created it");
        let windows_state = windows_state_guard.value();
        
        let time_entries = &windows_state.time_entries;
        let latest_time_idx = time_entries.latest_idx();

        let mut aggregations: IndexMap<WindowId, Vec<Box<dyn Aggregation>>> = IndexMap::new();
        let mut new_window_positions = HashMap::new();
        
        // Advance window positions to latest stored timestamp and configure aggregations for each window
        for (window_id, window_config) in &self.window_configs {
            let window_frame = window_config.window_expr.get_window_frame();
            let window_state = windows_state.window_states.get(window_id).expect("Window state should exist");
            let aggregator_type = self.window_configs[window_id].aggregator_type;
            let retractable = aggregator_type == AggregatorType::RetractableAccumulator;
            let tiles_owned = window_state.tiles.clone();
            let accumulator_state_owned = window_state.accumulator_state.clone();
            let start_idx = window_state.start_idx;
            let end_idx = window_state.end_idx;
            
            let (updates, retracts, new_window_start, new_window_end) = time_entries.slide_window_position(
                window_frame, 
                start_idx, 
                end_idx, 
                latest_time_idx, // slide to the latest
                retractable
            );

            // Store new positions for later update
            new_window_positions.insert(*window_id, (new_window_start, new_window_end));

            let mut aggs: Vec<Box<dyn Aggregation>> = Vec::new();

            if self.execution_mode == ExecutionMode::Regular {
                match aggregator_type {
                    AggregatorType::RetractableAccumulator => {
                        if let Some(new_entries) = &new_entries {
                            // Late entries are handled without retraction, so use PlainAggregation
                            let late_entries = get_late_entries(new_entries.clone(), end_idx);

                            if let Some(late_entries) = late_entries {

                                println!("Late len: {:?}",late_entries.len());
                                for lates in split_entries_for_parallelism(&late_entries) {
                                    aggs.push(Box::new(PlainAggregation::new(
                                        lates, window_config.window_expr.clone(), tiles_owned.clone(), time_entries
                                    )));
                                }
                            }
                        }   
                        // Non-late entries should match updates - they are already inserted, but not processed since the last window
                        // We use RetractableAggregation since they are sorted by timestamp
                        aggs.push(Box::new(RetractableAggregation::new(
                            updates, window_config.window_expr.clone(), tiles_owned.clone(), Some(retracts), accumulator_state_owned.clone()
                        )));
                    }
                    AggregatorType::PlainAccumulator => {
                        // use new entries if available, otherwise use updates
                        let entries = if let Some(new_entries) = &new_entries {
                            new_entries
                        } else {
                            &updates
                        };

                        for entries in split_entries_for_parallelism(&entries) {
                            aggs.push(Box::new(PlainAggregation::new(
                                entries, window_config.window_expr.clone(), tiles_owned.clone(), time_entries
                            )));
                        }

                    }
                    AggregatorType::Evaluator => {
                        panic!("Evaluator is not supported yet");
                    }
                }
            } else {
                // request mode handles only retractable aggs - only they update state
                match aggregator_type {
                    AggregatorType::RetractableAccumulator => {
                        aggs.push(Box::new(RetractableAggregation::new(
                            updates, window_config.window_expr.clone(), tiles_owned.clone(), Some(retracts), accumulator_state_owned.clone()
                        )));
                    }
                    _ => {}
                }
            }
            if !aggs.is_empty() {
                aggregations.insert(*window_id, aggs);
            }
        }

        drop(windows_state_guard);
        (aggregations, new_window_positions)
    }

    async fn advance_windows(
        &self, 
        key: &Key, 
        new_entries: Option<Vec<TimeIdx>>
    ) -> RecordBatch {

        let (aggregations, new_window_positions) = self.configure_aggregations(key, new_entries).await;
        
        // Get aggregated_values_idxs - time indexes of aggregated values
        // This is used to get input values
        let aggregated_values_idxs: Vec<TimeIdx> = aggregations
            .values()
            .next()
            .map(|args_vec| {
                args_vec
                    .iter()
                    .flat_map(|arg| arg.entries().iter().cloned())
                    .collect()
            })
            .expect("Should be able to get aggregated values idxs");
        
        // Load batches
        let batches = load_batches(self.state.get_batch_store(), key, &aggregations).await;
        
        // Run aggregation
        let aggregation_results = produce_aggregates(&self.window_configs, &aggregations, &batches, self.thread_pool.as_ref()).await;
        
        // Collect accumulator states before mutating windows_state
        let accumulator_states: HashMap<WindowId, Option<AccumulatorState>> = aggregation_results
            .iter()
            .map(|(window_id, results)| {
                let accumulator_state = results.last().and_then(|(_, state)| state.clone());
                (*window_id, accumulator_state)
            })
            .collect();
        
        // Drop aggregations to release borrows
        drop(aggregations);
        
        // Update window state positions and accumulator states
        self.state.update_window_positions_and_accumulators(key, &new_window_positions, &accumulator_states).await;

        if self.execution_mode == ExecutionMode::Request {
            // return empty batch - produce no records
            return RecordBatch::new_empty(self.output_schema.clone());
        }

        // Get input values (original values which were not aggregated)
        let input_values = get_input_values(&aggregated_values_idxs, &batches, &self.input_schema);

        // For each window in aggregator_results, collect/flatten all aggregates into Vec<Vec<_>>
        let aggregated_values: Vec<Vec<ScalarValue>> = aggregation_results
            .values()
            .map(|results| {
                results
                    .iter()
                    .flat_map(|(aggregates, _)| aggregates.clone())
                    .collect()
            })
            .collect();
        
        // Stack input value rows and result rows producing single result batch
        stack_concat_results(input_values, aggregated_values, &self.output_schema, &self.input_schema)
    }
}

pub async fn load_batches(storage: &BatchStore, key: &Key, aggregations: &IndexMap<WindowId, Vec<Box<dyn Aggregation>>>) -> HashMap<BatchId, RecordBatch> {
    let mut batches_to_load = IndexSet::new(); // preserve insertion order
    for (_window_id, aggs) in aggregations.iter() {
        for agg in aggs {
            let agg_batches = agg.get_batches_to_load();
            for batch_id in agg_batches {
                batches_to_load.insert(batch_id);
            }
        }
    }
    
    storage.load_batches(batches_to_load.into_iter().collect(), key).await
}

pub async fn produce_aggregates(window_configs: &BTreeMap<WindowId, WindowConfig>, aggregations: &IndexMap<WindowId, Vec<Box<dyn Aggregation>>>, batches: &HashMap<BatchId, RecordBatch>, thread_pool: Option<&ThreadPool>) -> IndexMap<WindowId, Vec<(Vec<ScalarValue>, Option<AccumulatorState>)>> {
    let futures: Vec<_> = aggregations.iter()
        .map(|(window_id, aggs)| {
            let window_id = *window_id;
            let window_config = window_configs.get(&window_id).expect("Window config should exist");
            let batches = batches;
            let thread_pool = thread_pool;
            
            async move {
                let mut results = Vec::new();
                for agg in aggs {
                    let result = agg.produce_aggregates(batches, thread_pool, window_config.exclude_current_row).await;
                    results.push(result);
                }
                (window_id, results)
            }
        })
        .collect();
        
    let results = future::join_all(futures).await;
    results.into_iter().collect()
}

#[async_trait]
impl OperatorTrait for WindowOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await?;
        
        let vertex_id = context.vertex_id().to_string();
        let operator_states = context.operator_states();

        operator_states.insert_operator_state(vertex_id, self.state.clone());
        
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
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
                
                let ingest_ts = message.ingest_timestamp();
                let extras = message.get_extras();
                match message {
                    Message::Keyed(keyed_message) => {
                        let key = keyed_message.key();

                        let new_entries = self.state.insert_batch(key, &self.window_configs, &self.tiling_configs, self.ts_column_index, self.lateness, keyed_message.base.record_batch.clone()).await;
                        if self.update_mode == UpdateMode::PerWatermark {
                            // buffer for processing on watermark
                            self.buffered_keys.insert(key.clone());
                            return OperatorPollResult::Continue;
                        } else {
                            // immidiate processing
                            let result = self.process_key(&key, Some(new_entries)).await;
                            if self.execution_mode == ExecutionMode::Request {
                                // request mode produces no output, just updates state
                                return OperatorPollResult::Continue;
                            } else {
                                // vertex_id will be set by stream task
                                return OperatorPollResult::Ready(Message::new(None, result, ingest_ts, extras));
                            }
                        }
                    }
                    Message::Watermark(watermark) => {
                        let vertex_id = self.base.runtime_context.as_ref().unwrap().vertex_id().to_string();
                        println!("[{}] Window operator received watermark: {:?}", vertex_id, watermark);
                        
                        if self.update_mode == UpdateMode::PerMessage {
                            // pass through
                            return OperatorPollResult::Ready(Message::Watermark(watermark));
                        }
                        // TODO we should respect lateness and process only within proper range

                        let result = self.process_buffered().await;
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
        let state_cp = self.state.to_checkpoint();
        let batch_store_cp = self.state.get_batch_store().to_checkpoint();

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

        let batch_store = if let Some(bytes) = batch_store_bytes {
            let cp: crate::storage::batch_store::BatchStoreCheckpoint = bincode::deserialize(bytes)?;
            Arc::new(crate::storage::batch_store::BatchStore::from_checkpoint(cp))
        } else {
            panic!("Batch store bytes are missing");
        };

        let restored_state = Arc::new(WindowOperatorState::new(batch_store));
        if let Some(bytes) = state_bytes {
            let cp: crate::runtime::operators::window::window_operator_state::WindowOperatorStateCheckpoint =
                bincode::deserialize(bytes)?;
            restored_state.apply_checkpoint(cp, &self.window_configs, &self.tiling_configs);
        } else {
            panic!("Window operator state bytes are missing");
        }

        self.state = restored_state;
        Ok(())
    }
}

pub fn drop_too_late_entries(record_batch: &RecordBatch, ts_column_index: usize, lateness_ms: i64, last_entry: TimeIdx) -> RecordBatch {
    use arrow::compute::{filter_record_batch};
    use arrow::compute::kernels::cmp::gt_eq;
    use arrow::array::{TimestampMillisecondArray, Scalar};
    
    let ts_column = record_batch.column(ts_column_index);
    
    // Calculate cutoff timestamp: last_entry.timestamp - lateness_ms
    let cutoff_timestamp = last_entry.timestamp - lateness_ms;
    
    // Create scalar array with the cutoff timestamp using the same data type as the timestamp column
    let cutoff_array = TimestampMillisecondArray::from_value(cutoff_timestamp, 1);
    let cutoff_scalar = Scalar::new(&cutoff_array);
    
    // Create boolean mask - keep rows where timestamp >= cutoff
    let keep_mask = gt_eq(ts_column, &cutoff_scalar)
        .expect("Should be able to compare with cutoff timestamp");
    
    // Check if all rows should be kept
    if keep_mask.true_count() == record_batch.num_rows() {
        record_batch.clone()
    } else {
        // Filter the batch using the boolean mask
        filter_record_batch(record_batch, &keep_mask)
            .expect("Should be able to filter record batch")
    }
}

pub fn get_late_entries(entries: Vec<TimeIdx>, last_entry: TimeIdx) -> Option<Vec<TimeIdx>> {
    let lates = entries.into_iter().filter(|idx| is_entry_late(*idx, last_entry)).collect::<Vec<_>>();
    if lates.len() > 0 {
        Some(lates)
    } else {
        None
    }
}

pub fn is_entry_late(entry: TimeIdx, last_entry: TimeIdx) -> bool {
    entry < last_entry
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

// Extract input values (values which were in original argument batch, but were not aggregated, e.g keys) 
// for each update row.
// We assume window operator schema is fixed: input columns first, then window columns
fn get_input_values(
    result_idxs: &Vec<TimeIdx>, 
    batches: &HashMap<BatchId, RecordBatch>, 
    input_schema: &SchemaRef
) -> Vec<Vec<ScalarValue>> {
    let mut input_values = Vec::new();
        
    let input_column_count = input_schema.fields().len();
    
    for result_idx in result_idxs {
        let batch: &RecordBatch = batches.get(&result_idx.batch_id).expect("Batch should exist");
        let row_idx = result_idx.row_idx;
        
        let mut row_input_values = Vec::new();
        for col_idx in 0..input_column_count {
            let array = batch.column(col_idx);
            let scalar_value = datafusion::common::ScalarValue::try_from_array(array, row_idx)
                .expect("Should be able to extract scalar value");
            row_input_values.push(scalar_value);
        }
        input_values.push(row_input_values);
    }
    input_values
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
    use crate::common::message::Message;
    use crate::runtime::runtime_context::RuntimeContext;
    use crate::runtime::state::OperatorStates;
    use crate::common::Key;

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

    fn create_keyed_message(batch: RecordBatch, partition_key: &str) -> Message {
        let key = create_test_key(partition_key);
        Message::new_keyed(None, batch, key, None, None)
    }

    fn create_test_runtime_context() -> RuntimeContext {
        RuntimeContext::new(
            "test_vertex".to_string(),
            0,
            1,
            None,
            Some(Arc::new(OperatorStates::new())),
            None
        )
    }

    #[tokio::test]
    async fn test_range_window_event_based_update() {
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
        window_config.update_mode = UpdateMode::PerMessage;
        window_config.parallelize = true;

        let operator_config = OperatorConfig::WindowConfig(window_config);
        
        let mut window_operator = WindowOperator::new(operator_config);
        let runtime_context = create_test_runtime_context();
        window_operator.open(&runtime_context).await.expect("Should be able to open operator");
        
        // Create all test messages
        let batch1 = create_test_batch(vec![1000], vec![10.0], vec!["A"]);
        let message1 = create_keyed_message(batch1, "A");
        
        let batch2 = create_test_batch(vec![1500, 2000], vec![30.0, 20.0], vec!["A", "A"]);
        let message2 = create_keyed_message(batch2, "A");
        
        let batch3 = create_test_batch(vec![3200], vec![5.0], vec!["A"]);
        let message3 = create_keyed_message(batch3, "A");
        
        let batch4 = create_test_batch(vec![1000, 2000], vec![100.0, 200.0], vec!["B", "B"]);
        let message4 = create_keyed_message(batch4, "B");
        
        let input_stream = Box::pin(futures::stream::iter(vec![message1, message2, message3, message4]));
        window_operator.set_input(Some(input_stream));
        
        // Process first message
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
        
        // Process second message (multi-row batch)
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
        
        // Process third message
        let result3 = window_operator.poll_next().await;
        
        let message3_result = result3.get_result_message();
        let result_batch3 = message3_result.record_batch();
        assert_eq!(result_batch3.num_rows(), 1, "Should have 1 result row");
        
        let sum_column3 = result_batch3.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let count_column3 = result_batch3.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        
        // Window now includes t=1500,2000,3200 (t=1000 excluded) -> SUM=55.0, COUNT=3
        assert_eq!(sum_column3.value(0), 55.0, "SUM at t=3200 should be 55.0 (30.0+20.0+5.0)");
        assert_eq!(count_column3.value(0), 3, "COUNT at t=3200 should be 3");
        
        // Process fourth message (different partition)
        let result4 = window_operator.poll_next().await;
        
        let message4_result = result4.get_result_message();
        let result_batch4 = message4_result.record_batch();
        assert_eq!(result_batch4.num_rows(), 2, "Should have 2 result rows for partition B");
        
        let sum_column4 = result_batch4.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // Partition B should have independent window state
        assert_eq!(sum_column4.value(0), 100.0, "SUM for partition B at t=1000 should be 100.0");
        assert_eq!(sum_column4.value(1), 300.0, "SUM for partition B at t=2000 should be 300.0 (100.0+200.0)");
        
        window_operator.close().await.expect("Should be able to close operator");
    }

    #[tokio::test]
    async fn test_rows_window_event_based_update() {
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
        window_config.update_mode = UpdateMode::PerMessage;
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
        
        let input_stream = Box::pin(futures::stream::iter(vec![message]));
        window_operator.set_input(Some(input_stream));
        
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
        
        window_operator.close().await.expect("Should be able to close operator");
    }

    #[tokio::test]
    async fn test_range_window_watermark_update() {
        // Test watermark-based emission mode with alias
        let sql = "SELECT 
            timestamp,
            value,
            partition_key,
            SUM(value) OVER w as sum_val
        FROM test_table
        WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW)";
        
        let window_exec = extract_window_exec_from_sql(sql).await;
        let mut window_config = WindowOperatorConfig::new(window_exec);
        window_config.update_mode = UpdateMode::PerWatermark;
        window_config.parallelize = true;
        
        let operator_config = OperatorConfig::WindowConfig(window_config);
        
        let mut window_operator = WindowOperator::new(operator_config);
        let runtime_context = create_test_runtime_context();
        window_operator.open(&runtime_context).await.expect("Should be able to open operator");
        
        // Send messages - should be buffered until watermark
        let batch1 = create_test_batch(vec![1000], vec![10.0], vec!["test"]);
        let message1 = create_keyed_message(batch1, "test");
        
        let batch2 = create_test_batch(vec![2000], vec![20.0], vec!["test"]);
        let message2 = create_keyed_message(batch2, "test");
        
        // Create watermark message
        let watermark_message = Message::Watermark(crate::common::WatermarkMessage::new(
            "test".to_string(),
            3000,
            Some(0)
        ));
        
        let input_stream = Box::pin(futures::stream::iter(vec![message1, message2, watermark_message]));
        window_operator.set_input(Some(input_stream));
        
        // First two should be OperatorResult::Continue
        window_operator.poll_next().await;
        window_operator.poll_next().await;
        
        // Watermark should trigger processing and return aggregated result
        let watermark_result = window_operator.poll_next().await;
        assert!(matches!(watermark_result, OperatorPollResult::Ready(_)), "Should have results after watermark");
        
        let watermark_message_result = watermark_result.get_result_message();
        let result_batch = watermark_message_result.record_batch();
        assert_eq!(result_batch.num_rows(), 2, "Should have 2 result rows");
        
        let sum_column = result_batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(sum_column.value(0), 10.0, "First SUM should be 10.0");
        assert_eq!(sum_column.value(1), 30.0, "Second SUM should be 30.0 (10.0+20.0)");
        
        // Next call should return the watermark
        let watermark_passthrough = window_operator.poll_next().await;
        assert!(matches!(watermark_passthrough.get_result_message(), Message::Watermark(_)), "Should be a watermark message");
        
        window_operator.close().await.expect("Should be able to close operator");
    }

    #[tokio::test]
    async fn test_late_entries_handling_retractable_window() {
        // Test late entries handling with RANGE window, only retractable aggregates (sum, count)
        let sql = "SELECT 
            timestamp,
            value,
            partition_key,
            SUM(value) OVER w as sum_val,
            COUNT(value) OVER w as count_val
        FROM test_table
        WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW)";
        
        let window_exec = extract_window_exec_from_sql(sql).await;
        let mut window_config = WindowOperatorConfig::new(window_exec);
        window_config.update_mode = UpdateMode::PerMessage;
        window_config.parallelize = true;
        
        let operator_config = OperatorConfig::WindowConfig(window_config);
        let runtime_context = create_test_runtime_context();
        
        let mut window_operator = WindowOperator::new(operator_config);
        window_operator.open(&runtime_context).await.expect("Should be able to open operator");
        
        // Create all test messages
        let batch1 = create_test_batch(vec![1000, 3000, 5000], vec![10.0, 30.0, 50.0], vec!["A", "A", "A"]);
        let message1 = create_keyed_message(batch1, "A");
        
        let batch2 = create_test_batch(vec![2000], vec![20.0], vec!["A"]);
        let message2 = create_keyed_message(batch2, "A");
        
        let batch3 = create_test_batch(vec![6000], vec![60.0], vec!["A"]);
        let message3 = create_keyed_message(batch3, "A");
        
        let input_stream = Box::pin(futures::stream::iter(vec![message1, message2, message3]));
        window_operator.set_input(Some(input_stream));
        
        // Step 1: Process initial batch with timestamps [1000, 3000, 5000]
        let result1 = window_operator.poll_next().await;
        
        let message1_result = result1.get_result_message();
        let result_batch1 = message1_result.record_batch();
        assert_eq!(result_batch1.num_rows(), 3, "Should have 3 result rows");
        
        // Verify initial results
        let sum_column1 = result_batch1.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let count_column1 = result_batch1.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        
        // Expected: [10.0], [10.0+30.0=40.0], [30.0+50.0=80.0] (1000 is outside 2000ms range of 5000)
        assert_eq!(sum_column1.value(0), 10.0, "First SUM should be 10.0");
        assert_eq!(count_column1.value(0), 1, "First COUNT should be 1");
        assert_eq!(sum_column1.value(1), 40.0, "Second SUM should be 40.0 (10.0+30.0)");
        assert_eq!(count_column1.value(1), 2, "Second COUNT should be 2");
        assert_eq!(sum_column1.value(2), 80.0, "Third SUM should be 80.0 (30.0+50.0, 1000 outside window)");
        assert_eq!(count_column1.value(2), 2, "Third COUNT should be 2");
        
        // Step 2: Process late entry with timestamp 2000 (between 1000 and 3000)
        let result2 = window_operator.poll_next().await;
        let message2_result = result2.get_result_message();
        let result_batch2 = message2_result.record_batch();
        // Should include results for the late entry and potentially updated results for affected windows
        assert!(result_batch2.num_rows() >= 1, "Should have at least 1 result row for late entry");
        
        let sum_column2 = result_batch2.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let count_column2 = result_batch2.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        
        // The late entry at t=2000 should create a window result
        // Window at t=2000 should include [1000, 2000] -> SUM=30.0, COUNT=2
        assert_eq!(sum_column2.value(0), 30.0, "Late entry SUM should be 30.0 (10.0+20.0)");
        assert_eq!(count_column2.value(0), 2, "Late entry COUNT should be 2");
        
        // Step 3: Process another batch to verify the late entry is properly integrated
        let result3 = window_operator.poll_next().await;
        let message3_result = result3.get_result_message();
        let result_batch3 = message3_result.record_batch();
        let sum_column3 = result_batch3.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let count_column3 = result_batch3.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        
        // Window at t=6000 should include [4000, 6000] range
        // Should include t=5000 (50.0) and t=6000 (60.0) -> SUM=110.0, COUNT=2
        assert_eq!(sum_column3.value(0), 110.0, "Final SUM should be 110.0 (50.0+60.0)");
        assert_eq!(count_column3.value(0), 2, "Final COUNT should be 2");
        
        window_operator.close().await.expect("Should be able to close operator");
    }

    #[tokio::test]
    async fn test_multiple_late_entries_handling_retractable_window() {
        // Test handling multiple late entries in a single batch, only retractable aggregates (sum)
        let sql = "SELECT 
            timestamp,
            value,
            partition_key,
            SUM(value) OVER w as sum_val
        FROM test_table
        WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)";
        
        let window_exec = extract_window_exec_from_sql(sql).await;
        let mut window_config = WindowOperatorConfig::new(window_exec);
        window_config.update_mode = UpdateMode::PerMessage;
        window_config.parallelize = true;
        
        let operator_config = OperatorConfig::WindowConfig(window_config);
        let runtime_context = create_test_runtime_context();
        
        let mut window_operator = WindowOperator::new(operator_config);
        window_operator.open(&runtime_context).await.expect("Should be able to open operator");
        
        // Create all test messages
        let batch1 = create_test_batch(vec![1000, 4000, 7000], vec![10.0, 40.0, 70.0], vec!["A", "A", "A"]);
        let message1 = create_keyed_message(batch1, "A");
        
        let batch2 = create_test_batch(vec![2000, 3000, 5000, 6000, 8000], vec![20.0, 30.0, 50.0, 60.0, 80.0], vec!["A", "A", "A", "A", "A"]);
        let message2 = create_keyed_message(batch2, "A");
        
        let batch3 = create_test_batch(vec![9000, 10000], vec![90.0, 100.0], vec!["A", "A"]);
        let message3 = create_keyed_message(batch3, "A");
        
        let input_stream = Box::pin(futures::stream::iter(vec![message1, message2, message3]));
        window_operator.set_input(Some(input_stream));
        
        // Step 1: Process initial ordered batch [1000, 4000, 7000]
        let result1 = window_operator.poll_next().await;
        let _message1_result = result1.get_result_message();
        
        // Step 2: Process batch with multiple late entries [2000, 3000, 5000, 6000] + one non-late entry [8000]
        let result2 = window_operator.poll_next().await;
        let message2_result = result2.get_result_message();
        let result_batch2 = message2_result.record_batch();
        assert_eq!(result_batch2.num_rows(), 5, "Should have 5 result rows (4 late entries + 1 non-late)");
        
        let sum_column2 = result_batch2.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // Verify late entries are processed in timestamp order:
        // t=2000: window=[1000,2000] -> SUM=30.0 (10.0+20.0)
        // t=3000: window=[1000,2000,3000] -> SUM=60.0 (10.0+20.0+30.0)
        // t=5000: window=[3000,4000,5000] -> SUM=120.0 (30.0+40.0+50.0)  
        // t=6000: window=[4000,5000,6000] -> SUM=150.0 (40.0+50.0+60.0)
        // t=8000: window=[6000,7000,8000] -> SUM=210.0 (60.0+70.0+80.0)
        assert_eq!(sum_column2.value(0), 30.0, "First late entry SUM should be 30.0");
        assert_eq!(sum_column2.value(1), 60.0, "Second late entry SUM should be 60.0");
        assert_eq!(sum_column2.value(2), 120.0, "Third late entry SUM should be 120.0");
        assert_eq!(sum_column2.value(3), 150.0, "Fourth late entry SUM should be 150.0");
        assert_eq!(sum_column2.value(4), 210.0, "Non-late entry SUM should be 210.0");
        
        // Step 3: Process more non-late entries to verify late entries have updated accumulators
        let result3 = window_operator.poll_next().await;
        let message3_result = result3.get_result_message();
        let result_batch3 = message3_result.record_batch();
        assert_eq!(result_batch3.num_rows(), 2, "Should have 2 result rows for final batch");
        
        let sum_column3 = result_batch3.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // Verify that late entries have been properly integrated into accumulator state:
        // t=9000: window=[7000,8000,9000] -> SUM=240.0 (70.0+80.0+90.0)
        // t=10000: window=[8000,9000,10000] -> SUM=270.0 (80.0+90.0+100.0)
        // These results should reflect that all previous events (including late ones) are part of the time index
        assert_eq!(sum_column3.value(0), 240.0, "t=9000 SUM should be 240.0 (includes late entry effects)");
        assert_eq!(sum_column3.value(1), 270.0, "t=10000 SUM should be 270.0 (includes late entry effects)");
        
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
        window_config.update_mode = UpdateMode::PerMessage;
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
        
        let input_stream = Box::pin(futures::stream::iter(vec![message1, message2, message3]));
        window_operator.set_input(Some(input_stream));

        // Test 1: All ordered events
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

        // Test 2: Mixed batch with late entries
        let result2 = window_operator.poll_next().await;
        let message2_result = result2.get_result_message();
        let result_batch2 = message2_result.record_batch();
        assert_eq!(result_batch2.num_rows(), 3, "Should have 3 result rows");
        
        let sum_small_col2 = result_batch2.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let avg_large_col2 = result_batch2.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // Verify late entries are processed correctly in timestamp order, late first, non-late after
        // Late entry t=1500: small window=[1000,1500] -> SUM=25.0, large window=[1000,1500] -> AVG=12.5
        // Late entry t=2500: small window=[1500,2000,2500] -> SUM=60.0, large window=[1000,1500,2000,2500] -> AVG=17.5
        // Non-late t=6000: small window=[5000,6000] -> SUM=110.0, large window=[3000,4000,5000,6000] -> AVG=45.0
        
        assert_eq!(sum_small_col2.value(0), 25.0, "Late t=1500 small window SUM should be 25.0");
        assert_eq!(sum_small_col2.value(1), 60.0, "Late t=2500 small window SUM should be 60.0");
        assert_eq!(sum_small_col2.value(2), 110.0, "t=6000 small window SUM should be 110.0");
        
        assert_eq!(avg_large_col2.value(0), 12.5, "Late t=1500 large window AVG should be 12.5");
        assert_eq!(avg_large_col2.value(1), 17.5, "Late t=2500 large window AVG should be 17.5");
        assert_eq!(avg_large_col2.value(2), 45.0, "t=6000 large window AVG should be 45.0");

        // Test 3: Verify accumulator integration with subsequent events
        let result3 = window_operator.poll_next().await;
        let message3_result = result3.get_result_message();
        let result_batch3 = message3_result.record_batch();
        assert_eq!(result_batch3.num_rows(), 2, "Should have 2 result rows");
        
        let sum_small_col3 = result_batch3.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let avg_large_col3 = result_batch3.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // Verify that late entries are reflected in subsequent calculations
        // t=7000: small window=[6000,7000] -> SUM=130.0, large window=[4000,5000,6000,7000] -> AVG=55.0 (220.0/4)
        // t=8000: small window=[7000,8000] -> SUM=150.0, large window=[5000,6000,7000,8000] -> AVG=65.0 (260.0/4)
        assert_eq!(sum_small_col3.value(0), 130.0, "t=7000 small window SUM should be 130.0");
        assert_eq!(sum_small_col3.value(1), 150.0, "t=8000 small window SUM should be 150.0");
        
        assert_eq!(avg_large_col3.value(0), 55.0, "t=7000 large window AVG should be 55.0 (includes late entry effects)");
        assert_eq!(avg_large_col3.value(1), 65.0, "t=8000 large window AVG should be 65.0 (includes late entry effects)");

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
        window_config.update_mode = UpdateMode::PerMessage;
        window_config.parallelize = true;
        
        // Set up tiling configs for all three aggregates
        use crate::runtime::operators::window::tiles::{TileConfig, TimeGranularity};
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
        
        let input_stream = Box::pin(futures::stream::iter(vec![message1, message2, message3, message4]));
        window_operator.set_input(Some(input_stream));

        // Step 1: Process initial ordered batch to establish baseline
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
        
        // Step 2: Process batch with late entries and out-of-order data
        let result2 = window_operator.poll_next().await;
        let message2_result = result2.get_result_message();
        let result_batch2 = message2_result.record_batch();
        assert_eq!(result_batch2.num_rows(), 4, "Should have 4 result rows (3 late + 1 new)");
        
        let min_column2 = result_batch2.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let max_column2 = result_batch2.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        let avg_column2 = result_batch2.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // Expected results for late entries (processed in timestamp order):
        // t=120000: window=[120000] -> MIN=20.0, MAX=20.0, AVG=20.0 (all others outside 5s window)
        // t=240000: window=[240000] -> MIN=40.0, MAX=40.0, AVG=40.0 (all others outside 5s window)
        // t=360000: window=[360000] -> MIN=60.0, MAX=60.0, AVG=60.0 (all others outside 5s window)
        // t=480000: window=[480000] -> MIN=80.0, MAX=80.0, AVG=80.0 (all others outside 5s window)

        assert_eq!(min_column2.value(0), 20.0, "Late t=120000 MIN should be 20.0");
        assert_eq!(max_column2.value(0), 20.0, "Late t=120000 MAX should be 20.0");
        assert_eq!(avg_column2.value(0), 20.0, "Late t=120000 AVG should be 20.0");
        
        assert_eq!(min_column2.value(1), 40.0, "Late t=240000 MIN should be 40.0");
        assert_eq!(max_column2.value(1), 40.0, "Late t=240000 MAX should be 40.0");
        assert_eq!(avg_column2.value(1), 40.0, "Late t=240000 AVG should be 40.0");
        
        assert_eq!(min_column2.value(2), 60.0, "Late t=360000 MIN should be 60.0");
        assert_eq!(max_column2.value(2), 60.0, "Late t=360000 MAX should be 60.0");
        assert_eq!(avg_column2.value(2), 60.0, "Late t=360000 AVG should be 60.0");
        
        assert_eq!(min_column2.value(3), 80.0, "t=480000 MIN should be 80.0");
        assert_eq!(max_column2.value(3), 80.0, "t=480000 MAX should be 80.0");
        assert_eq!(avg_column2.value(3), 80.0, "t=480000 AVG should be 80.0");
        
        // Step 3: Process more data to verify tiles are working correctly
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
        
        // Step 4: Test different partition to ensure tiles are partition-aware
        let result4 = window_operator.poll_next().await;
        let message4_result = result4.get_result_message();
        let result_batch4 = message4_result.record_batch();
        assert_eq!(result_batch4.num_rows(), 3, "Should have 3 result rows for partition B");
        
        let min_column4 = result_batch4.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let max_column4 = result_batch4.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        let avg_column4 = result_batch4.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // Partition B should have independent tiles and state
        // t=60000: window=[60000] -> MIN=5.0, MAX=5.0, AVG=5.0
        // t=120000: window=[120000] -> MIN=15.0, MAX=15.0, AVG=15.0 (60000 outside 5s window)
        // t=180000: window=[180000] -> MIN=25.0, MAX=25.0, AVG=25.0 (120000 outside 5s window)
        
        assert_eq!(min_column4.value(0), 5.0, "Partition B t=60000 MIN should be 5.0");
        assert_eq!(max_column4.value(0), 5.0, "Partition B t=60000 MAX should be 5.0");
        assert_eq!(avg_column4.value(0), 5.0, "Partition B t=60000 AVG should be 5.0");
        
        assert_eq!(min_column4.value(1), 15.0, "Partition B t=120000 MIN should be 15.0");
        assert_eq!(max_column4.value(1), 15.0, "Partition B t=120000 MAX should be 15.0");
        assert_eq!(avg_column4.value(1), 15.0, "Partition B t=120000 AVG should be 15.0");
        
        assert_eq!(min_column4.value(2), 25.0, "Partition B t=180000 MIN should be 25.0");
        assert_eq!(max_column4.value(2), 25.0, "Partition B t=180000 MAX should be 25.0");
        assert_eq!(avg_column4.value(2), 25.0, "Partition B t=180000 AVG should be 25.0");
        
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
        window_config.update_mode = UpdateMode::PerMessage;
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
            vec![16000, 18000, 25000], // 16s (too late, dropped), 18s (late, but processed), 25s (on time)
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
        
        let input_stream = Box::pin(futures::stream::iter(vec![message1, message2, message3]));
        window_operator.set_input(Some(input_stream));

        // Step 1: Process initial batch
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

        // Step 2: Test late event filtering and partial pruning
        let result2 = window_operator.poll_next().await;
        let message2_result = result2.get_result_message();
        let result_batch2 = message2_result.record_batch();
        // Should process 2 events: 18000, 25000 (16000 dropped as too late)
        assert_eq!(result_batch2.num_rows(), 2, "Should have 2 result rows (1 event dropped)");
        
        // Verify the window calculations for the last event (25000ms)
        let sum_column = result_batch2.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let avg_column = result_batch2.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        let min_column = result_batch2.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        let count_column = result_batch2.column(6).as_any().downcast_ref::<Int64Array>().unwrap();
        let max_column = result_batch2.column(7).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // For event at 25000ms:
        // w1 (2s range): includes 25000ms only -> sum = 250.0
        // w2 (5s range): includes 20000ms, 25000ms -> avg = (200.0 + 250.0) / 2 = 225.0, min = 200.0
        // w3 (3 rows): includes last 3 events: 18000ms, 20000ms, 25000ms -> count = 3, max = 250.0
        assert_eq!(sum_column.value(1), 250.0, "2s window should include only current event");
        assert_eq!(avg_column.value(1), 225.0, "5s window should include 20000ms and 25000ms");
        assert_eq!(min_column.value(1), 200.0, "5s window min should be 200.0");
        assert_eq!(count_column.value(1), 3, "3-row window should include last 3 events");
        assert_eq!(max_column.value(1), 250.0, "3-row window max should be 250.0");
        
        // Verify state after step 2 - additional pruning should occur
        // After step 2, we have entries: [10000, 15000, 18000, 20000, 25000] (16000 was dropped as late)
        // w1 (2s range): cutoff = 25000 - (3000 + 2000) = 20000ms
        // w2 (5s range): cutoff = 25000 - (3000 + 5000) = 17000ms
        // w3 (3 rows): Search timestamp = 25000 - 3000 = 22000ms
        //              Last entry <= 22000ms is 20000ms
        //              For 3-row window ending at 20000ms, window start is 15000ms (last 3: [15000, 18000, 20000])
        //              So w3 cutoff = 15000ms  
        // Minimal cutoff = min(20000, 17000, 15000) = 15000ms
        window_operator.get_state().verify_pruning_for_testing(&partition_key, 15000).await;

        // Step 3: Add many events close together to make range window the limiting factor
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
        // After step 3, we have entries: [15000, 18000, 20000, 25000, 26000, 27000, 28000, 29000, 30000]
        // w1 (2s range): cutoff = 30000 - (3000 + 2000) = 25000ms
        // w2 (5s range): cutoff = 30000 - (3000 + 5000) = 22000ms
        // w3 (3 rows): Search timestamp = 30000 - 3000 = 27000ms
        //              Last entry <= 27000ms is 27000ms
        //              For 3-row window ending at 27000ms, window start is 25000ms (last 3: [25000, 26000, 27000])
        //              So w3 cutoff = 25000ms
        // Minimal cutoff = min(25000, 22000, 25000) = 22000ms
        window_operator.get_state().verify_pruning_for_testing(&partition_key, 22000).await;
        
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
        window_config.update_mode = UpdateMode::PerMessage;
        window_config.parallelize = true;
        
        // Set up tiling configs for MIN and MAX (plain aggregates)
        use crate::runtime::operators::window::tiles::{TileConfig, TimeGranularity};
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
        
        let input_stream = Box::pin(futures::stream::iter(vec![message1]));
        window_operator.set_input(Some(input_stream));
        
        let result1 = window_operator.poll_next().await;
        assert!(matches!(result1, OperatorPollResult::Continue), "Request mode should produce no output");
    
        // Verify state was updated
        let state1_guard = window_operator.state.get_windows_state(&partition_key).await
            .expect("State should exist");
        let state1 = state1_guard.value();
        assert_eq!(state1.time_entries.entries.len(), 3, "Should have 3 time entries");
        
        // Verify accumulator states were updated for retractable aggregates
        let window_state_w1 = state1.window_states.get(&0).expect("Window state should exist");
        assert!(window_state_w1.accumulator_state.is_some(), "Accumulator state should be updated");
        let window_state_w2 = state1.window_states.get(&1).expect("Window state should exist");
        assert!(window_state_w2.accumulator_state.is_some(), "Accumulator state should be updated");
        let window_state_w3 = state1.window_states.get(&2).expect("Window state should exist");
        assert!(window_state_w3.accumulator_state.is_some(), "Accumulator state should be updated");
        
        // Drop guard before proceeding to next step
        drop(state1_guard);
        
        // Step 2: Process late entry [1500] - should update state
        let batch2 = create_test_batch(vec![1500], vec![15.0], vec!["A"]);
        let message2 = create_keyed_message(batch2, "A");
        
        let input_stream2 = Box::pin(futures::stream::iter(vec![message2]));
        window_operator.set_input(Some(input_stream2));
        
        let result2 = window_operator.poll_next().await;
        assert!(matches!(result2, OperatorPollResult::Continue), "Request mode should produce no output");
        
        // Verify state was updated with late entry
        let state2_guard = window_operator.state.get_windows_state(&partition_key).await
            .expect("State should exist");
        let state2 = state2_guard.value();
        assert_eq!(state2.time_entries.entries.len(), 4, "Should have 4 time entries after late entry");
        
        // Verify accumulator state was updated (late entries update retractable accumulators)
        let window_state2 = state2.window_states.get(&0).expect("Window state should exist");
        assert!(window_state2.accumulator_state.is_some(), "Accumulator state should be updated after late entry");
        
        // Drop guard before proceeding to next step
        drop(state2_guard);
        
        // Step 3: Process more entries [4000, 5000]
        let batch3 = create_test_batch(vec![4000, 5000], vec![40.0, 50.0], vec!["A", "A"]);
        let message3 = create_keyed_message(batch3, "A");
        
        let input_stream3 = Box::pin(futures::stream::iter(vec![message3]));
        window_operator.set_input(Some(input_stream3));
        
        let result3 = window_operator.poll_next().await;
        assert!(matches!(result3, OperatorPollResult::Continue), "Request mode should produce no output");
        
        // Verify state was updated
        let state3_guard = window_operator.state.get_windows_state(&partition_key).await
            .expect("State should exist");
        let state3 = state3_guard.value();
        assert_eq!(state3.time_entries.entries.len(), 6, "Should have 6 time entries");
        
        // Verify window positions advanced
        let window_state3_w3 = state3.window_states.get(&3).expect("Window state should exist");
        assert_eq!(window_state3_w3.end_idx.timestamp, 5000, "Window end should advance to latest entry");
        let window_state3_w4 = state3.window_states.get(&4).expect("Window state should exist");
        
        // Verify tiles were updated (for MIN and MAX)
        assert!(window_state3_w3.tiles.is_some(), "Tiles should exist for MIN aggregates");
        assert!(window_state3_w4.tiles.is_some(), "Tiles should exist for MAX aggregates");
        
        window_operator.close().await.expect("Should be able to close operator");
    }

}