use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Schema, SchemaBuilder, SchemaRef};
use async_trait::async_trait;
use crossbeam_skiplist::SkipSet;
use futures::future;
use tokio_rayon::rayon::{ThreadPool, ThreadPoolBuilder};

use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use crate::common::message::Message;
use crate::common::Key;
use crate::runtime::operators::operator::{OperatorBase, OperatorConfig, OperatorTrait, OperatorType};
use crate::runtime::operators::window::aggregates::{get_aggregate_type, produce_aggregates, run_retractable_accumulator, run_retractable_accumulator_parallel};
use crate::runtime::operators::window::state::{State, WindowId, WindowsState};
use crate::runtime::operators::window::time_index::{get_batches_for_entries, slide_window_position, TimeIdx, TimeIndex};
use crate::runtime::operators::window::{AggregatorType, TileConfig};
use crate::runtime::runtime_context::RuntimeContext;
use crate::storage::storage::{Storage};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionMode {
    EventBased, // all events are processed immediately, late events are handled with best effort
    WatermarkBased, // events are buffered until watermark is reached, late events are dropped
}

#[derive(Debug, Clone)]
pub struct WindowConfig {
    pub window_id: WindowId,
    pub window_expr: Arc<dyn WindowExpr>,
    pub tiling: Option<TileConfig>,
    pub aggregator_type: AggregatorType,
}

#[derive(Debug, Clone)]
pub struct WindowOperatorConfig {
    pub window_exec: Arc<BoundedWindowAggExec>,
    pub execution_mode: ExecutionMode,
    pub parallelize: bool,
    pub tiling_configs: Vec<Option<TileConfig>>,
}

impl WindowOperatorConfig {
    // TODO pass all the fileds from upstream/parse window_exprs
    pub fn new(window_exec: Arc<BoundedWindowAggExec>) -> Self {    
        Self {
            window_exec,
            execution_mode: ExecutionMode::WatermarkBased,
            parallelize: false,
            tiling_configs: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct WindowOperator {
    base: OperatorBase,
    windows: BTreeMap<WindowId, WindowConfig>,
    state: State,
    time_index: TimeIndex,
    ts_column_index: usize,
    buffered_keys: HashSet<Key>,
    execution_mode: ExecutionMode,
    parallelize: bool,
    thread_pool: Option<ThreadPool>,
    output_schema: SchemaRef,
    input_schema: SchemaRef,
    tiling_configs: Vec<Option<TileConfig>>,
}

impl WindowOperator {
    pub fn new(config: OperatorConfig, storage: Arc<Storage>) -> Self {
        let window_operator_config = match config.clone() {
            OperatorConfig::WindowConfig(window_config) => window_config,
            _ => panic!("Expected WindowConfig, got {:?}", config),
        };

        let ts_column_index = window_operator_config.window_exec.window_expr()[0].order_by()[0].expr.as_any().downcast_ref::<Column>().expect("Expected Column expression in ORDER BY").index();

        let mut windows = BTreeMap::new();
        for (window_id, window_expr) in window_operator_config.window_exec.window_expr().iter().enumerate() {
            windows.insert(window_id, WindowConfig {
                window_id,
                window_expr: window_expr.clone(),
                tiling: window_operator_config.tiling_configs.get(window_id).and_then(|config| config.clone()),
                aggregator_type: get_aggregate_type(window_expr),
            });
        }

        let input_schema = window_operator_config.window_exec.input().schema();
        let output_schema = create_output_schema(&input_schema, &window_operator_config.window_exec.window_expr());

        let thread_pool = if window_operator_config.parallelize {
            Some(ThreadPoolBuilder::new()
                .num_threads(std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
                .build()
                .expect("Failed to create thread pool"))
        } else {
            None
        };

        Self {
            base: OperatorBase::new(config, storage),
            windows,
            state: State::new(),
            time_index: TimeIndex::new(),
            ts_column_index,
            buffered_keys: HashSet::new(),
            execution_mode: window_operator_config.execution_mode,
            parallelize: window_operator_config.parallelize,
            thread_pool,
            output_schema,
            input_schema,
            tiling_configs: window_operator_config.tiling_configs,
        }
    }

    async fn process_key(&self, key: &Key, late_entries: Option<Vec<TimeIdx>>) -> RecordBatch {
        let window_ids: Vec<_> = self.windows.keys().cloned().collect();
        let window_exprs: Vec<_> = self.windows.values().map(|window| window.window_expr.clone()).collect();
        let windows_state = self.state.get_or_create_windows_state(key, &window_ids, &self.tiling_configs, &window_exprs).await;
        let time_entries = self.time_index.get_or_create_time_index(key).await;
        let (result, updated_windows_state) = self.advance_windows(key, &windows_state, &time_entries, late_entries).await;
        self.state.insert_windows_state(key, updated_windows_state).await;
        result
    }

    async fn process_buffered(&self) -> RecordBatch {
        let futures: Vec<_> = self.buffered_keys.iter()
            .map(|key| async move {
                self.process_key(key, None).await
            })
            .collect();
        
        let results = future::join_all(futures).await;

        if results.is_empty() {
            return RecordBatch::new_empty(self.output_schema.clone());
        }
        
        arrow::compute::concat_batches(&self.output_schema, &results)
            .expect("Should be able to concat result batches")
    }

    async fn update_retractable_windows_state_with_late_entries(
        &self, 
        key: &Key,
        windows_state: &WindowsState,
        late_entries: &Vec<TimeIdx>
    ) -> WindowsState {
        let mut updated_windows_state = windows_state.clone();
        // Create late_entries_per_window map for reuse
        let late_entries_per_window: std::collections::BTreeMap<WindowId, Vec<TimeIdx>> = self.windows.iter()
            .map(|(window_id, _)| {
                // Only process late entries for retractable windows
                let aggregator_type = self.windows.get(window_id).expect("Window config should exist").aggregator_type;
                if aggregator_type == AggregatorType::RetractableAccumulator {
                    let window_state = updated_windows_state.get(window_id).expect("Window state should exist");
                    let window_start = window_state.start_idx;
                    let window_end = window_state.end_idx;
                    
                    let late_entries_in_window: Vec<_> = late_entries.iter()
                        .filter(|entry| **entry >= window_start && **entry < window_end)
                        .cloned()
                        .collect();
                    
                    (*window_id, late_entries_in_window)
                } else {
                    (*window_id, Vec::new())
                }
            })
            .collect();
        
        // Compose batches_to_load from late entries
        let mut batches_to_load = std::collections::BTreeSet::new();
        for late_entries_in_window in late_entries_per_window.values() {
            for late_entry in late_entries_in_window {
                batches_to_load.insert(late_entry.batch_id);
            }
        }
        
        // Get relevant data from storage
        let batches = self.base.storage.load_batches(batches_to_load.into_iter().collect(), key).await;
        
        let updated_accumulator_states_futs: Vec<_> = self.windows.iter()
            .map(|(window_id, window_config)| {
                let window_expr_clone = window_config.window_expr.clone();
                let batches_clone = batches.clone();
                let thread_pool = self.thread_pool.as_ref().clone();
                let parallelize = self.parallelize;
                let window_id_copy = *window_id;
                let window_state = updated_windows_state.get(window_id).expect("Window state should exist").clone();
                let late_entries_in_window = late_entries_per_window.get(window_id).cloned().unwrap_or_default();
                
                async move {
                    // late_entries_in_window is already filtered by aggregator_type == RetractableAccumulator
                    if !late_entries_in_window.is_empty() {
                        let (_, updated_accumulator_state) = if parallelize {
                            run_retractable_accumulator_parallel(thread_pool.expect("ThreadPool should exist"), window_expr_clone, late_entries_in_window, None, batches_clone, Some(window_state.accumulator_state.expect("Accumulator state should exist"))).await
                        } else {
                            run_retractable_accumulator(&window_expr_clone, late_entries_in_window, None, &batches_clone, Some(window_state.accumulator_state.expect("Accumulator state should exist")))
                        };
                        
                        Some((window_id_copy, updated_accumulator_state))
                    } else {
                        None
                    }
                }
            })
            .collect();
        
        let updated_accumulator_states = future::join_all(updated_accumulator_states_futs).await;
        
        // Update windows_state with late entry results
        for result in updated_accumulator_states {
            if let Some((window_id, updated_accumulator_state)) = result {
                if let Some(window_state) = updated_windows_state.get_mut(&window_id) {
                    window_state.accumulator_state = Some(updated_accumulator_state);
                }
            }
        }

        updated_windows_state
    }

    async fn advance_windows(
        &self, 
        key: &Key, 
        windows_state: &WindowsState,
        time_entries: &Arc<SkipSet<TimeIdx>>,
        late_entries: Option<Vec<TimeIdx>>
    ) -> (RecordBatch, WindowsState) {
        // Step 0: Update accumulator states for all retractable windows with late entries if needed
        let windows_state = if let Some(ref late_entries_ref) = late_entries {
            self.update_retractable_windows_state_with_late_entries(key, windows_state, late_entries_ref).await
        } else {
            windows_state.clone()
        };

        // Step 1: Advance window positions to latest timestamp
        let mut window_data = Vec::new();
        
        for (window_id, window_config) in &self.windows {
            let window_frame = window_config.window_expr.get_window_frame();
            let window_state = windows_state.get(window_id).expect("Window state should exist");
            let mut window_state_copy = window_state.clone();
            let retractable = self.windows[window_id].aggregator_type == AggregatorType::RetractableAccumulator;
            
            let (updates, retracts) = slide_window_position(window_frame, &mut window_state_copy, &time_entries, retractable);
            
            window_data.push((*window_id, window_state_copy, updates, retracts));
        }
        
        // Step 2: Compose batches_to_load from all updates, retracts and late entries
        let mut batches_to_load = std::collections::BTreeSet::new();
        for (window_id, _, updates, retracts) in window_data.iter() {
            let retractable = self.windows[window_id].aggregator_type == AggregatorType::RetractableAccumulator;
            let window_frame = self.windows[window_id].window_expr.get_window_frame();
            let window_state = windows_state.get(window_id).expect("Window state should exist");
                
            if retractable {
                // for retarctables, we do not need to scan whole window on each update
                for i in 0..updates.len() {
                    let update_idx = &updates[i];
                    batches_to_load.insert(update_idx.batch_id);
                    if retractable {
                        let retract_idxs = &retracts[i];
                        for retract_idx in retract_idxs {
                            batches_to_load.insert(retract_idx.batch_id);
                        }
                    }
                }
            } else {
                // non-rets (plain and evaluators) need whole window data (excluding tiles) for each update/entry
                let batch_ids = get_batches_for_entries(updates, time_entries, window_frame, Some(window_state));
                for batch_id in batch_ids {
                    batches_to_load.insert(batch_id);
                }
            }

            // for late entries we also need to include all events (excluding tiled ranges) 
            // falling into the window since we do full rebuild
            if let Some(ref late_entries_ref) = late_entries {
                let late_entries_batches = get_batches_for_entries(late_entries_ref, time_entries, window_frame, Some(window_state));
                for batch_id in late_entries_batches {
                    batches_to_load.insert(batch_id);
                }
            }
        }
        
        // Step 3: Get relevant data from storage
        let batches = self.base.storage.load_batches(batches_to_load.into_iter().collect(), key).await;

        // Step 4: Calculate late results if needed (parallel per window)
        let late_results = if let Some(ref late_entries_ref) = late_entries {
            let late_futures: Vec<_> = self.windows.iter()
                .map(|(_, window_config)| {
                    let window_expr_clone = window_config.window_expr.clone();
                    let late_entries_clone = late_entries_ref.clone();
                    let time_entries_clone = time_entries.clone();
                    let batches_clone = batches.clone();
                    let thread_pool = self.thread_pool.as_ref().clone();
                    let parallelize = self.parallelize;
                    
                    async move {
                        let late_results_for_window = produce_aggregates(
                            &window_expr_clone, 
                            None,
                            &late_entries_clone,
                            time_entries_clone, 
                            &batches_clone, 
                            thread_pool, 
                            parallelize
                        ).await;
                        assert_eq!(late_results_for_window.len(), late_entries_clone.len());
                        late_results_for_window
                    }
                })
                .collect();
            
            future::join_all(late_futures).await
        } else {
            Vec::new()
        };

        // Step 5: Concurrently produce aggregates for all windows
        let batches = Arc::new(batches);
        let accumulator_futures: Vec<_> = window_data.iter()
            .map(|(window_id, window_state, updates, retracts)| {
                let window_expr = self.windows[window_id].window_expr.clone();
                let aggregator_type = self.windows[window_id].aggregator_type.clone();
                let batches_clone = batches.clone();
                let accumulator_state_clone = window_state.accumulator_state.clone();
                let window_id_copy = *window_id;
                let window_state_clone = window_state.clone();
                let time_entries_clone = time_entries.clone();
                async move {
                    // Accumulator state is already updated in Step 0 if there were late entries
                    let acummulator_state = accumulator_state_clone;

                    let (aggregates, accumulator_state) = 
                        match aggregator_type {
                            AggregatorType::RetractableAccumulator => {
                                if self.parallelize {
                                    let (aggs, acc_state) = run_retractable_accumulator_parallel(
                                        self.thread_pool.as_ref().expect("ThreadPool should exist"), 
                                        window_expr, 
                                        updates.clone(), 
                                        Some(retracts.clone()), 
                                        (*batches_clone).clone(), 
                                        acummulator_state
                                    ).await;
                                    (aggs, Some(acc_state))
                                } else {
                                    let (aggs, acc_state) = run_retractable_accumulator(
                                        &window_expr, 
                                        updates.clone(), 
                                        Some(retracts.clone()), 
                                        &batches_clone, 
                                        acummulator_state
                                    );
                                    (aggs, Some(acc_state))
                                }
                            },
                            AggregatorType::PlainAccumulator => {
                                (produce_aggregates(
                                    &window_expr, 
                                    None,
                                    &updates, 
                                    time_entries_clone, 
                                    &batches_clone, 
                                    self.thread_pool.as_ref(), 
                                    self.parallelize
                                ).await, None)
                            },
                            AggregatorType::Evaluator => {
                                panic!("Evaluator aggregator is not supported yet");
                            }
                        };
                    
                    (window_id_copy, aggregates, accumulator_state, window_state_clone)
                }
            })
            .collect();
        
        let accumulator_results = future::join_all(accumulator_futures).await;
        
        // Step 6: Update window states
        let mut updated_windows_state = HashMap::new();
        for result in &accumulator_results {
            let (window_id, _, accumulator_state, window_state) = result;
            let mut updated_window_state = window_state.clone();
            updated_window_state.accumulator_state = accumulator_state.clone();
            updated_windows_state.insert(*window_id, updated_window_state);
        }
        
        // Step 7: Extract input column values from all update rows
        let mut update_idxs = if let Some(ref late_entries) = late_entries {
            // prepend late entries
            late_entries.iter().cloned().collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        // use first update idxs (they should be all same for all windows)
        update_idxs.extend(window_data.first().expect("Window data should exist").2.clone());
        
        let mut input_values = Vec::new();
        
        // Extract input values for each update row
        // Window operator schema is fixed: input columns first, then window columns
        let input_column_count = self.input_schema.fields().len();
        
        for update_idx in update_idxs {
            let batch: &RecordBatch = batches.get(&update_idx.batch_id).expect("Batch should exist");
            let row_idx = update_idx.row_idx;
            
            let mut row_input_values = Vec::new();
            for col_idx in 0..input_column_count {
                let array = batch.column(col_idx);
                let scalar_value = datafusion::common::ScalarValue::try_from_array(array, row_idx)
                    .expect("Should be able to extract scalar value");
                row_input_values.push(scalar_value);
            }
            input_values.push(row_input_values);
        }
        
        let mut results = Vec::new();
        for i in 0..accumulator_results.len() {
            // prepend late results if needed
            let mut results_for_window = if late_results.len() > 0 {
                late_results[i].clone()
            } else {
                Vec::new()
            };
            results_for_window.extend(accumulator_results[i].1.clone());
            results.push(results_for_window);
        }

        // Step 8: Concat results and create a batch
        (concat_results(results, input_values, &self.output_schema, &self.input_schema), updated_windows_state)
    }
}

#[async_trait]
impl OperatorTrait for WindowOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    async fn process_message(&mut self, message: Message) -> Option<Vec<Message>> {
        let storage = self.base.storage.clone();
        let partition_key = message.key().expect("Window Operator expects KeyedMessage");
        let time_entries = self.time_index.get_or_create_time_index(partition_key).await;
        let last_entry_before_update = time_entries.back();

        // TODO based one execution mode (event vs watermark based) we may need to drop late events

        // calculate pre-aggregated tiles if needed
        let has_tiled_windows = self.tiling_configs.iter().any(|config| config.is_some());
        if has_tiled_windows {
            let window_ids: Vec<_> = self.windows.keys().cloned().collect();
            let window_exprs: Vec<_> = self.windows.values().map(|window| window.window_expr.clone()).collect();
            let mut windows_state = self.state.get_or_create_windows_state(
                partition_key, 
                &window_ids, 
                &self.tiling_configs, 
                &window_exprs
            ).await;
            for (_, window_state) in windows_state.iter_mut() {
                if let Some(ref mut tiles) = window_state.tiles {
                    tiles.add_batch(message.record_batch(), self.ts_column_index);
                }
            }
        }

        // TODO pruning
        
        let batches = storage.append_records(message.record_batch().clone(), partition_key, self.ts_column_index).await;
        let mut inserted_idxs = Vec::new();
        for (batch_id, batch) in batches {
            inserted_idxs.extend(self.time_index.update_time_index(partition_key, batch_id, &batch, self.ts_column_index).await);
        }
        
        if self.execution_mode == ExecutionMode::WatermarkBased {
            // buffer for processing on watermark
            self.buffered_keys.insert(partition_key.clone());
            None
        } else {
            let late_entries = if let Some(last_entry) = last_entry_before_update {
                Some(inserted_idxs.into_iter().filter(|idx| idx < last_entry.value()).collect::<Vec<_>>())
            } else {
                None
            };
            let result = self.process_key(&partition_key, late_entries).await;
            // vertex_id will be set by stream task
            // TODO ingest timestamp?
            Some(vec![Message::new(None, result, None)])
        }
    }

    async fn process_watermark(&mut self, _watermark: u64) -> Option<Vec<Message>> {
        if self.execution_mode == ExecutionMode::EventBased {
            panic!("EventBased execution mode does not support watermark processing");
        }
        let result = self.process_buffered().await;
        self.buffered_keys.clear();
        // vertex_id will be set by stream task
        // TODO ingest timestamp?
        Some(vec![Message::new(None, result, None)])
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }
}

// copied from private DataFusion function
fn create_output_schema(
    input_schema: &Schema,
    window_expr: &[Arc<dyn WindowExpr>],
) -> Arc<Schema> {
    let capacity = input_schema.fields().len() + window_expr.len();
    let mut builder = SchemaBuilder::with_capacity(capacity);
    builder.extend(input_schema.fields().iter().cloned());
    // append results to the schema
    for expr in window_expr {
        builder.push(expr.field().expect("Should be able to get field"));
    }
    Arc::new(builder
        .finish()
        .with_metadata(input_schema.metadata().clone())
    )
}

fn concat_results(
    results: Vec<Vec<ScalarValue>>, 
    input_values: Vec<Vec<ScalarValue>>, 
    output_schema: &SchemaRef,
    input_schema: &SchemaRef
) -> RecordBatch {
    let mut columns: Vec<ArrayRef> = Vec::new();
    
    // Create input columns (first N columns in output schema)
    for col_idx in 0..input_schema.fields().len() {
        
        // Extract values for this column from all rows
        let column_values: Vec<ScalarValue> = input_values.iter()
            .map(|row| {
                row[col_idx].clone()
            })
            .collect();
        
        let array = ScalarValue::iter_to_array(column_values.into_iter())
            .expect("Should be able to convert input values to array");
        columns.push(array);
    }
    
    // Add window result columns (remaining columns in output schema)
    for window_results in results.iter() {
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
    use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
    use crate::runtime::operators::operator::OperatorConfig;
    use crate::common::message::Message;
    use crate::runtime::runtime_context::RuntimeContext;
    use crate::storage::storage::Storage;
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
        
        let logical_graph = planner.sql_to_graph(sql).await.unwrap();
        let nodes: Vec<_> = logical_graph.get_nodes().collect();
        
        for node in &nodes {
            if let OperatorConfig::WindowConfig(config) = &node.operator_config {
                return config.window_exec.clone();
            }
        }
        
        panic!("No window operator found in SQL: {}", sql);
    }

    fn create_keyed_message(batch: RecordBatch, partition_key: &str) -> Message {
        let key = create_test_key(partition_key);
        Message::new_keyed(None, batch, key, None)
    }

    fn create_test_runtime_context() -> RuntimeContext {
        RuntimeContext::new(
            "test_vertex".to_string(),
            0,
            1,
            None
        )
    }

    #[tokio::test]
    async fn test_range_window_event_based_exec() {
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
        window_config.execution_mode = ExecutionMode::EventBased;
        window_config.parallelize = true;

        let storage = Arc::new(Storage::default());
        let operator_config = OperatorConfig::WindowConfig(window_config);
        
        let mut window_operator = WindowOperator::new(operator_config, storage);
        let runtime_context = create_test_runtime_context();
        window_operator.open(&runtime_context).await.expect("Should be able to open operator");
        
        // Batch 1: Single row for partition "A"
        let batch1 = create_test_batch(vec![1000], vec![10.0], vec!["A"]);
        let message1 = create_keyed_message(batch1, "A");
        
        let results1 = window_operator.process_message(message1).await;
        assert!(results1.is_some(), "Should have results for first batch");
        
        let result_messages1 = results1.unwrap();
        assert_eq!(result_messages1.len(), 1, "Should have 1 result message");
        
        let result_batch1 = result_messages1[0].record_batch();
        assert_eq!(result_batch1.num_rows(), 1, "Should have 1 result row");
        assert_eq!(result_batch1.num_columns(), 8, "Should have 8 columns (timestamp, value, partition_key, + 5 aggregates)");
        
        // Verify first row results: SUM=10.0, COUNT=1, AVG=10.0, MIN=10.0, MAX=10.0
        // Columns: [0=timestamp, 1=value, 2=partition_key, 3=sum_val, 4=count_val, 5=avg_val, 6=min_val, 7=max_val]
        let sum_column = result_batch1.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let count_column = result_batch1.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        
        assert_eq!(sum_column.value(0), 10.0, "SUM should be 10.0");
        assert_eq!(count_column.value(0), 1, "COUNT should be 1");
        
        // Batch 2: Multi-row batch within window
        let batch2 = create_test_batch(vec![1500, 2000], vec![30.0, 20.0], vec!["A", "A"]);
        let message2 = create_keyed_message(batch2, "A");
        
        let results2 = window_operator.process_message(message2).await;
        assert!(results2.is_some(), "Should have results for second batch");
        
        let result_messages2 = results2.unwrap();
        let result_batch2 = result_messages2[0].record_batch();
        assert_eq!(result_batch2.num_rows(), 2, "Should have 2 result rows");
        
        let sum_column2 = result_batch2.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let count_column2 = result_batch2.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        
        // Row 1 (t=1500): includes t=1000,1500 -> SUM=40.0, COUNT=2
        assert_eq!(sum_column2.value(0), 40.0, "SUM at t=1500 should be 40.0 (10.0+30.0)");
        assert_eq!(count_column2.value(0), 2, "COUNT at t=1500 should be 2");
        
        // Row 2 (t=2000): includes t=1000,1500,2000 -> SUM=60.0, COUNT=3  
        assert_eq!(sum_column2.value(1), 60.0, "SUM at t=2000 should be 60.0 (10.0+30.0+20.0)");
        assert_eq!(count_column2.value(1), 3, "COUNT at t=2000 should be 3");
        
        // Batch 3: Partial retraction (t=3200 causes t=1000 to be excluded)
        let batch3 = create_test_batch(vec![3200], vec![5.0], vec!["A"]);
        let message3 = create_keyed_message(batch3, "A");
        
        let results3 = window_operator.process_message(message3).await;
        assert!(results3.is_some(), "Should have results for third batch");
        
        let result_messages3 = results3.unwrap();
        let result_batch3 = result_messages3[0].record_batch();
        assert_eq!(result_batch3.num_rows(), 1, "Should have 1 result row");
        
        let sum_column3 = result_batch3.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let count_column3 = result_batch3.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        
        // Window now includes t=1500,2000,3200 (t=1000 excluded) -> SUM=55.0, COUNT=3
        assert_eq!(sum_column3.value(0), 55.0, "SUM at t=3200 should be 55.0 (30.0+20.0+5.0)");
        assert_eq!(count_column3.value(0), 3, "COUNT at t=3200 should be 3");
        
        // Test different partition
        let batch4 = create_test_batch(vec![1000, 2000], vec![100.0, 200.0], vec!["B", "B"]);
        let message4 = create_keyed_message(batch4, "B");
        
        let results4 = window_operator.process_message(message4).await;
        assert!(results4.is_some(), "Should have results for partition B");
        
        let result_messages4 = results4.unwrap();
        let result_batch4 = result_messages4[0].record_batch();
        assert_eq!(result_batch4.num_rows(), 2, "Should have 2 result rows for partition B");
        
        let sum_column4 = result_batch4.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        
        // Partition B should have independent window state
        assert_eq!(sum_column4.value(0), 100.0, "SUM for partition B at t=1000 should be 100.0");
        assert_eq!(sum_column4.value(1), 300.0, "SUM for partition B at t=2000 should be 300.0 (100.0+200.0)");
        
        window_operator.close().await.expect("Should be able to close operator");
    }

    #[tokio::test]
    async fn test_rows_window_event_based_exec() {
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
        window_config.execution_mode = ExecutionMode::EventBased;
        window_config.parallelize = true;

        let storage = Arc::new(Storage::default());
        let operator_config = OperatorConfig::WindowConfig(window_config);
        
        let mut window_operator = WindowOperator::new(operator_config, storage);
        let runtime_context = create_test_runtime_context();
        window_operator.open(&runtime_context).await.expect("Should be able to open operator");
        
        // Test data: 5 rows with increasing values
        let batch = create_test_batch(
            vec![1000, 2000, 3000, 4000, 5000], 
            vec![10.0, 20.0, 30.0, 40.0, 50.0], 
            vec!["test", "test", "test", "test", "test"]
        );
        let message = create_keyed_message(batch, "test");
        
        let results = window_operator.process_message(message).await;
        assert!(results.is_some(), "Should have results");
        
        let result_messages = results.unwrap();
        let result_batch = result_messages[0].record_batch();
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
    async fn test_range_window_watermark_exec() {
        // Test watermark-based execution mode with alias
        let sql = "SELECT 
            timestamp,
            value,
            partition_key,
            SUM(value) OVER w as sum_val
        FROM test_table
        WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW)";
        
        let window_exec = extract_window_exec_from_sql(sql).await;
        let mut window_config = WindowOperatorConfig::new(window_exec);
        window_config.execution_mode = ExecutionMode::WatermarkBased;
        window_config.parallelize = true;
        
        let storage = Arc::new(Storage::default());
        let operator_config = OperatorConfig::WindowConfig(window_config);
        
        let mut window_operator = WindowOperator::new(operator_config, storage);
        let runtime_context = create_test_runtime_context();
        window_operator.open(&runtime_context).await.expect("Should be able to open operator");
        
        // Send messages - should be buffered until watermark
        let batch1 = create_test_batch(vec![1000], vec![10.0], vec!["test"]);
        let message1 = create_keyed_message(batch1, "test");
        
        let results1 = window_operator.process_message(message1).await;
        assert!(results1.is_none(), "Should buffer message until watermark");
        
        let batch2 = create_test_batch(vec![2000], vec![20.0], vec!["test"]);
        let message2 = create_keyed_message(batch2, "test");
        
        let results2 = window_operator.process_message(message2).await;
        assert!(results2.is_none(), "Should buffer message until watermark");
        
        // Send watermark - should trigger processing
        let watermark_results = window_operator.process_watermark(3000).await;
        assert!(watermark_results.is_some(), "Should have results after watermark");
        
        let watermark_messages = watermark_results.unwrap();
        assert_eq!(watermark_messages.len(), 1, "Should have 1 result message");
        
        let result_batch = watermark_messages[0].record_batch();
        assert_eq!(result_batch.num_rows(), 2, "Should have 2 result rows");
        
        let sum_column = result_batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(sum_column.value(0), 10.0, "First SUM should be 10.0");
        assert_eq!(sum_column.value(1), 30.0, "Second SUM should be 30.0 (10.0+20.0)");
        
        window_operator.close().await.expect("Should be able to close operator");
    }

    #[tokio::test]
    async fn test_late_entries_handling() {
        // Test late entries handling with RANGE window
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
        window_config.execution_mode = ExecutionMode::EventBased;
        window_config.parallelize = true;
        
        let storage = Arc::new(Storage::default());
        let operator_config = OperatorConfig::WindowConfig(window_config);
        let runtime_context = create_test_runtime_context();
        
        let mut window_operator = WindowOperator::new(operator_config, storage.clone());
        window_operator.open(&runtime_context).await.expect("Should be able to open operator");
        
        // Step 1: Process initial batch with timestamps [1000, 3000, 5000]
        let batch1 = create_test_batch(vec![1000, 3000, 5000], vec![10.0, 30.0, 50.0], vec!["A", "A", "A"]);
        let message1 = create_keyed_message(batch1, "A");
        
        let results1 = window_operator.process_message(message1).await;
        assert!(results1.is_some(), "Should have results for initial batch");
        
        let result_messages1 = results1.unwrap();
        let result_batch1 = result_messages1[0].record_batch();
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
        // This should affect the windows that include timestamp 2000
        let batch2 = create_test_batch(vec![2000], vec![20.0], vec!["A"]);
        let message2 = create_keyed_message(batch2, "A");
        
        let results2 = window_operator.process_message(message2).await;
        assert!(results2.is_some(), "Should have results for late entry");
        
        let result_messages2 = results2.unwrap();
        let result_batch2 = result_messages2[0].record_batch();
        // Should include results for the late entry and potentially updated results for affected windows
        assert!(result_batch2.num_rows() >= 1, "Should have at least 1 result row for late entry");
        
        let sum_column2 = result_batch2.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let count_column2 = result_batch2.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        
        // The late entry at t=2000 should create a window result
        // Window at t=2000 should include [1000, 2000] -> SUM=30.0, COUNT=2
        assert_eq!(sum_column2.value(0), 30.0, "Late entry SUM should be 30.0 (10.0+20.0)");
        assert_eq!(count_column2.value(0), 2, "Late entry COUNT should be 2");
        
        // Step 3: Process another batch to verify the late entry is properly integrated
        let batch3 = create_test_batch(vec![6000], vec![60.0], vec!["A"]);
        let message3 = create_keyed_message(batch3, "A");
        
        let results3 = window_operator.process_message(message3).await;
        assert!(results3.is_some(), "Should have results for final batch");
        
        let result_messages3 = results3.unwrap();
        let result_batch3 = result_messages3[0].record_batch();
        let sum_column3 = result_batch3.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let count_column3 = result_batch3.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        
        // Window at t=6000 should include [4000, 6000] range
        // Should include t=5000 (50.0) and t=6000 (60.0) -> SUM=110.0, COUNT=2
        assert_eq!(sum_column3.value(0), 110.0, "Final SUM should be 110.0 (50.0+60.0)");
        assert_eq!(count_column3.value(0), 2, "Final COUNT should be 2");
        
        window_operator.close().await.expect("Should be able to close operator");
    }

    #[tokio::test]
    async fn test_multiple_late_entries() {
        // Test handling multiple late entries in a single batch
        let sql = "SELECT 
            timestamp,
            value,
            partition_key,
            SUM(value) OVER w as sum_val
        FROM test_table
        WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)";
        
        let window_exec = extract_window_exec_from_sql(sql).await;
        let mut window_config = WindowOperatorConfig::new(window_exec);
        window_config.execution_mode = ExecutionMode::EventBased;
        window_config.parallelize = true;
        
        let storage = Arc::new(Storage::default());
        let operator_config = OperatorConfig::WindowConfig(window_config);
        let runtime_context = create_test_runtime_context();
        
        let mut window_operator = WindowOperator::new(operator_config, storage.clone());
        window_operator.open(&runtime_context).await.expect("Should be able to open operator");
        
        // Step 1: Process initial ordered batch [1000, 4000, 7000]
        let batch1 = create_test_batch(vec![1000, 4000, 7000], vec![10.0, 40.0, 70.0], vec!["A", "A", "A"]);
        let message1 = create_keyed_message(batch1, "A");
        
        let results1 = window_operator.process_message(message1).await;
        assert!(results1.is_some(), "Should have results for initial batch");
        
        // Step 2: Process batch with multiple late entries [2000, 3000, 5000, 6000] + one non-late entry [8000]
        // Late entries are relative to the last processed timestamp (7000)
        let batch2 = create_test_batch(vec![2000, 3000, 5000, 6000, 8000], vec![20.0, 30.0, 50.0, 60.0, 80.0], vec!["A", "A", "A", "A", "A"]);
        let message2 = create_keyed_message(batch2, "A");
        
        let results2 = window_operator.process_message(message2).await;
        assert!(results2.is_some(), "Should have results for late entries batch");
        
        let result_messages2 = results2.unwrap();
        let result_batch2 = result_messages2[0].record_batch();
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
        let batch3 = create_test_batch(vec![9000, 10000], vec![90.0, 100.0], vec!["A", "A"]);
        let message3 = create_keyed_message(batch3, "A");
        
        let results3 = window_operator.process_message(message3).await;
        assert!(results3.is_some(), "Should have results for final batch");
        
        let result_messages3 = results3.unwrap();
        let result_batch3 = result_messages3[0].record_batch();
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
        window_config.execution_mode = ExecutionMode::EventBased;
        window_config.parallelize = true;
        
        let storage = Arc::new(Storage::default());
        let operator_config = OperatorConfig::WindowConfig(window_config);
        
        let mut window_operator = WindowOperator::new(operator_config, storage);
        let runtime_context = create_test_runtime_context();
        window_operator.open(&runtime_context).await.expect("Should be able to open operator");

        // Test 1: All ordered events
        println!("\n=== Test 1: All Ordered Events ===");
        
        // Process batch with ordered timestamps [1000, 2000, 3000, 4000, 5000]
        let batch1 = create_test_batch(vec![1000, 2000, 3000, 4000, 5000], vec![10.0, 20.0, 30.0, 40.0, 50.0], vec!["A", "A", "A", "A", "A"]);
        let message1 = create_keyed_message(batch1, "A");
        
        let results1 = window_operator.process_message(message1).await;
        assert!(results1.is_some(), "Should have results for ordered batch");
        
        let result_messages1 = results1.unwrap();
        let result_batch1 = result_messages1[0].record_batch();
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
        println!("\n=== Test 2: Mixed Batch with Late Entries ===");
        
        // Process batch with late entries [1500, 6000, 2500] - 1500 and 2500 are late relative to 5000
        let batch2 = create_test_batch(vec![1500, 6000, 2500], vec![15.0, 60.0, 25.0], vec!["A", "A", "A"]);
        let message2 = create_keyed_message(batch2, "A");
        
        let results2 = window_operator.process_message(message2).await;
        assert!(results2.is_some(), "Should have results for mixed batch");
        
        let result_messages2 = results2.unwrap();
        let result_batch2 = result_messages2[0].record_batch();
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
        println!("\n=== Test 3: Verify Accumulator Integration ===");
        
        // Process more events to verify late entries have been integrated into accumulator state
        let batch3 = create_test_batch(vec![7000, 8000], vec![70.0, 80.0], vec!["A", "A"]);
        let message3 = create_keyed_message(batch3, "A");
        
        let results3 = window_operator.process_message(message3).await;
        assert!(results3.is_some(), "Should have results for final batch");
        
        let result_messages3 = results3.unwrap();
        let result_batch3 = result_messages3[0].record_batch();
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
        // Test tiled aggregates (MIN, MAX, AVG) with late entries
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
        window_config.execution_mode = ExecutionMode::EventBased;
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
        
        let storage = Arc::new(Storage::default());
        let operator_config = OperatorConfig::WindowConfig(window_config);
        let runtime_context = create_test_runtime_context();
        
        let mut window_operator = WindowOperator::new(operator_config, storage.clone());
        window_operator.open(&runtime_context).await.expect("Should be able to open operator");
        
        // Step 1: Process initial ordered batch to establish baseline
        // Timestamps spread across multiple tile buckets (minutes apart) with some close entries
        // [60000, 180000, 300000, 420000] = [1min, 3min, 5min, 7min] with values [10.0, 30.0, 50.0, 70.0]
        let batch1 = create_test_batch(
            vec![60000, 180000, 300000, 420000], 
            vec![10.0, 30.0, 50.0, 70.0], 
            vec!["A", "A", "A", "A"]
        );
        let message1 = create_keyed_message(batch1, "A");
        
        let results1 = window_operator.process_message(message1).await;
        assert!(results1.is_some(), "Should have results for initial batch");
        
        let result_messages1 = results1.unwrap();
        let result_batch1 = result_messages1[0].record_batch();
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
        // Late entries: [120000, 240000, 360000] + new entry [480000] = [2min, 4min, 6min, 8min]
        // This creates entries in different tile buckets while having some close entries within 5s window
        let batch2 = create_test_batch(
            vec![120000, 240000, 360000, 480000], 
            vec![20.0, 40.0, 60.0, 80.0], 
            vec!["A", "A", "A", "A"]
        );
        let message2 = create_keyed_message(batch2, "A");
        
        let results2 = window_operator.process_message(message2).await;
        assert!(results2.is_some(), "Should have results for late entries batch");
        
        let result_messages2 = results2.unwrap();
        let result_batch2 = result_messages2[0].record_batch();
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
        // Add data with some close entries that should create overlapping windows and benefit from tiles
        // [540000, 541000, 542000] = [9min, 9min+1s, 9min+2s] - close entries within 5s window
        let batch3 = create_test_batch(
            vec![540000, 541000, 542000], 
            vec![90.0, 100.0, 110.0], 
            vec!["A", "A", "A"]
        );
        let message3 = create_keyed_message(batch3, "A");
        
        let results3 = window_operator.process_message(message3).await;
        assert!(results3.is_some(), "Should have results for final batch");
        
        let result_messages3 = results3.unwrap();
        let result_batch3 = result_messages3[0].record_batch();
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
        // Use different tile buckets for partition B: [60000, 120000, 180000] = [1min, 2min, 3min]
        let batch4 = create_test_batch(
            vec![60000, 120000, 180000], 
            vec![5.0, 15.0, 25.0], 
            vec!["B", "B", "B"]
        );
        let message4 = create_keyed_message(batch4, "B");
        
        let results4 = window_operator.process_message(message4).await;
        assert!(results4.is_some(), "Should have results for partition B");
        
        let result_messages4 = results4.unwrap();
        let result_batch4 = result_messages4[0].record_batch();
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

}