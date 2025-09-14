use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::datatypes::{Schema, SchemaBuilder, SchemaRef};
use async_trait::async_trait;
use futures::future;
use tokio_rayon::rayon::{ThreadPool, ThreadPoolBuilder};
use tokio_rayon::AsyncThreadPool;

use datafusion::logical_expr::Accumulator;
use datafusion::physical_expr::window::SlidingAggregateWindowExpr;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use uuid::Uuid;
use crate::common::message::Message;
use crate::common::Key;
use crate::runtime::operators::operator::{OperatorBase, OperatorConfig, OperatorTrait, OperatorType};
use crate::runtime::operators::window::state::state::{State, WindowId, WindowState};
use crate::runtime::operators::window::time_index::{TimeIdx, TimeIndex, advance_window_position};
use crate::runtime::runtime_context::RuntimeContext;
use crate::storage::storage::{BatchId, Storage};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionMode {
    EventBased, // all events are processed immediately, late events are handled with best effort
    WatermarkBased, // events are buffered until watermark is reached, late events are dropped
}

#[derive(Debug, Clone)]
pub struct WindowConfig {
    pub window_exec: Arc<BoundedWindowAggExec>,
    pub execution_mode: ExecutionMode,
    pub parallelize: bool,
}

impl WindowConfig {
    pub fn new(window_exec: Arc<BoundedWindowAggExec>) -> Self {    
        Self {
            window_exec,
            execution_mode: ExecutionMode::WatermarkBased,
            parallelize: false,
        }
    }
}

#[derive(Debug)]
pub struct WindowOperator {
    base: OperatorBase,
    windows: HashMap<WindowId, Arc<dyn WindowExpr>>,
    windows_state: State,
    time_index: TimeIndex,
    ts_column_index: usize,
    keys_to_process: HashSet<Key>,
    execution_mode: ExecutionMode,
    parallelize: bool,
    thread_pool: Option<ThreadPool>,
    output_schema: SchemaRef,
}

impl WindowOperator {
    pub fn new(config: OperatorConfig, storage: Arc<Storage>) -> Self {
        let window_config = match config.clone() {
            OperatorConfig::WindowConfig(window_config) => window_config,
            _ => panic!("Expected WindowConfig, got {:?}", config),
        };

        let ts_column_index = window_config.window_exec.window_expr()[0].order_by()[0].expr.as_any().downcast_ref::<Column>().expect("Expected Column expression in ORDER BY").index();

        let mut windows = HashMap::new();
        for (window_id, window_expr) in window_config.window_exec.window_expr().iter().enumerate() {
            windows.insert(window_id, window_expr.clone());
        }

        let output_schema = create_output_schema(&window_config.window_exec.input().schema(), &window_config.window_exec.window_expr());

        let thread_pool = if window_config.parallelize {
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
            windows_state: State::new(),
            time_index: TimeIndex::new(),
            ts_column_index,
            keys_to_process: HashSet::new(),
            execution_mode: window_config.execution_mode,
            parallelize: window_config.parallelize,
            thread_pool,
            output_schema,
        }
    }

    async fn process_keys(&self, keys: &HashSet<Key>) -> RecordBatch {
        let futures: Vec<_> = keys.iter()
            .map(|key| self.advance_windows(key))
            .collect();
        
        let results = future::join_all(futures).await;
        
        if results.is_empty() {
            return RecordBatch::new_empty(self.output_schema.clone());
        }
        
        arrow::compute::concat_batches(&self.output_schema, &results)
            .expect("Should be able to concat result batches")
    }

    async fn advance_windows(&self, key: &Key) -> RecordBatch {
        let futures: Vec<_> = self.windows.keys()
            .map(|window_id| self.advance_window(key, *window_id))
            .collect();
        
        let results = future::join_all(futures).await;
        concat_results(results, &self.output_schema)
    }

    // moves window to the end of time index
    async fn advance_window(&self, key: &Key, window_id: WindowId) -> Vec<ScalarValue> {
        let mut window_state = match self.windows_state.get_window_state(key, window_id).await {
            Some(state) => state,
            None => WindowState {
                accumulator_state: None,
                start_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
                end_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
            }
        };
    
        let window_expr = self.windows[&window_id].clone();
        let window_frame = window_expr.get_window_frame();
        let time_entries = self.time_index.get_time_index(key).expect("Time entries should exist");
        
        let updates_and_retracts = advance_window_position(window_frame, &mut window_state, &time_entries);

        // TODO this should be an incremental iterator
        let batches = self.load_bacthes(key, &updates_and_retracts).await;
        
        // CPU heavy part - use parallel version if configured
        let (results, accumulator_state) = if self.parallelize {
            run_accumulator_parallel(
                self.thread_pool.as_ref().expect("ThreadPool should exist"), 
                window_expr.clone(), 
                updates_and_retracts, 
                batches, 
                window_state.accumulator_state
            ).await
        } else {
            run_accumulator(
                &window_expr, 
                &updates_and_retracts, 
                &batches, window_state.accumulator_state
            )
        };

        // update final accumulator state
        window_state.accumulator_state = Some(accumulator_state);

        // update window state
        self.windows_state.insert_window_state(key, window_id, window_state).await;
        
        results
    }

    // TODO we should make an iterator here with fixed loaded memory and pre-loading
    async fn load_bacthes(
        &self,
        key: &Key,
        updates_and_retracts: &Vec<(TimeIdx, Vec<TimeIdx>)>
    ) -> HashMap<BatchId, RecordBatch> {
        let mut batches_to_load = std::collections::BTreeSet::new();
        for (update_idx, retracts) in updates_and_retracts {
            batches_to_load.insert(update_idx.batch_id.clone());
            for retract_idx in retracts {
                batches_to_load.insert(retract_idx.batch_id.clone());
            }
        }

        self.base.storage.load_batches(batches_to_load.into_iter().collect(), key).await
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

        // TODO based one execution mode (event vs watermark based) we may need to drop late events

        // TODO calculate pre-aggregates

        // TODO pruning
        
        let batches = storage.append_records(message.record_batch().clone(), partition_key, self.ts_column_index).await;
        for (batch_id, batch) in batches {
            self.time_index.update_time_index(partition_key, batch_id, &batch, self.ts_column_index);
        }
        
        if self.execution_mode == ExecutionMode::WatermarkBased {
            // buffer for processing on watermark
            self.keys_to_process.insert(partition_key.clone());
            None
        } else {
            // immidiate processing for event based mode
            let mut keys = HashSet::new();
            keys.insert(partition_key.clone());
            let result = self.process_keys(&keys).await;
            // vertex_id will be set by stream task
            // TODO ingest timestamp?
            Some(vec![Message::new(None, result, None)])
        }
    }

    async fn process_watermark(&mut self, _watermark: u64) -> Option<Vec<Message>> {
        if self.execution_mode == ExecutionMode::EventBased {
            panic!("EventBased execution mode does not support watermark processing");
        }
        let result = self.process_keys(&self.keys_to_process).await;
        self.keys_to_process.clear();
        // vertex_id will be set by stream task
        // TODO ingest timestamp?
        Some(vec![Message::new(None, result, None)])
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }
}

fn create_sliding_accumulator(window_expr: &Arc<dyn WindowExpr>) -> Box<dyn Accumulator> {
    let aggregate_expr = window_expr.as_any()
        .downcast_ref::<SlidingAggregateWindowExpr>()
        .expect("Only SlidingAggregateWindowExpr is supported");
    
    let accumulator = aggregate_expr.get_aggregate_expr().create_sliding_accumulator()
        .expect("Should be able to create accumulator");

    if !accumulator.supports_retract_batch() {
        panic!("Accumulator {:?} does not support retract batch", accumulator);
    }

    accumulator
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

fn run_accumulator(
    window_expr: &Arc<dyn WindowExpr>, 
    updates_and_retracts: &Vec<(TimeIdx, Vec<TimeIdx>)>, 
    batches: &HashMap<BatchId, RecordBatch>,
    previous_accumulator_state: Option<Vec<ScalarValue>>
) -> (Vec<ScalarValue>, Vec<ScalarValue>) {
    let mut accumulator = create_sliding_accumulator(&window_expr);
    if let Some(accumulator_state) = previous_accumulator_state {
        let state_arrays: Vec<ArrayRef> = accumulator_state
            .iter()
            .map(|sv| sv.to_array_of_size(1))
            .collect::<Result<Vec<_>, _>>()
            .expect("Should be able to convert scalar values to arrays");
        
        accumulator.merge_batch(&state_arrays).expect("Should be able to merge accumulator state");
    }

    let mut results = Vec::new();

    for (update_idx, retract_idxs) in updates_and_retracts {
        let update_batch = batches.get(&update_idx.batch_id).expect("Update batch should exist");
        // Extract single row from update batch using the row index
        let update_row_batch = {
            let indices = UInt64Array::from(vec![update_idx.row_idx as u64]);
            arrow::compute::take_record_batch(update_batch, &indices)
                .expect("Should be able to take row from batch")
        };

        let update_args = window_expr.evaluate_args(&update_row_batch)
            .expect("Should be able to evaluate window args");

        accumulator.update_batch(&update_args).expect("Should be able to update accumulator");

        // Handle retractions
        if !retract_idxs.is_empty() {
            // Extract retract rows in original order
            let mut retract_batches = Vec::new();
            for retract_idx in retract_idxs {
                let batch = batches.get(&retract_idx.batch_id).expect("Retract batch should exist");
                let indices_array = UInt64Array::from(vec![retract_idx.row_idx as u64]);
                let retract_batch = arrow::compute::take_record_batch(batch, &indices_array)
                    .expect("Should be able to take row from batch");
                retract_batches.push(retract_batch);
            }

            // Concatenate retract batches if multiple, otherwise use single batch
            let final_retract_batch = if retract_batches.len() == 1 {
                retract_batches.into_iter().next().unwrap()
            } else {
                let schema = retract_batches[0].schema();
                arrow::compute::concat_batches(&schema, &retract_batches)
                    .expect("Should be able to concat retract batches")
            };

            let retract_args = window_expr.evaluate_args(&final_retract_batch)
                .expect("Should be able to evaluate retract args");

            accumulator.retract_batch(&retract_args).expect("Should be able to retract from accumulator");
        }

        let result = accumulator.evaluate().expect("Should be able to evaluate accumulator");
        results.push(result);
    }

    (results, accumulator.state().expect("Should be able to get accumulator state"))
}

async fn run_accumulator_parallel(
    thread_pool: &ThreadPool,
    window_expr: Arc<dyn WindowExpr>, 
    updates_and_retracts: Vec<(TimeIdx, Vec<TimeIdx>)>, 
    batches: HashMap<BatchId, RecordBatch>,
    previous_accumulator_state: Option<Vec<ScalarValue>>
) -> (Vec<ScalarValue>, Vec<ScalarValue>) {
    let result = thread_pool.spawn_fifo_async(move || {
        run_accumulator(&window_expr, &updates_and_retracts, &batches, previous_accumulator_state)
    }).await;
    
     result
}

fn concat_results(results: Vec<Vec<ScalarValue>>, output_schema: &SchemaRef) -> RecordBatch {
    if results.is_empty() {
        return RecordBatch::new_empty(output_schema.clone());
    }

    // Each Vec<ScalarValue> represents results for one window across multiple time points
    // We need to transpose this to create columns for each window
    let num_windows = results.len();
    let num_rows = if num_windows > 0 { results[0].len() } else { 0 };

    if num_rows == 0 {
        return RecordBatch::new_empty(output_schema.clone());
    }

    let mut columns: Vec<ArrayRef> = Vec::new();

    // Use the schema from BoundedWindowAggExec to ensure correct field names and types
    let schema_fields = output_schema.fields();
    
    for (_, window_results) in results.iter().enumerate() {
        // Convert Vec<ScalarValue> to ArrayRef
        let array = ScalarValue::iter_to_array(window_results.iter().cloned())
            .expect("Should be able to convert scalar values to array");
        
        columns.push(array);
    }

    // Ensure we have the right number of columns for the schema
    if columns.len() != schema_fields.len() {
        panic!("Mismatch between number of result columns ({}) and schema fields ({})", 
               columns.len(), schema_fields.len());
    }

    RecordBatch::try_new(output_schema.clone(), columns)
        .expect("Should be able to create RecordBatch from window results")
}