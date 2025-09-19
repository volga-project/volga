use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::datatypes::{Schema, SchemaBuilder, SchemaRef};
use async_trait::async_trait;
use futures::future;
use tokio_rayon::rayon::{ThreadPool, ThreadPoolBuilder};
use tokio_rayon::AsyncThreadPool;

use datafusion::logical_expr::{window_state, Accumulator};
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
    windows: BTreeMap<WindowId, Arc<dyn WindowExpr>>,
    windows_state: State,
    time_index: TimeIndex,
    ts_column_index: usize,
    keys_to_process: HashSet<Key>,
    execution_mode: ExecutionMode,
    parallelize: bool,
    thread_pool: Option<ThreadPool>,
    output_schema: SchemaRef,
    input_schema: SchemaRef,
}

impl WindowOperator {
    pub fn new(config: OperatorConfig, storage: Arc<Storage>) -> Self {
        let window_config = match config.clone() {
            OperatorConfig::WindowConfig(window_config) => window_config,
            _ => panic!("Expected WindowConfig, got {:?}", config),
        };

        let ts_column_index = window_config.window_exec.window_expr()[0].order_by()[0].expr.as_any().downcast_ref::<Column>().expect("Expected Column expression in ORDER BY").index();

        let mut windows = BTreeMap::new();
        for (window_id, window_expr) in window_config.window_exec.window_expr().iter().enumerate() {
            windows.insert(window_id, window_expr.clone());
        }

        let input_schema = window_config.window_exec.input().schema();
        let output_schema = create_output_schema(&input_schema, &window_config.window_exec.window_expr());

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
            input_schema,
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
        let window_ids: Vec<_> = self.windows.keys().cloned().collect();
        
        // Step 1: Load window states and generate updates_and_retracts for all windows concurrently
        let state_futures: Vec<_> = window_ids.iter()
            .map(|&window_id| async move {
                let mut window_state = match self.windows_state.get_window_state(key, window_id).await {
                    Some(state) => state,
                    None => WindowState {
                        accumulator_state: None,
                        start_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
                        end_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
                    }
                };
                
                let window_expr = &self.windows[&window_id];
                let window_frame = window_expr.get_window_frame();
                let time_entries = self.time_index.get_time_index(key).expect("Time entries should exist");
                
                let updates_and_retracts = advance_window_position(window_frame, &mut window_state, &time_entries);
                
                (window_id, window_state, updates_and_retracts)
            })
            .collect();
        
        let window_data = future::join_all(state_futures).await;
        
        // Step 2: Compose batches_to_load from all updates_and_retracts
        let mut batches_to_load = std::collections::BTreeSet::new();
        for (_, _, updates_and_retracts) in window_data.iter() {
            for (update_idx, retract_idxs) in updates_and_retracts {
                batches_to_load.insert(update_idx.batch_id);
                for retract_idx in retract_idxs {
                    batches_to_load.insert(retract_idx.batch_id);
                }
            }
        }
        
        // Step 3: Get relevant data from storage
        let batches = self.base.storage.load_batches(batches_to_load.into_iter().collect(), key).await;
        
        // Step 4: Concurrently run_accumulator for all windows
        let batches = Arc::new(batches);
        let accumulator_futures: Vec<_> = window_data.iter()
            .map(|(window_id, window_state, updates_and_retracts)| {
                let window_expr = self.windows[window_id].clone();
                let batches_clone = batches.clone();
                let accumulator_state_clone = window_state.accumulator_state.clone();
                let window_id_copy = *window_id;
                let window_state_clone = window_state.clone();
                
                async move {
                    let (results, accumulator_state) = if self.parallelize {
                        run_accumulator_parallel(
                            self.thread_pool.as_ref().expect("ThreadPool should exist"), 
                            window_expr, 
                            updates_and_retracts.clone(), 
                            (*batches_clone).clone(), 
                            accumulator_state_clone
                        ).await
                    } else {
                        run_accumulator(
                            &window_expr, 
                            updates_and_retracts, 
                            &batches_clone, 
                            accumulator_state_clone
                        )
                    };
                    
                    (window_id_copy, results, accumulator_state, window_state_clone)
                }
            })
            .collect();
        
        let accumulator_results = future::join_all(accumulator_futures).await;
        
        // Update window states
        for (window_id, _, accumulator_state, window_state) in &accumulator_results {
            let mut updated_window_state = window_state.clone();
            updated_window_state.accumulator_state = Some(accumulator_state.clone());
            self.windows_state.insert_window_state(key, *window_id, updated_window_state).await;
        }
        
        // Step 5: Extract input column values from all update rows
        let first_updates_and_retracts = &window_data.first().expect("Window data should exist").2;
        let mut input_values = Vec::new();
        
        // Extract input values for each update row
        // Window operator schema is fixed: input columns first, then window columns
        let input_column_count = self.input_schema.fields().len();
        
        for (update_idx, _) in first_updates_and_retracts {
            let batch = batches.get(&update_idx.batch_id).expect("Batch should exist");
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
        
        // Step 6: Concat results and create a batch
        let results: Vec<Vec<ScalarValue>> = accumulator_results.into_iter()
            .map(|(_, results, _, _)| results)
            .collect();
        
        concat_results(results, input_values, &self.output_schema, &self.input_schema)
    }

    // async fn update_accum_with_late_events(window_state: &mut WindowState, sorted_late_tsx: Vec<TimeIdx>, batches: &HashMap<BatchId, RecordBatch>) {
    //     let accum
    // }
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
        let mut window_config = WindowConfig::new(window_exec);
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
        let mut window_config = WindowConfig::new(window_exec);
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
        let mut window_config = WindowConfig::new(window_exec);
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
}