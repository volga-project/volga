use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::ops::Bound;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use futures::StreamExt;
use indexmap::IndexMap;

use datafusion::scalar::ScalarValue;

use crate::common::message::Message;
use crate::common::Key;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::operators::operator::{MessageStream, OperatorBase, OperatorConfig, OperatorPollResult, OperatorTrait, OperatorType};
use crate::runtime::operators::window::aggregates::{split_entries_for_parallelism, Aggregation};
use crate::runtime::operators::window::aggregates::plain::PlainAggregation;
use crate::runtime::operators::window::aggregates::retractable::RetractableAggregation;
use crate::runtime::operators::window::window_operator_state::{WindowOperatorState, WindowId};
use crate::runtime::operators::window::time_entries::{TimeEntries, TimeIdx};
use crate::runtime::operators::window::{AggregatorType, TileConfig};
use crate::runtime::operators::window::window_operator::{
    init, is_entry_late, is_ts_too_late, load_batches, produce_aggregates, stack_concat_results, WindowConfig, WindowOperatorConfig
};
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::state::OperatorState;
use crate::storage::batch_store::{extract_timestamp, BatchId};
use tokio_rayon::rayon::ThreadPool;
use tokio::time::{sleep, Duration, Instant};

#[derive(Debug, Clone)]
pub struct WindowRequestOperatorConfig {
    pub window_exec: Arc<BoundedWindowAggExec>,
    pub tiling_configs: Vec<Option<TileConfig>>,
    pub parallelize: bool,
    pub lateness: Option<i64>,
}

impl WindowRequestOperatorConfig {
    pub fn from_window_operator_config(window_operator_config: WindowOperatorConfig) -> Self {    
        Self {
            window_exec: window_operator_config.window_exec,
            tiling_configs: window_operator_config.tiling_configs,
            parallelize: window_operator_config.parallelize,
            lateness: window_operator_config.lateness
        }
    }
}

pub struct WindowRequestOperator {
    base: OperatorBase,
    window_configs: BTreeMap<WindowId, WindowConfig>,
    state: Option<Arc<dyn OperatorState>>,
    ts_column_index: usize,
    parallelize: bool,
    thread_pool: Option<ThreadPool>,
    output_schema: SchemaRef,
    input_schema: SchemaRef,
    lateness: Option<i64>,
}

impl fmt::Debug for WindowRequestOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WindowRequestOperator")
        .field("base", &self.base)
        .field("windows", &self.window_configs)
        .field("state", &self.state)

        .field("parallelize", &self.parallelize)
        .field("thread_pool", &self.thread_pool)
        .field("output_schema", &self.output_schema)
        .field("input_schema", &self.input_schema)
        .finish()
    }
}

impl WindowRequestOperator {
    pub fn new(config: OperatorConfig) -> Self {
        let window_request_operator_config = match config.clone() {
            OperatorConfig::WindowRequestConfig(config) => config,
            _ => panic!("Expected WindowRequestConfig, got {:?}", config),
        };

        let (ts_column_index, windows, input_schema, output_schema, thread_pool) = init(
            true, &window_request_operator_config.window_exec, &window_request_operator_config.tiling_configs, window_request_operator_config.parallelize
        );

        Self {
            base: OperatorBase::new(config),
            window_configs: windows,
            state: None,
            ts_column_index,
            parallelize: window_request_operator_config.parallelize,
            thread_pool,
            output_schema,
            input_schema,
            lateness: window_request_operator_config.lateness,
        }
    }

    fn get_state(&self) -> &WindowOperatorState {
        self.state.as_ref()
            .expect("State should be initialized")
            .as_any()
            .downcast_ref::<WindowOperatorState>()
            .expect("State should be WindowOperatorState")
    }

    // TODO use info from logical graph to get window_operator<->window_request_operator pair
    fn get_peer_window_operator_vertex_id(&self, vertex_id: String, graph: &ExecutionGraph) -> String {
        let current_vertex = graph.get_vertex(&vertex_id)
            .expect(&format!("Vertex {} not found in execution graph", vertex_id));
        let task_index = current_vertex.task_index;

        // Collect all window operator vertices
        let mut window_operator_ids = HashSet::new();
        let mut window_vertices = Vec::new();
        
        for (vid, vertex) in graph.get_vertices() {
            if let OperatorConfig::WindowConfig(_) = &vertex.operator_config {
                window_operator_ids.insert(vertex.operator_id.clone());
                window_vertices.push((vid.clone(), vertex));
            }
        }

        // Ensure there's only one window operator (by operator_id) in the graph
        if window_operator_ids.len() > 1 {
            panic!("Multiple window operators found in execution graph: {:?}. Expected exactly one.", window_operator_ids);
        }

        if window_operator_ids.is_empty() {
            panic!("No window operator found in execution graph for vertex {}", vertex_id);
        }

        // Find the window operator vertex with the same task_index
        for (vid, vertex) in window_vertices {
            if vid == vertex_id {
                continue;
            }
            
            if vertex.task_index == task_index {
                return vid;
            }
        }

        panic!("No window operator vertex found with task_index {} for vertex {}", task_index, vertex_id)
    }

    fn create_virtual_entries(
        &self,
        record_batch: &RecordBatch,
        time_entries: &TimeEntries,
    ) -> (Vec<Option<TimeIdx>>, BatchId) {
        let mut virtual_entries = Vec::with_capacity(record_batch.num_rows());
        let last_entry = time_entries.entries.back().map(|entry| *entry);
        let first_entry = time_entries.entries.front().map(|entry| *entry);

        let batch_id = BatchId::random(); // virtual batch id

        // TODO can we use arrow::compute::* kernels here for SIMD?
        for row_idx in 0..record_batch.num_rows() {
            let timestamp = extract_timestamp(record_batch.column(self.ts_column_index), row_idx);

            let search_key = TimeIdx {
                timestamp,
                pos_idx: usize::MAX,
                batch_id,
                row_idx: 0,
            };

            // filter out entries before first entry
            if let Some(first_entry) = first_entry {
                if timestamp < first_entry.timestamp {
                    virtual_entries.push(None);
                    continue;
                }
            }

            // filter out late entries
            if let (Some(lateness_ms), Some(last_entry)) = (self.lateness, last_entry) {
                if is_ts_too_late(timestamp, last_entry, lateness_ms) {
                    virtual_entries.push(None);
                    continue;
                }
            }

            let pos_idx = time_entries
                .entries
                .upper_bound(Bound::Included(&search_key))
                .map(|existing_entry| {
                    if existing_entry.timestamp == timestamp {
                        existing_entry.pos_idx + 1
                    } else {
                        0
                    }
                })
                .unwrap_or(0);

            virtual_entries.push(Some(TimeIdx {
                timestamp,
                pos_idx,
                batch_id,
                row_idx,
            }));
        }

        (virtual_entries, batch_id)
    }

    async fn process_key(&self, key: &Key, record_batch: &RecordBatch) -> RecordBatch {
        let windows_state_guard = self.get_state().get_windows_state(key).await;

        if windows_state_guard.is_none() {
            return RecordBatch::new_empty(self.output_schema.clone());
        }

        let windows_state_guard = windows_state_guard.unwrap();
        let windows_state = windows_state_guard.value();
        
        let time_entries = &windows_state.time_entries;
        let (virtual_entries, temp_batch_id) = self.create_virtual_entries(record_batch, time_entries);
        
        let last_entry = time_entries.latest_idx();

        let mut orig_positions = HashMap::new();
        let mut aggregations = IndexMap::new();
        
        for (window_id, window_config) in &self.window_configs {
            let window_frame = window_config.window_expr.get_window_frame();
            let window_state = windows_state.window_states.get(window_id).expect("Window state should exist");
            let aggregator_type = self.window_configs[window_id].aggregator_type;
            let tiles = &window_state.tiles;
            let accumulator_state = window_state.accumulator_state.as_ref();
            
            let mut aggs: Vec<Box<dyn Aggregation>> = Vec::new();
            let mut agg_idx = 0;

            let mut plain_agg_entries = Vec::new();
            let mut plain_agg_orig_positions = Vec::new();
            let mut eval_agg_entries = Vec::new();
            let mut eval_agg_orig_positions = Vec::new();

            for i in 0..virtual_entries.len() {
                if let Some(entry) = virtual_entries[i] {
                    let is_late = is_entry_late(entry, last_entry);
                    let aggregator_type = if is_late {
                        // lates are handled as plain
                        AggregatorType::PlainAccumulator
                    } else {
                        aggregator_type
                    };

                    match aggregator_type {
                        AggregatorType::RetractableAccumulator => {
                            let (retracts, _) = time_entries.find_retracts(window_frame, window_state.start_idx, entry);
                            let aggregation: Box<dyn Aggregation> = Box::new(RetractableAggregation::new(
                                vec![entry],
                                window_config.window_expr.clone(),
                                tiles.clone(),
                                Some(vec![retracts]),
                                accumulator_state.cloned(),
                            ));
                            aggs.push(aggregation);
                            orig_positions.insert((*window_id, agg_idx), vec![i]);
                            agg_idx += 1;
                        }
                        AggregatorType::PlainAccumulator => {
                            plain_agg_entries.push(entry);
                            plain_agg_orig_positions.push(i);
                        }
                        AggregatorType::Evaluator => {
                            eval_agg_entries.push(entry);
                            eval_agg_orig_positions.push(i);
                        }
                    }
                }
            }

            let mut pos_idx = 0;
            for entries in split_entries_for_parallelism(&plain_agg_entries) {
                let entries_len = entries.len();
                let aggregation: Box<dyn Aggregation> = Box::new(PlainAggregation::new(
                    entries,
                    window_config.window_expr.clone(),
                    tiles.clone(),
                    time_entries,
                ));
                aggs.push(aggregation);
                let orig_pos = plain_agg_orig_positions[pos_idx..pos_idx + entries_len].to_vec();
                orig_positions.insert((*window_id, agg_idx), orig_pos);
                pos_idx += entries_len;
                agg_idx += 1;
            }

            pos_idx = 0;
            for entries in split_entries_for_parallelism(&eval_agg_entries) {
                let entries_len = entries.len();
                let aggregation: Box<dyn Aggregation> = Box::new(PlainAggregation::new(
                    entries,
                    window_config.window_expr.clone(),
                    tiles.clone(),
                    time_entries,
                ));
                aggs.push(aggregation);
                let orig_pos = eval_agg_orig_positions[pos_idx..pos_idx + entries_len].to_vec();
                orig_positions.insert((*window_id, agg_idx), orig_pos);
                pos_idx += entries_len;
                agg_idx += 1;
            }

            aggregations.insert(*window_id, aggs);
        }

        // Drop guard after creating all aggregations - they own their data now
        drop(windows_state_guard);

        // Load batches
        let state = self.get_state();
        let mut batches = load_batches(state.get_batch_store(), key, &aggregations).await;
        
        // Add current batch
        batches.insert(temp_batch_id, record_batch.clone());

        // Run aggregation
        let aggregation_results = produce_aggregates(&self.window_configs, &aggregations, &batches, self.thread_pool.as_ref()).await;
        
        let mut aggregated_values = Vec::new();
        
        // Insert aggregated values into original positions
        for (window_id, aggs) in aggregation_results {
            let null_scalar = ScalarValue::Null;
            let mut values: Vec<ScalarValue> = vec![null_scalar.clone(); virtual_entries.len()];
            
            for (agg_idx, (agg_values, _)) in aggs.iter().enumerate() {
                let orig_pos = orig_positions.get(&(window_id, agg_idx)).expect("orig position should exist");
                if agg_values.len() != orig_pos.len() {
                    panic!("Mismatch between aggregate values count ({}) and original positions count ({})", agg_values.len(), orig_pos.len());
                }
                for (i, scalar) in orig_pos.iter().zip(agg_values.iter()) {
                    values[*i] = scalar.clone();
                }
            }
            
            aggregated_values.push(values);
        }
        
        let input_values = get_input_values(&record_batch, &self.input_schema);

        // Stack input value rows and result rows producing single result batch
        stack_concat_results(input_values, aggregated_values, &self.output_schema, &self.input_schema)
    }
}


// Extract input values (values which were in original argument batch, but were not aggregated, e.g keys) 
// for each update row.
// We assume window operator schema is fixed: input columns first, then window columns
fn get_input_values(
    batch: &RecordBatch, 
    input_schema: &SchemaRef
) -> Vec<Vec<ScalarValue>> {
    let mut input_values = Vec::new();
        
    let input_column_count = input_schema.fields().len();
    
    for row_idx in 0..batch.num_rows() {
        let mut row_input_values = Vec::new();
        for col_idx in 0..input_column_count {
            let array = batch.column(col_idx);
            let scalar_value = ScalarValue::try_from_array(array, row_idx)
                .expect("Should be able to extract scalar value");
            row_input_values.push(scalar_value);
        }
        input_values.push(row_input_values);
    }
    input_values
}


#[async_trait]
impl OperatorTrait for WindowRequestOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await?;
        
        let vertex_id = context.vertex_id().to_string();
        let operator_states = context.operator_states();
        let window_operator_vertex_id = self.get_peer_window_operator_vertex_id(vertex_id.clone(), context.execution_graph());
        
        let timeout = Duration::from_secs(2);
        let retry_interval = Duration::from_millis(100);
        let start = Instant::now();
        
        loop {
            if let Some(state_arc) = operator_states.get_operator_state(&window_operator_vertex_id) {
                self.state = Some(state_arc);
                break;
            }
            
            if start.elapsed() >= timeout {
                panic!("Timeout waiting for window operator state for vertex_id: {}", window_operator_vertex_id);
            }
            
            sleep(retry_interval).await;
        }

        return Ok(());
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

    async fn poll_next(&mut self) -> OperatorPollResult {
        let input_stream = self.base.input.as_mut().expect("input stream not set");
        
        match input_stream.next().await {
            Some(message) => {
                let ingest_ts = message.ingest_timestamp();
                let extras = message.get_extras();
                
                match message {
                    Message::Keyed(keyed_message) => {
                        let key = keyed_message.key();
                        
                        // read-only access to windows state
                        // let state = self.get_state();

                        // TODO we have a race condition here:
                        // getting state copy is ok even if winow operator updates previous version of it
                        // The problem is that window operator may prune batches (they are not part of the state, we only have references to them) that are still used by request operator for this version of state.
                        // We need to somehow sync this or add a flag to state to indicate which batches are still used by request operator
                        // eg similar to mvcc pattern
                            
                        // let result = if let Some(windows_state) = windows_state {
                        //     self.process_key(&key, &windows_state, &keyed_message.base.record_batch.clone()).await
                        // } else {
                        //     RecordBatch::new_empty(self.output_schema.clone())
                        // };


                        let result = self.process_key(&key, &keyed_message.base.record_batch.clone()).await;
                        OperatorPollResult::Ready(Message::new(None, result, ingest_ts, extras))
                    },
                    Message::Watermark(watermark) => {
                        // pass through
                        return OperatorPollResult::Ready(Message::Watermark(watermark));
                    },
                    _ => {
                        panic!("Window request operator expects keyed messages only");
                    }
                }
            }
            None => OperatorPollResult::None,
        }
    }
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
    use crate::runtime::operators::window::window_operator::{ExecutionMode, UpdateMode, WindowOperator, WindowOperatorConfig};
    use crate::common::message::Message;
    use crate::runtime::runtime_context::RuntimeContext;
    use crate::runtime::state::OperatorStates;
    use crate::common::Key;
    use futures::stream;

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

    #[tokio::test]
    async fn test_window_request_operator() {
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
        let mut window_config = WindowOperatorConfig::new(window_exec.clone());
        window_config.update_mode = UpdateMode::PerMessage;
        window_config.execution_mode = ExecutionMode::Request;
        window_config.parallelize = true;
        window_config.lateness = Some(2000); // 2 seconds lateness tolerance

        let operator_states = Arc::new(OperatorStates::new());
        let window_operator_vertex_id = "window_op".to_string();
        let request_operator_vertex_id = "window_request_op".to_string();

        // Create and set up window operator
        let mut window_operator = WindowOperator::new(OperatorConfig::WindowConfig(window_config.clone()));
        let window_context = RuntimeContext::new(
            window_operator_vertex_id.clone(),
            0,
            1,
            None,
            Some(operator_states.clone()),
            None,
        );
        window_operator.open(&window_context).await.expect("Should be able to open window operator");

        // Create request operator
        let request_config = WindowRequestOperatorConfig::from_window_operator_config(window_config.clone());
        let mut request_operator = WindowRequestOperator::new(
            OperatorConfig::WindowRequestConfig(request_config.clone()),
        );

        // Create execution graph with both vertices
        use crate::runtime::execution_graph::ExecutionVertex;
        let mut execution_graph = ExecutionGraph::new();
        
        let window_vertex = ExecutionVertex::new(
            window_operator_vertex_id.clone(),
            "window_op".to_string(),
            OperatorConfig::WindowConfig(window_config),
            1,
            0,
        );
        execution_graph.add_vertex(window_vertex);
        
        let request_vertex = ExecutionVertex::new(
            request_operator_vertex_id.clone(),
            "window_request_op".to_string(),
            OperatorConfig::WindowRequestConfig(request_config),
            1,
            0,
        );
        execution_graph.add_vertex(request_vertex);

        let request_context = RuntimeContext::new(
            request_operator_vertex_id.clone(),
            0,
            1,
            None,
            Some(operator_states.clone()),
            Some(execution_graph),
        );

        // Open request operator - it will find the peer window operator vertex from the execution graph
        request_operator.open(&request_context).await.expect("Should open");

        // Feed data to window operator to populate state
        let batch1 = create_test_batch(vec![1000, 2000], vec![10.0, 20.0], vec!["A", "A"]);
        let message1 = create_keyed_message(batch1, "A");
        let batch2 = create_test_batch(vec![3000], vec![30.0], vec!["A"]);
        let message2 = create_keyed_message(batch2, "A");

        let input_stream = Box::pin(stream::iter(vec![message1, message2]));
        window_operator.set_input(Some(input_stream));

        // Process messages to populate state
        window_operator.poll_next().await;
        window_operator.poll_next().await;

        // Create request batch with multiple entries:
        // - Regular entry: 3500 (on time, > latest entry 3000, within window)
        // - Late entry: 1500 (late but within lateness tolerance of 2000ms, last entry is 3000)
        // - Too late entry: 0 (too late, should be dropped)
        let request_batch = create_test_batch(vec![3500, 1500, 0], vec![35.0, 15.0, 5.0], vec!["A", "A", "A"]);
        let request_message = create_keyed_message(request_batch, "A");

        let request_stream = Box::pin(stream::iter(vec![request_message]));
        request_operator.set_input(Some(request_stream));

        // Process request
        let result = request_operator.poll_next().await;
        let result_message = result.get_result_message();
        let result_batch = result_message.record_batch();

        // Too late entry at 0 should be dropped, so we expect 2 rows (2500 and 1500)
        assert_eq!(result_batch.num_rows(), 2, "Should have 2 result rows (too late entry at 0 should be dropped)");
        assert_eq!(result_batch.num_columns(), 8, "Should have 8 columns (timestamp, value, partition_key, sum_val, count_val, avg_val, min_val, max_val)");

        // Verify aggregated results
        let sum_column = result_batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let count_column = result_batch.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        let avg_column = result_batch.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        let min_column = result_batch.column(6).as_any().downcast_ref::<Float64Array>().unwrap();
        let max_column = result_batch.column(7).as_any().downcast_ref::<Float64Array>().unwrap();

        // Row 0: Regular entry at 3500
        // Window is [1500, 3500] (RANGE 2000ms PRECEDING from 3500)
        // Entries in window: 2000, 3000 (from state) + virtual 3500 -> SUM=85.0, COUNT=3, AVG=28.33, MIN=20.0, MAX=35.0
        assert_eq!(sum_column.value(0), 85.0, "SUM at 3500 should be 85.0 (20.0+30.0+35.0)");
        assert_eq!(count_column.value(0), 3, "COUNT at 3500 should be 3");
        assert!((avg_column.value(0) - 28.333333333333332).abs() < 0.001, "AVG at 3500 should be ~28.33");
        assert_eq!(min_column.value(0), 20.0, "MIN at 3500 should be 20.0");
        assert_eq!(max_column.value(0), 35.0, "MAX at 3500 should be 35.0");

        // Row 1: Late entry at 1500
        // Window includes [1000, 1500] -> SUM=25.0, COUNT=2, AVG=12.5, MIN=10.0, MAX=15.0
        assert_eq!(sum_column.value(1), 25.0, "SUM at 1500 (late) should be 25.0 (10.0+15.0)");
        assert_eq!(count_column.value(1), 2, "COUNT at 1500 (late) should be 2");
        assert_eq!(avg_column.value(1), 12.5, "AVG at 1500 (late) should be 12.5");
        assert_eq!(min_column.value(1), 10.0, "MIN at 1500 (late) should be 10.0");
        assert_eq!(max_column.value(1), 15.0, "MAX at 1500 (late) should be 15.0");

        // Step 2: Add another batch to window operator to slide time
        let batch3 = create_test_batch(vec![5000], vec![50.0], vec!["A"]);
        let message3 = create_keyed_message(batch3, "A");

        let input_stream2 = Box::pin(stream::iter(vec![message3]));
        window_operator.set_input(Some(input_stream2));

        // Process message to slide window state
        window_operator.poll_next().await;

        // Step 3: Make another request with multiple entries
        // - Regular entry: 5500 (on time, > latest entry 5000, within window)
        // - Late entry: 3500 (late but within lateness tolerance)
        // - Too late entry: 500 (too late, should be dropped)
        let request_batch2 = create_test_batch(vec![5500, 3500, 500], vec![55.0, 35.0, 5.0], vec!["A", "A", "A"]);
        let request_message2 = create_keyed_message(request_batch2, "A");

        let request_stream2 = Box::pin(stream::iter(vec![request_message2]));
        request_operator.set_input(Some(request_stream2));

        // Process second request
        let result2 = request_operator.poll_next().await;
        let result_message2 = result2.get_result_message();
        let result_batch2 = result_message2.record_batch();

        // Too late entry at 500 should be dropped, so we expect 2 rows (4500 and 3500)
        assert_eq!(result_batch2.num_rows(), 2, "Should have 2 result rows (too late entry at 500 should be dropped)");
        assert_eq!(result_batch2.num_columns(), 8, "Should have 8 columns");

        let sum_column2 = result_batch2.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let count_column2 = result_batch2.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        let avg_column2 = result_batch2.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        let min_column2 = result_batch2.column(6).as_any().downcast_ref::<Float64Array>().unwrap();
        let max_column2 = result_batch2.column(7).as_any().downcast_ref::<Float64Array>().unwrap();

        // Row 0: Regular entry at 5500
        // Window is [3500, 5500] (RANGE 2000ms PRECEDING from 5500)
        // Entries in window: 5000 (from state) + virtual 5500 (3500 is virtual but only for its own row, not for 5500)
        // -> SUM=105.0, COUNT=2, AVG=52.5, MIN=50.0, MAX=55.0
        assert_eq!(sum_column2.value(0), 105.0, "SUM at 5500 should be 105.0 (50.0+55.0)");
        assert_eq!(count_column2.value(0), 2, "COUNT at 5500 should be 2");
        assert_eq!(avg_column2.value(0), 52.5, "AVG at 5500 should be 52.5");
        assert_eq!(min_column2.value(0), 50.0, "MIN at 5500 should be 50.0");
        assert_eq!(max_column2.value(0), 55.0, "MAX at 5500 should be 55.0");

        // Row 1: Late entry at 3500
        // Window is [1500, 3500] (RANGE 2000ms PRECEDING)
        // Entries in window: 2000, 3000 (from state) + virtual 3500 -> SUM=85.0, COUNT=3, AVG=28.33, MIN=20.0, MAX=35.0
        assert_eq!(sum_column2.value(1), 85.0, "SUM at 3500 (late) should be 85.0 (20.0+30.0+35.0)");
        assert_eq!(count_column2.value(1), 3, "COUNT at 3500 (late) should be 3");
        assert!((avg_column2.value(1) - 28.333333333333332).abs() < 0.001, "AVG at 3500 (late) should be ~28.33");
        assert_eq!(min_column2.value(1), 20.0, "MIN at 3500 (late) should be 20.0");
        assert_eq!(max_column2.value(1), 35.0, "MAX at 3500 (late) should be 35.0");

        // Step 4: Test exclude_current_row functionality
        // Set exclude_current_row to true - request entries should not be included in aggregates
        for window_config in request_operator.window_configs.values_mut() {
            window_config.exclude_current_row = Some(true);
        }

        // Make a new request without updating window operator state
        // Latest entry in state is 5000, lateness tolerance is 2000ms
        // - Regular entry: 6000 (on time, > latest entry 5000, exclude_current_row=true so 6000 should not be included)
        // - Late entry: 3500 (late but within lateness tolerance: 5000 - 2000 = 3000, so >= 3000 is acceptable)
        let request_batch3 = create_test_batch(vec![6000, 3500], vec![60.0, 35.0], vec!["A", "A"]);
        let request_message3 = create_keyed_message(request_batch3, "A");

        let request_stream3 = Box::pin(stream::iter(vec![request_message3]));
        request_operator.set_input(Some(request_stream3));

        // Process third request
        let result3 = request_operator.poll_next().await;
        let result_message3 = result3.get_result_message();
        let result_batch3 = result_message3.record_batch();

        assert_eq!(result_batch3.num_rows(), 2, "Should have 2 result rows");
        assert_eq!(result_batch3.num_columns(), 8, "Should have 8 columns");

        let sum_column3 = result_batch3.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let count_column3 = result_batch3.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        let avg_column3 = result_batch3.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        let min_column3 = result_batch3.column(6).as_any().downcast_ref::<Float64Array>().unwrap();
        let max_column3 = result_batch3.column(7).as_any().downcast_ref::<Float64Array>().unwrap();

        // Row 0: Regular entry at 6000 with exclude_current_row=true
        // Window is [4000, 6000] (RANGE 2000ms PRECEDING from 6000)
        // Entries in window from state: 5000 (3000 is outside window since 3000 < 4000)
        // Virtual 6000 is EXCLUDED due to exclude_current_row=true
        // -> SUM=50.0, COUNT=1, AVG=50.0, MIN=50.0, MAX=50.0
        assert_eq!(sum_column3.value(0), 50.0, "SUM at 6000 (exclude_current_row=true) should be 50.0 (excluding 60.0)");
        assert_eq!(count_column3.value(0), 1, "COUNT at 6000 should be 1 (excluding current row)");
        assert_eq!(avg_column3.value(0), 50.0, "AVG at 6000 should be 50.0");
        assert_eq!(min_column3.value(0), 50.0, "MIN at 6000 should be 50.0");
        assert_eq!(max_column3.value(0), 50.0, "MAX at 6000 should be 50.0");

        // Row 1: Late entry at 3500 with exclude_current_row=true
        // Window is [1500, 3500] (RANGE 2000ms PRECEDING from 3500)
        // Entries in window from state: 2000, 3000 (5000 is outside window since 5000 > 3500)
        // Virtual 3500 is EXCLUDED due to exclude_current_row=true
        // -> SUM=50.0, COUNT=2, AVG=25.0, MIN=20.0, MAX=30.0
        assert_eq!(sum_column3.value(1), 50.0, "SUM at 3500 (late, exclude_current_row=true) should be 50.0 (20.0+30.0, excluding 35.0)");
        assert_eq!(count_column3.value(1), 2, "COUNT at 3500 should be 2 (excluding current row)");
        assert_eq!(avg_column3.value(1), 25.0, "AVG at 3500 should be 25.0");
        assert_eq!(min_column3.value(1), 20.0, "MIN at 3500 should be 20.0");
        assert_eq!(max_column3.value(1), 30.0, "MAX at 3500 should be 30.0");
    }
}
