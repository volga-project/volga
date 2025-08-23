use std::sync::Arc;
use std::ops::Range;
use arrow::record_batch::RecordBatch;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::SchemaRef;
use datafusion::common::ScalarValue;
use datafusion::physical_plan::{WindowExpr, expressions::Column};
use datafusion::physical_expr::window::SlidingAggregateWindowExpr;

use crate::runtime::operators::window::state::keyed_state::WindowState;

#[derive(Debug)]
pub struct LocalWindowsState {
    states: Vec<WindowState>,
    data: RecordBatch,
    time_column_idx: usize
}

impl LocalWindowsState {
    pub fn create(window_exprs: Vec<Arc<dyn WindowExpr>>, schema: SchemaRef) -> Self {
        let mut states = Vec::new();

        // TODO assert all windows have same orderbys
        let order_by_exprs = window_exprs[0].order_by().to_vec();

        for window_expr in window_exprs {
            // Cast to AggregateWindowExpr to get accumulator
            let aggregate_expr = window_expr.as_any()
                .downcast_ref::<SlidingAggregateWindowExpr>()
                .expect("Only SlidingAggregateWindowExpr is supported");
            
            let accumulator = aggregate_expr.get_aggregate_expr().create_sliding_accumulator()
                .expect("Should be able to create accumulator");

            if !accumulator.supports_retract_batch() {
                panic!("Accumulator {:?} does not support retract batch", accumulator);
            }
            
            let window_state = WindowState::new(accumulator, window_expr, 0);
            states.push(window_state);
        }
        
        // Extract time_column_idx from the first ORDER BY expression
        let time_column_idx = order_by_exprs.first()
            .expect("order bys should not be empty").expr.as_any().downcast_ref::<Column>()
            .expect("ok")
            .index();
        
        let data = RecordBatch::new_empty(Arc::clone(&schema));

        Self {
            states,
            data,
            time_column_idx
        }
    }

    pub fn update_batch(&mut self, batch: &RecordBatch) -> Vec<Vec<ScalarValue>> {
        self.data = arrow::compute::concat_batches(&self.data.schema(), [&self.data, &batch])
            .expect("Should be able to concat batches");
        
        let mut all_results = Vec::new();
        
        // Process each state individually to avoid borrow checker issues
        for i in 0..self.states.len() {
            let to_update: Vec<Arc<dyn Array>> = self.states[i].expr.evaluate_args(batch)
                .expect("Should be able to evaluate window args");
            let window_results = self.update_window_at_index(i, to_update);
            all_results.push(window_results);
        }

        self.prune();
        all_results
    }

    fn update_window_at_index(&mut self, state_index: usize, to_update: Vec<Arc<dyn Array>>) -> Vec<ScalarValue> {
        // Update accumulator row by row, adding new data and retracting old data, while recording results
        let num_rows = to_update.first().expect("Expected at least one value").len();
        let mut results = Vec::new();

        let mut last_retract_end = self.states[state_index].start;
        for row_idx in 0..num_rows {
            // Extract single element from each array using slice
            let row_arrays: Vec<ArrayRef> = to_update
                .iter()
                .map(|array| array.slice(row_idx, 1))
                .collect();
            
            println!("Updating accumulator with row_arrays: {:?}", row_arrays);
            self.states[state_index].accumulator.update_batch(&row_arrays)
                .expect("Should be able to update accumulator");

            // find values to retract in self.data
            let window_time_length = 0; // TODO use window expr window frame to get window time length
            let retract_range = self.find_retract_range(row_idx, last_retract_end, window_time_length);
            println!("Retract range: {:?}", retract_range);
            last_retract_end = retract_range.end;

            // extract values to retract by slicing self.data
            let retract_batch = self.data.slice(retract_range.start, retract_range.end - retract_range.start);

            // retract from accumulator
            let retract_values = self.states[state_index].expr.evaluate_args(&retract_batch)
                .expect("Should be able to evaluate retract args");

            println!("Retracting from accumulator with retract_values: {:?}", retract_values);
            self.states[state_index].accumulator.retract_batch(&retract_values)
                .expect("Should be able to retract from accumulator");
            let result = self.states[state_index].accumulator.evaluate()
                .expect("Should be able to evaluate accumulator");
            println!("Result: {:?}", result);
            results.push(result);
        }

        self.states[state_index].start = last_retract_end;
        results
    }

    fn prune(&mut self) {
        // Find minimum start position across all window states
        let min_start = self.states.iter().map(|state| state.start).min().unwrap_or(0);
        
        if min_start > 0 && min_start < self.data.num_rows() {
            // Prune the data by removing rows before min_start
            self.data = self.data.slice(min_start, self.data.num_rows() - min_start);
            
            // Update all window state start positions after pruning
            for state in &mut self.states {
                state.start = state.start.saturating_sub(min_start);
            }
        }
    }

    fn find_retract_range(&self, row_idx: usize, last_retract_end: usize, window_time_length: i64) -> Range<usize> {
        // TODO this is for time based window with RANGE type, also handle ROWS and other interval types
        let time_column = self.data.column(self.time_column_idx);
        let time_val = ScalarValue::try_from_array(time_column, row_idx)
            .expect("Should be able to extract time value from array");
        let time_val = self.extract_timestamp_value(&time_val).expect("Should be able to extract time value");
        
        let start = last_retract_end;
        let mut end = start;
        
        // Find the end of retract range using while loop
        while end < self.data.num_rows() {
            let cur_time = ScalarValue::try_from_array(time_column, end)
                .expect("Should be able to extract current time value");
            let cur_time = self.extract_timestamp_value(&cur_time).expect("Should be able to extract time value");
            
            if cur_time < time_val.saturating_sub(window_time_length) {
                end += 1;
            } else {
                break;
            }
        }
        
        Range { start, end }
    }

    /// Extract timestamp value from ScalarValue
    /// TODO check datafusion time utils for this
    fn extract_timestamp_value(&self, scalar: &ScalarValue) -> Option<i64> {
        match scalar {
            ScalarValue::TimestampSecond(Some(val), _) => Some(*val),
            ScalarValue::TimestampMillisecond(Some(val), _) => Some(*val),
            ScalarValue::TimestampMicrosecond(Some(val), _) => Some(*val),
            ScalarValue::TimestampNanosecond(Some(val), _) => Some(*val),
            ScalarValue::Int64(Some(val)) => Some(*val),
            ScalarValue::UInt64(Some(val)) => Some(*val as i64),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::planner::{Planner, PlanningContext};
    use datafusion::prelude::SessionContext;
    use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
    use crate::runtime::operators::operator::OperatorConfig;
    use arrow::array::{Int64Array, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn create_test_batch(timestamps: Vec<i64>, values: Vec<i64>) -> RecordBatch {
        let schema = create_test_schema();
        let timestamp_array = Arc::new(TimestampMillisecondArray::from(timestamps));
        let value_array = Arc::new(Int64Array::from(values));
        
        RecordBatch::try_new(schema, vec![timestamp_array, value_array])
            .expect("Should be able to create test batch")
    }

    async fn extract_window_expr_from_sql(sql: &str) -> Vec<Arc<dyn WindowExpr>> {
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
                return config.window_exec.window_expr().to_vec();
            }
        }
        
        panic!("No window operator found in SQL: {}", sql);
    }

    #[tokio::test]
    async fn test_sum_sliding_window() {
        let sql = "SELECT timestamp, SUM(value) OVER (ORDER BY timestamp RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW) as sum_value FROM test_table";
        let window_exprs = extract_window_expr_from_sql(sql).await;
        let schema = create_test_schema();
        
        let mut state = LocalWindowsState::create(window_exprs, schema);
        
        // Test data with timestamps and values
        // Window size is 2000ms, so we expect sliding behavior
        let test_cases = vec![
            (vec![1000], vec![10], vec![ScalarValue::Int64(Some(10))]),     // t=1000, value=10 -> sum=10
            (vec![1500], vec![20], vec![ScalarValue::Int64(Some(30))]),     // t=1500, value=20 -> sum=30 (10+20)
            (vec![2000], vec![30], vec![ScalarValue::Int64(Some(60))]),     // t=2000, value=30 -> sum=60 (10+20+30)
            (vec![3500], vec![40], vec![ScalarValue::Int64(Some(90))]),     // t=3500, value=40 -> sum=90 (20+30+40, 10 excluded)
            (vec![4000], vec![50], vec![ScalarValue::Int64(Some(120))]),    // t=4000, value=50 -> sum=120 (30+40+50, 10+20 excluded)
        ];
        
        for (timestamps, values, expected_results) in test_cases {
            let batch = create_test_batch(timestamps, values);
            let results = state.update_batch(&batch);
            
            // Should have 1 window (SUM) and 1 result per input row
            assert_eq!(results.len(), 1, "Should have 1 window result");
            assert_eq!(results[0].len(), 1, "Should have 1 result for single input row");
            
            // Verify the sum result
            assert_eq!(results[0][0], expected_results[0], "Sum result should match expected");
        }
        
        // Verify the sliding window maintained proper state
        assert_eq!(state.data.num_rows(), 5, "Should have 5 total rows");
        assert_eq!(state.states.len(), 1, "Should have 1 window state");
    }

    #[tokio::test]
    async fn test_count_sliding_window() {
        let sql = "SELECT timestamp, COUNT(*) OVER (ORDER BY timestamp RANGE BETWEEN INTERVAL '1500' MILLISECOND PRECEDING AND CURRENT ROW) as count_value FROM test_table";
        let window_exprs = extract_window_expr_from_sql(sql).await;
        let schema = create_test_schema();
        
        let mut state = LocalWindowsState::create(window_exprs, schema);
        
        // Test sliding count with 1500ms window
        let test_cases = vec![
            (vec![1000], vec![1], vec![ScalarValue::Int64(Some(1))]),      // t=1000 -> count=1
            (vec![2000], vec![2], vec![ScalarValue::Int64(Some(1))]),      // t=2000 -> count=1 (only 2000, 1000 excluded by 1500ms window)
            (vec![2200], vec![3], vec![ScalarValue::Int64(Some(2))]),      // t=2200 -> count=2 (2000,2200 within 1500ms)
            (vec![4000], vec![4], vec![ScalarValue::Int64(Some(1))]),      // t=4000 -> count=1 (only 4000 within 1500ms from 4000)
        ];
        
        for (timestamps, values, expected_results) in test_cases {
            let batch = create_test_batch(timestamps, values);
            let results = state.update_batch(&batch);
            
            // Should have 1 window (COUNT) and 1 result per input row
            assert_eq!(results.len(), 1, "Should have 1 window result");
            assert_eq!(results[0].len(), 1, "Should have 1 result for single input row");
            
            // Verify the count result
            assert_eq!(results[0][0], expected_results[0], "Count result should match expected");
        }
        
        assert_eq!(state.data.num_rows(), 4, "Should have 4 total rows");
    }

    #[tokio::test]
    async fn test_avg_sliding_window() {
        let sql = "SELECT timestamp, AVG(value) OVER (ORDER BY timestamp RANGE BETWEEN INTERVAL '3000' MILLISECOND PRECEDING AND CURRENT ROW) as avg_value FROM test_table";
        let window_exprs = extract_window_expr_from_sql(sql).await;
        let schema = create_test_schema();
        
        let mut state = LocalWindowsState::create(window_exprs, schema);
        
        // Test sliding average with 3000ms window
        let batch1 = create_test_batch(vec![1000, 2000, 3000], vec![10, 20, 30]);
        let results1 = state.update_batch(&batch1);
        
        // Verify results for 3-row batch
        assert_eq!(results1.len(), 1, "Should have 1 window result");
        assert_eq!(results1[0].len(), 3, "Should have 3 results for 3 input rows");
        
        // Expected averages: 10.0, 15.0 (10+20)/2, 20.0 (10+20+30)/3
        let expected_avg1 = ScalarValue::Float64(Some(10.0));
        let expected_avg2 = ScalarValue::Float64(Some(15.0));
        let expected_avg3 = ScalarValue::Float64(Some(20.0));
        
        assert_eq!(results1[0][0], expected_avg1, "First average should be 10.0");
        assert_eq!(results1[0][1], expected_avg2, "Second average should be 15.0");
        assert_eq!(results1[0][2], expected_avg3, "Third average should be 20.0");
        
        let batch2 = create_test_batch(vec![4500], vec![60]);  // Should exclude 1000ms data
        let results2 = state.update_batch(&batch2);
        
        // Should have average of (20+30+60)/3 = 36.67
        assert_eq!(results2.len(), 1, "Should have 1 window result");
        assert_eq!(results2[0].len(), 1, "Should have 1 result for single input row");
        
        assert_eq!(state.data.num_rows(), 4, "Should have 4 total rows");
        
        // Test pruning behavior
        let batch3 = create_test_batch(vec![6000], vec![100]); // Should trigger pruning
        let results3 = state.update_batch(&batch3);
        
        // Verify we get a result
        assert_eq!(results3.len(), 1, "Should have 1 window result");
        assert_eq!(results3[0].len(), 1, "Should have 1 result for single input row");
        
        // After pruning, older data should be removed
        assert!(state.data.num_rows() <= 5, "Pruning should limit data growth");
    }

    #[tokio::test]
    async fn test_min_max_sliding_window() {
        let sql = "SELECT timestamp, MIN(value) OVER (ORDER BY timestamp RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW) as min_value, MAX(value) OVER (ORDER BY timestamp RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW) as max_value FROM test_table";
        let window_exprs = extract_window_expr_from_sql(sql).await;
        let schema = create_test_schema();
        
        let mut state = LocalWindowsState::create(window_exprs, schema);
        
        // Test with values that will test min/max behavior
        let batch = create_test_batch(vec![1000, 1500, 2500, 3000], vec![50, 10, 80, 20]);
        let results = state.update_batch(&batch);
        
        // Should have 2 windows (MIN and MAX) and 4 results each (one per input row)
        assert_eq!(results.len(), 2, "Should have 2 window results (MIN and MAX)");
        assert_eq!(results[0].len(), 4, "Should have 4 results for MIN window");
        assert_eq!(results[1].len(), 4, "Should have 4 results for MAX window");
        
        // Verify MIN results: 50, 10, 10, 10 (within 2000ms window)
        assert_eq!(results[0][0], ScalarValue::Int64(Some(50)), "MIN at t=1000 should be 50");
        assert_eq!(results[0][1], ScalarValue::Int64(Some(10)), "MIN at t=1500 should be 10");
        assert_eq!(results[0][2], ScalarValue::Int64(Some(10)), "MIN at t=2500 should be 10 (1500,2500 in window)");
        assert_eq!(results[0][3], ScalarValue::Int64(Some(20)), "MIN at t=3000 should be 20 (2500,3000 in window)");
        
        // Verify MAX results: 50, 50, 80, 80
        assert_eq!(results[1][0], ScalarValue::Int64(Some(50)), "MAX at t=1000 should be 50");
        assert_eq!(results[1][1], ScalarValue::Int64(Some(50)), "MAX at t=1500 should be 50");
        assert_eq!(results[1][2], ScalarValue::Int64(Some(80)), "MAX at t=2500 should be 80");
        assert_eq!(results[1][3], ScalarValue::Int64(Some(80)), "MAX at t=3000 should be 80");
        
        assert_eq!(state.data.num_rows(), 4, "Should have 4 total rows");
        assert_eq!(state.states.len(), 2, "Should have 2 window states (MIN and MAX)");
    }

    #[tokio::test]
    async fn test_edge_case_empty_window() {
        let sql = "SELECT timestamp, SUM(value) OVER (ORDER BY timestamp RANGE BETWEEN INTERVAL '100' MILLISECOND PRECEDING AND CURRENT ROW) as sum_value FROM test_table";
        let window_exprs = extract_window_expr_from_sql(sql).await;
        let schema = create_test_schema();
        
        let mut state = LocalWindowsState::create(window_exprs, schema);
        
        // Test with sparse timestamps that don't overlap in small window
        let batch1 = create_test_batch(vec![1000], vec![10]);
        let results1 = state.update_batch(&batch1);
        assert_eq!(results1[0][0], ScalarValue::Int64(Some(10)), "First sum should be 10");
        
        let batch2 = create_test_batch(vec![2000], vec![20]); // 1000ms gap, outside 100ms window
        let results2 = state.update_batch(&batch2);
        assert_eq!(results2[0][0], ScalarValue::Int64(Some(20)), "Second sum should be 20 (no overlap)");
        
        assert_eq!(state.data.num_rows(), 2, "Should have 2 total rows");
    }

    #[tokio::test]
    async fn test_edge_case_identical_timestamps() {
        let sql = "SELECT timestamp, COUNT(*) OVER (ORDER BY timestamp RANGE BETWEEN INTERVAL '0' MILLISECOND PRECEDING AND CURRENT ROW) as count_value FROM test_table";
        let window_exprs = extract_window_expr_from_sql(sql).await;
        let schema = create_test_schema();
        
        let mut state = LocalWindowsState::create(window_exprs, schema);
        
        // Test with identical timestamps
        let batch = create_test_batch(vec![1000, 1000, 1000], vec![10, 20, 30]);
        let results = state.update_batch(&batch);
        
        // All rows have same timestamp, so each should include all previous rows with same timestamp
        assert_eq!(results[0][0], ScalarValue::Int64(Some(1)), "First count should be 1");
        assert_eq!(results[0][1], ScalarValue::Int64(Some(2)), "Second count should be 2");
        assert_eq!(results[0][2], ScalarValue::Int64(Some(3)), "Third count should be 3");
        
        assert_eq!(state.data.num_rows(), 3, "Should have 3 total rows");
    }

    #[tokio::test] 
    async fn test_large_window_no_retraction() {
        let sql = "SELECT timestamp, SUM(value) OVER (ORDER BY timestamp RANGE BETWEEN INTERVAL '10000' MILLISECOND PRECEDING AND CURRENT ROW) as sum_value FROM test_table";
        let window_exprs = extract_window_expr_from_sql(sql).await;
        let schema = create_test_schema();
        
        let mut state = LocalWindowsState::create(window_exprs, schema);
        
        // Test with large window where no retraction should occur
        let batch = create_test_batch(vec![1000, 2000, 3000, 4000], vec![10, 20, 30, 40]);
        let results = state.update_batch(&batch);
        
        // Verify cumulative sums: 10, 30, 60, 100
        assert_eq!(results[0][0], ScalarValue::Int64(Some(10)), "First sum should be 10");
        assert_eq!(results[0][1], ScalarValue::Int64(Some(30)), "Second sum should be 30");
        assert_eq!(results[0][2], ScalarValue::Int64(Some(60)), "Third sum should be 60");
        assert_eq!(results[0][3], ScalarValue::Int64(Some(100)), "Fourth sum should be 100");
        
        // All data should be retained since window is large
        assert_eq!(state.data.num_rows(), 4, "Should have 4 total rows");
        // Start positions should remain 0 since no retraction
        for window_state in &state.states {
            assert_eq!(window_state.start, 0, "Start should remain 0 with large window");
        }
    }

    #[tokio::test]
    async fn test_multiple_windows_multiple_aggregates() {
        // Test query with multiple different aggregation functions over different window sizes
        let sql = "SELECT timestamp, 
                   SUM(value) OVER (ORDER BY timestamp RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW) as sum_1s,
                   COUNT(*) OVER (ORDER BY timestamp RANGE BETWEEN INTERVAL '1500' MILLISECOND PRECEDING AND CURRENT ROW) as count_1_5s,
                   AVG(value) OVER (ORDER BY timestamp RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW) as avg_2s,
                   MIN(value) OVER (ORDER BY timestamp RANGE BETWEEN INTERVAL '2500' MILLISECOND PRECEDING AND CURRENT ROW) as min_2_5s,
                   MAX(value) OVER (ORDER BY timestamp RANGE BETWEEN INTERVAL '3000' MILLISECOND PRECEDING AND CURRENT ROW) as max_3s
                   FROM test_table";
        let window_exprs = extract_window_expr_from_sql(sql).await;
        let schema = create_test_schema();
        
        let mut state = LocalWindowsState::create(window_exprs, schema);
        
        // Should have 5 different window states for 5 different aggregations
        assert_eq!(state.states.len(), 5, "Should have 5 window states for 5 different aggregations");
        
        // Test data: timestamps with 500ms intervals, values: 100, 50, 200, 25, 150
        let batch = create_test_batch(
            vec![1000, 1500, 2000, 2500, 3000], 
            vec![100, 50, 200, 25, 150]
        );
        let results = state.update_batch(&batch);
        
        // Should have results for all 5 windows, each with 5 results (one per input row)
        assert_eq!(results.len(), 5, "Should have 5 window results");
        for (i, window_result) in results.iter().enumerate() {
            assert_eq!(window_result.len(), 5, "Window {} should have 5 results", i);
        }
        
        // Verify SUM results (1000ms window)
        // t=1000: sum=100, t=1500: sum=150 (100+50), t=2000: sum=250 (50+200), t=2500: sum=225 (200+25), t=3000: sum=175 (25+150)
        assert_eq!(results[0][0], ScalarValue::Int64(Some(100)), "SUM at t=1000 should be 100");
        assert_eq!(results[0][1], ScalarValue::Int64(Some(150)), "SUM at t=1500 should be 150");
        assert_eq!(results[0][2], ScalarValue::Int64(Some(250)), "SUM at t=2000 should be 250");
        assert_eq!(results[0][3], ScalarValue::Int64(Some(225)), "SUM at t=2500 should be 225");
        assert_eq!(results[0][4], ScalarValue::Int64(Some(175)), "SUM at t=3000 should be 175");
        
        // Verify COUNT results (1500ms window)
        // t=1000: count=1, t=1500: count=2, t=2000: count=2 (1500,2000), t=2500: count=2 (2000,2500), t=3000: count=2 (2500,3000)
        assert_eq!(results[1][0], ScalarValue::Int64(Some(1)), "COUNT at t=1000 should be 1");
        assert_eq!(results[1][1], ScalarValue::Int64(Some(2)), "COUNT at t=1500 should be 2");
        assert_eq!(results[1][2], ScalarValue::Int64(Some(2)), "COUNT at t=2000 should be 2");
        assert_eq!(results[1][3], ScalarValue::Int64(Some(2)), "COUNT at t=2500 should be 2");
        assert_eq!(results[1][4], ScalarValue::Int64(Some(2)), "COUNT at t=3000 should be 2");
        
        // Verify AVG results (2000ms window)
        // t=1000: avg=100, t=1500: avg=75 (100+50)/2, t=2000: avg=116.67 (100+50+200)/3, t=2500: avg=91.67 (50+200+25)/3, t=3000: avg=125 (200+25+150)/3
        assert_eq!(results[2][0], ScalarValue::Float64(Some(100.0)), "AVG at t=1000 should be 100.0");
        assert_eq!(results[2][1], ScalarValue::Float64(Some(75.0)), "AVG at t=1500 should be 75.0");
        let expected_avg_2000 = (100.0 + 50.0 + 200.0) / 3.0;
        assert_eq!(results[2][2], ScalarValue::Float64(Some(expected_avg_2000)), "AVG at t=2000 should be ~116.67");
        let expected_avg_2500 = (50.0 + 200.0 + 25.0) / 3.0;
        assert_eq!(results[2][3], ScalarValue::Float64(Some(expected_avg_2500)), "AVG at t=2500 should be ~91.67");
        let expected_avg_3000 = (200.0 + 25.0 + 150.0) / 3.0;
        assert_eq!(results[2][4], ScalarValue::Float64(Some(expected_avg_3000)), "AVG at t=3000 should be 125.0");
        
        // Verify MIN results (2500ms window)
        // t=1000: min=100, t=1500: min=50, t=2000: min=50, t=2500: min=25, t=3000: min=25
        assert_eq!(results[3][0], ScalarValue::Int64(Some(100)), "MIN at t=1000 should be 100");
        assert_eq!(results[3][1], ScalarValue::Int64(Some(50)), "MIN at t=1500 should be 50");
        assert_eq!(results[3][2], ScalarValue::Int64(Some(50)), "MIN at t=2000 should be 50");
        assert_eq!(results[3][3], ScalarValue::Int64(Some(25)), "MIN at t=2500 should be 25");
        assert_eq!(results[3][4], ScalarValue::Int64(Some(25)), "MIN at t=3000 should be 25");
        
        // Verify MAX results (3000ms window)
        // t=1000: max=100, t=1500: max=100, t=2000: max=200, t=2500: max=200, t=3000: max=200
        assert_eq!(results[4][0], ScalarValue::Int64(Some(100)), "MAX at t=1000 should be 100");
        assert_eq!(results[4][1], ScalarValue::Int64(Some(100)), "MAX at t=1500 should be 100");
        assert_eq!(results[4][2], ScalarValue::Int64(Some(200)), "MAX at t=2000 should be 200");
        assert_eq!(results[4][3], ScalarValue::Int64(Some(200)), "MAX at t=2500 should be 200");
        assert_eq!(results[4][4], ScalarValue::Int64(Some(200)), "MAX at t=3000 should be 200");
        
        // Verify data state
        assert_eq!(state.data.num_rows(), 5, "Should have 5 total rows");
        
        // Test with additional batch to verify sliding behavior across all windows
        let batch2 = create_test_batch(vec![4000], vec![300]);
        let results2 = state.update_batch(&batch2);
        
        // Each window should produce 1 result for the new row
        for (i, window_result) in results2.iter().enumerate() {
            assert_eq!(window_result.len(), 1, "Window {} should have 1 result for single input row", i);
        }
        
        // Verify that different windows have different retraction behavior
        // SUM (1000ms): should only include value 300 = 300
        assert_eq!(results2[0][0], ScalarValue::Int64(Some(300)), "SUM at t=4000 should be 300 (only current)");
        
        // COUNT (1500ms): should only count current row = 1
        assert_eq!(results2[1][0], ScalarValue::Int64(Some(1)), "COUNT at t=4000 should be 1");
        
        // AVG (2000ms): should only include value 300 = 300.0
        assert_eq!(results2[2][0], ScalarValue::Float64(Some(300.0)), "AVG at t=4000 should be 300.0");
        
        // MIN (2500ms): should include 150 and 300, min = 150
        assert_eq!(results2[3][0], ScalarValue::Int64(Some(150)), "MIN at t=4000 should be 150");
        
        // MAX (3000ms): should include 150 and 300, max = 300
        assert_eq!(results2[4][0], ScalarValue::Int64(Some(300)), "MAX at t=4000 should be 300");
    }
}