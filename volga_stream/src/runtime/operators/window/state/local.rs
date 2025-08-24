use std::sync::Arc;
use std::ops::Range;
use arrow::record_batch::RecordBatch;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::SchemaRef;
use arrow::compute::SortOptions;
use datafusion::common::{ScalarValue, utils::compare_rows};
use datafusion::physical_plan::{WindowExpr, expressions::Column};
use datafusion::physical_expr::window::SlidingAggregateWindowExpr;
use datafusion::logical_expr::{WindowFrame, WindowFrameBound};

use crate::runtime::operators::window::state::keyed_state::WindowState;

#[derive(Debug)]
pub struct LocalWindowsState {
    states: Vec<WindowState>,
    data: RecordBatch,
}

impl LocalWindowsState {
    pub fn create(window_exprs: Vec<Arc<dyn WindowExpr>>, schema: SchemaRef) -> Self {
        let mut states = Vec::new();

        // TODO assert all windows have same orderbys

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
        
        let data = RecordBatch::new_empty(Arc::clone(&schema));

        Self {
            states,
            data,
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

            // TODO check if we need retraction - check UNBOUNDED PRECEDING case

            // find values to retract in self.data using window frame logic
            let current_row_idx = self.data.num_rows() - num_rows + row_idx;
            let retract_range = self.find_retract_range(&self.states[state_index].expr, current_row_idx, last_retract_end);
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

    fn find_retract_range(&self, window_expr: &Arc<dyn WindowExpr>, current_row_idx: usize, last_retract_end: usize) -> Range<usize> {
        // Get window frame and ORDER BY information from the window expression
        // let window_expr = &self.states[state_index].expr;
        let window_frame = window_expr.get_window_frame();
        let sort_options: Vec<SortOptions> = window_expr.order_by().iter().map(|o| o.options).collect();
        
        // Get current row's ORDER BY values efficiently
        let current_row_values = self.get_order_by_values_at_row(window_expr, current_row_idx);
        
        // Calculate target values for window start boundary
        let target_values = self.calculate_window_start_target(&current_row_values, window_frame);
        
        let start = last_retract_end;
        let mut end = start;
        
        // Find rows that should be retracted using DataFusion's compare_rows
        // We retract rows that are before the window start boundary
        while end < self.data.num_rows() {
            let row_values = self.get_order_by_values_at_row(window_expr, end);
            
            // Compare current row with window start boundary
            // If current row < window_start, it should be retracted
            let comparison = compare_rows(&row_values, &target_values, &sort_options)
                .expect("Should be able to compare rows");
            
            if comparison.is_lt() {
                end += 1;  // This row should be retracted
            } else {
                break;     // We've reached the window boundary
            }
        }
        
        Range { start, end }
    }
    
    /// Get ORDER BY values for a specific row without copying entire columns
    fn get_order_by_values_at_row(&self, window_expr: &Arc<dyn WindowExpr>, row_idx: usize) -> Vec<ScalarValue> {
        window_expr.order_by()
            .iter()
            .map(|sort_expr| {
                if let Some(column) = sort_expr.expr.as_any().downcast_ref::<Column>() {
                    let column_array = self.data.column(column.index());
                    ScalarValue::try_from_array(column_array, row_idx)
                        .expect("Should be able to extract scalar value from array")
                } else {
                    panic!("Expected Column expression in ORDER BY");
                }
            })
            .collect()
    }
    
    /// Calculate target values for window start boundary using DataFusion's interval arithmetic
    fn calculate_window_start_target(&self, current_values: &[ScalarValue], window_frame: &WindowFrame) -> Vec<ScalarValue> {
        match &window_frame.start_bound {
            WindowFrameBound::Preceding(delta) => {
                if window_frame.start_bound.is_unbounded() || delta.is_null() {
                    panic!("Can not retract UNBOUNDED PRECEDING");
                }
                // Calculate current_value - delta for PRECEDING
                current_values.iter().map(|value| {
                    if value.is_null() {
                        value.clone()
                    } else {
                        value.sub(delta).expect("Should be able to subtract delta from value")
                    }
                }).collect()
            },
            WindowFrameBound::CurrentRow => {
                // Window starts at current row
                current_values.to_vec()
            },
            WindowFrameBound::Following(_) => {
                panic!("Following bound is not supported");
            }
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

    // TODO use single window definition
    #[tokio::test]
    async fn test_sliding_time_based_windows() {
        // Single window definition with multiple aggregates and various update patterns
        let sql = "SELECT 
            timestamp,
            SUM(value) OVER (ORDER BY timestamp RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW) as sum_val,
            COUNT(value) OVER (ORDER BY timestamp RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW) as count_val,
            AVG(value) OVER (ORDER BY timestamp RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW) as avg_val,
            MIN(value) OVER (ORDER BY timestamp RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW) as min_val,
            MAX(value) OVER (ORDER BY timestamp RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW) as max_val
        FROM test_table";
        
        let window_exprs = extract_window_expr_from_sql(sql).await;
        let schema = create_test_schema();
        let mut state = LocalWindowsState::create(window_exprs, schema);
        assert_eq!(state.states.len(), 5, "Should have 5 window states");
        
        // Update 1: Single row batch
        let batch1 = create_test_batch(vec![1000], vec![10]);
        let results1 = state.update_batch(&batch1);
        
        assert_eq!(results1.len(), 5, "Should have 5 aggregates");
        assert_eq!(results1[0][0], ScalarValue::Int64(Some(10)), "SUM: 10");
        assert_eq!(results1[1][0], ScalarValue::Int64(Some(1)), "COUNT: 1");
        assert_eq!(results1[2][0], ScalarValue::Float64(Some(10.0)), "AVG: 10.0");
        assert_eq!(results1[3][0], ScalarValue::Int64(Some(10)), "MIN: 10");
        assert_eq!(results1[4][0], ScalarValue::Int64(Some(10)), "MAX: 10");
        assert_eq!(state.data.num_rows(), 1, "After update 1: should have 1 row (t=1000)");
        
        // Update 2: Multi-row batch within window
        let batch2 = create_test_batch(vec![1500, 2000], vec![30, 20]);
        let results2 = state.update_batch(&batch2);
        
        // Row 1 (t=1500): includes t=1000,1500
        assert_eq!(results2[0][0], ScalarValue::Int64(Some(40)), "SUM: 10+30=40");
        assert_eq!(results2[1][0], ScalarValue::Int64(Some(2)), "COUNT: 2");
        assert_eq!(results2[2][0], ScalarValue::Float64(Some(20.0)), "AVG: (10+30)/2=20.0");
        assert_eq!(results2[3][0], ScalarValue::Int64(Some(10)), "MIN: 10");
        assert_eq!(results2[4][0], ScalarValue::Int64(Some(30)), "MAX: 30");
        
        // Row 2 (t=2000): includes t=1000,1500,2000
        assert_eq!(results2[0][1], ScalarValue::Int64(Some(60)), "SUM: 10+30+20=60");
        assert_eq!(results2[1][1], ScalarValue::Int64(Some(3)), "COUNT: 3");
        assert_eq!(results2[2][1], ScalarValue::Float64(Some(20.0)), "AVG: (10+30+20)/3=20.0");
        assert_eq!(results2[3][1], ScalarValue::Int64(Some(10)), "MIN: 10");
        assert_eq!(results2[4][1], ScalarValue::Int64(Some(30)), "MAX: 30");
        assert_eq!(state.data.num_rows(), 3, "After update 2: should have 3 rows (t=1000,1500,2000)");
        
        // Update 3: Partial retraction (t=3200 causes t=1000 to be excluded)
        let batch3 = create_test_batch(vec![3200], vec![5]);
        let results3 = state.update_batch(&batch3);
        
        // Window now includes t=1500,2000,3200 (t=1000 excluded)
        assert_eq!(results3[0][0], ScalarValue::Int64(Some(55)), "SUM: 30+20+5=55");
        assert_eq!(results3[1][0], ScalarValue::Int64(Some(3)), "COUNT: 3");
        assert_eq!(results3[2][0], ScalarValue::Float64(Some(55.0/3.0)), "AVG: 55/3≈18.33");
        assert_eq!(results3[3][0], ScalarValue::Int64(Some(5)), "MIN: 5");
        assert_eq!(results3[4][0], ScalarValue::Int64(Some(30)), "MAX: 30");
        assert_eq!(state.data.num_rows(), 3, "After update 3: should have 3 rows (t=1500,2000,3200, t=1000 pruned)");
        
        // Update 4: Large time gap causing significant retraction
        let batch4 = create_test_batch(vec![6000], vec![100]);
        let results4 = state.update_batch(&batch4);
        
        // Only t=6000 value should be in window (others >2000ms old)
        assert_eq!(results4[0][0], ScalarValue::Int64(Some(100)), "SUM: 100 (only current)");
        assert_eq!(results4[1][0], ScalarValue::Int64(Some(1)), "COUNT: 1");
        assert_eq!(results4[2][0], ScalarValue::Float64(Some(100.0)), "AVG: 100.0");
        assert_eq!(results4[3][0], ScalarValue::Int64(Some(100)), "MIN: 100");
        assert_eq!(results4[4][0], ScalarValue::Int64(Some(100)), "MAX: 100");
        assert_eq!(state.data.num_rows(), 1, "After update 4: should have 1 row (t=6000, older rows pruned)");
        
        // Update 5: Identical timestamps with different values
        let batch5 = create_test_batch(vec![6000, 6000], vec![50, 75]);
        let results5 = state.update_batch(&batch5);
        
        // Row 1: includes previous 100 + current 50
        assert_eq!(results5[0][0], ScalarValue::Int64(Some(150)), "SUM: 100+50=150");
        assert_eq!(results5[1][0], ScalarValue::Int64(Some(2)), "COUNT: 2");
        assert_eq!(results5[2][0], ScalarValue::Float64(Some(75.0)), "AVG: (100+50)/2=75.0");
        assert_eq!(results5[3][0], ScalarValue::Int64(Some(50)), "MIN: 50");
        assert_eq!(results5[4][0], ScalarValue::Int64(Some(100)), "MAX: 100");
        
        // Row 2: includes 100 + 50 + 75
        assert_eq!(results5[0][1], ScalarValue::Int64(Some(225)), "SUM: 100+50+75=225");
        assert_eq!(results5[1][1], ScalarValue::Int64(Some(3)), "COUNT: 3");
        assert_eq!(results5[2][1], ScalarValue::Float64(Some(75.0)), "AVG: (100+50+75)/3=75.0");
        assert_eq!(results5[3][1], ScalarValue::Int64(Some(50)), "MIN: 50");
        assert_eq!(results5[4][1], ScalarValue::Int64(Some(100)), "MAX: 100");
        assert_eq!(state.data.num_rows(), 3, "After update 5: should have 3 rows (all t=6000 values)");
        
        // Update 6: Mixed batch with some values in window, some causing retraction
        let batch6 = create_test_batch(vec![7000, 7500, 8500], vec![25, 80, 15]);
        let results6 = state.update_batch(&batch6);
        
        // Row 1 (t=7000): includes all t=6000 values + current
        assert_eq!(results6[0][0], ScalarValue::Int64(Some(250)), "SUM: 100+50+75+25=250");
        
        // Row 2 (t=7500): still includes all t=6000 values + t=7000 + current
        assert_eq!(results6[0][1], ScalarValue::Int64(Some(330)), "SUM: 100+50+75+25+80=330");
        
        // Row 3 (t=8500): excludes t=6000 values, includes t=7000,7500,8500
        assert_eq!(results6[0][2], ScalarValue::Int64(Some(120)), "SUM: 25+80+15=120");
        assert_eq!(results6[1][2], ScalarValue::Int64(Some(3)), "COUNT: 3");
        assert_eq!(results6[2][2], ScalarValue::Float64(Some(40.0)), "AVG: (25+80+15)/3=40.0");
        assert_eq!(results6[3][2], ScalarValue::Int64(Some(15)), "MIN: 15");
        assert_eq!(results6[4][2], ScalarValue::Int64(Some(80)), "MAX: 80");
        assert_eq!(state.data.num_rows(), 3, "After update 6: should have 3 rows (t=7000,7500,8500, t=6000 values pruned)");
    }

    #[tokio::test]
    async fn test_simple_sum_sliding_window() {
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
        assert_eq!(state.data.num_rows(), 3, "Should have 3 total rows");
        assert_eq!(state.states.len(), 1, "Should have 1 window state");
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
        
        assert_eq!(state.data.num_rows(), 1, "Should have 1 total rows");
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
    async fn test_multiple_windows() {
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
    }
}