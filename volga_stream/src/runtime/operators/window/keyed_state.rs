use std::collections::HashMap;
use std::sync::Arc;
use std::ops::Range;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use arrow::array::{Array, ArrayRef};
use arrow::row::Row;
use arrow::datatypes::SchemaRef;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Accumulator;
use datafusion::physical_plan::{WindowExpr, expressions::Column};
use datafusion::physical_expr::window::SlidingAggregateWindowExpr;

#[derive(Debug)]
pub struct KeyedWindowsState {
    states: HashMap<Vec<u8>, WindowsState>,
}

impl KeyedWindowsState {
    pub fn new() -> Self {
        Self {
            states: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct WindowState {
    accumulator: Box<dyn Accumulator>,
    expr: Arc<dyn WindowExpr>,
    start: usize // all windows end with current, so not tracking it
}

#[derive(Debug)]
pub enum WindowsState {
    Local(LocalWindowsState),
    Remote(RemoteWindowsState),
}

#[derive(Debug)]
pub struct LocalWindowsState {
    states: Vec<WindowState>,
    data: RecordBatch,
    schema: SchemaRef,
    time_column_idx: usize
}

impl LocalWindowsState {
    pub fn create(window_exprs: Vec<Arc<dyn WindowExpr>>, schema: SchemaRef) -> Result<Self> {
        let mut states = Vec::new();

        // TODO assert all windows have same orderbys
        let order_by_exprs = window_exprs[0].order_by().to_vec();

        for window_expr in window_exprs {
            // Cast to AggregateWindowExpr to get accumulator
            let aggregate_expr = window_expr.as_any()
                .downcast_ref::<SlidingAggregateWindowExpr>()
                .ok_or_else(|| anyhow::anyhow!("Only SlidingAggregateWindowExpr is supported"))?;
            
            let accumulator = aggregate_expr.get_aggregate_expr().create_accumulator()?;
            let window_state = WindowState {
                accumulator, 
                expr: window_expr, 
                start: 0
            };
            states.push(window_state);
        }
        
        // Extract time_column_idx from the first ORDER BY expression
        let time_column_idx = if let Some(first_order_by) = order_by_exprs.first() {
            // Find the column index in the schema by matching column names
            if let Some(column_expr) = first_order_by.expr.as_any().downcast_ref::<Column>() {
                column_expr.index()
            } else {
                // If not a simple column reference, default to 0
                0
            }
        } else {
            // No ORDER BY clause, default to 0
            0
        };
        
        let data = RecordBatch::new_empty(Arc::clone(&schema));

        Ok(Self {
            states,
            data,
            schema,
            time_column_idx
        })
    }

    pub fn update_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.data = arrow::compute::concat_batches(&self.data.schema(), [&self.data, &batch])?;
        
        // Process each state individually to avoid borrow checker issues
        for i in 0..self.states.len() {
            let to_update: Vec<Arc<dyn Array>> = self.states[i].expr.evaluate_args(batch)?;
            self.update_window_at_index(i, to_update)?;
        }

        self.prune();
        Ok(())
    }

    fn update_window_at_index(&mut self, state_index: usize, to_update: Vec<Arc<dyn Array>>) -> Result<()> {
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
            
            self.states[state_index].accumulator.update_batch(&row_arrays)?;

            // find values to retract in self.data
            let retract_range = self.find_retract_range(row_idx, last_retract_end)?;
            last_retract_end = retract_range.end;

            // extract values to retract by slicing self.data
            let retract_batch = self.data.slice(retract_range.start, retract_range.end - retract_range.start);

            // retract from accumulator
            let retract_values = self.states[state_index].expr.evaluate_args(&retract_batch)?;
            self.states[state_index].accumulator.retract_batch(&retract_values)?;
            let result = self.states[state_index].accumulator.evaluate()?;
            results.push(result);
        }

        self.states[state_index].start = last_retract_end;
        Ok(())
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

    fn find_retract_range(&self, row_idx: usize, last_retract_end: usize) -> Result<Range<usize>> {
        // TODO this is for time based window with RANGE type, also handle ROWS and other interval types
        let time_column = self.data.column(self.time_column_idx);
        let time_val = ScalarValue::try_from_array(time_column, row_idx)?;
        let time_val = self.extract_timestamp_value(&time_val)?;
        
        let start = last_retract_end;
        let mut end = start;
        
        // Find the end of retract range using while loop
        while end < self.data.num_rows() {
            let cur_time = ScalarValue::try_from_array(time_column, end)?;
            let cur_time = self.extract_timestamp_value(&cur_time)?;
            
            if let (Some(cur_time), Some(time_val)) = (cur_time, time_val) {
                if cur_time <= time_val {
                    end += 1;
                } else {
                    break;
                }
            } else {
                // Handle null values - skip them
                end += 1;
            }
        }
        
        Ok(Range { start, end })
    }

    /// Extract timestamp value from ScalarValue
    /// TODO check datafusion time utils for this
    fn extract_timestamp_value(&self, scalar: &ScalarValue) -> Result<Option<i64>> {
        match scalar {
            ScalarValue::TimestampSecond(Some(val), _) => Ok(Some(*val)),
            ScalarValue::TimestampMillisecond(Some(val), _) => Ok(Some(*val)),
            ScalarValue::TimestampMicrosecond(Some(val), _) => Ok(Some(*val)),
            ScalarValue::TimestampNanosecond(Some(val), _) => Ok(Some(*val)),
            ScalarValue::Int64(Some(val)) => Ok(Some(*val)),
            ScalarValue::UInt64(Some(val)) => Ok(Some(*val as i64)),
            _ => Ok(None),
        }
    }
}

#[derive(Debug)]
pub struct RemoteWindowsState {
    // TODO: Add internal state
}

impl RemoteWindowsState {
    pub fn create(_window_expr: Arc<dyn WindowExpr>, _schema: SchemaRef) -> Result<Self> {
        // TODO: Implementation for remote state
        Ok(Self {})
    }

    pub fn add_row(&mut self, _row: Row) -> Result<()> {
        // TODO: Implementation
        Ok(())
    }

    pub fn retract_range(&mut self, _start_time: i64, _end_time: i64) -> Result<()> {
        // TODO: Implementation
        Ok(())
    }

    pub fn get_result(&mut self) -> Result<ScalarValue> {
        // TODO: Implementation
        Ok(ScalarValue::Null)
    }
}