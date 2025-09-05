use std::collections::HashMap;
use std::sync::Arc;
use std::ops::Range;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use arrow::array::{Array, ArrayRef};
use arrow::row::Row;
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{Accumulator, WindowFrameBound, WindowFrameUnits};
use datafusion::physical_plan::{WindowExpr, expressions::Column};
use datafusion::physical_expr::window::SlidingAggregateWindowExpr;

use crate::runtime::operators::window::state::{LocalWindowsState, RemoteWindowsState};

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
    pub accumulator: Box<dyn Accumulator>,
    pub expr: Arc<dyn WindowExpr>,
    pub start_row_idx: usize // all windows end with current, so not tracking it
}

impl WindowState {
    pub fn new(accumulator: Box<dyn Accumulator>, expr: Arc<dyn WindowExpr>, start: usize) -> Self {
        Self {
            accumulator,
            expr,
            start_row_idx: start,
        }
    }
}

#[derive(Debug)]
pub enum WindowsState {
    Local(LocalWindowsState),
    Remote(RemoteWindowsState),
}

pub fn validate_window_expr(expr: &Arc<dyn WindowExpr>, input_schema: &Schema) {
    let window_frame = expr.get_window_frame();

    // Support only ROWS and RANGE
    if window_frame.units == WindowFrameUnits::Groups {
        panic!("Groups WindowFrameUnits are not supported");
    }

    // start bound should be only preceeding, end should always be current row
    match window_frame.start_bound {
        WindowFrameBound::Preceding(_) => {}
        _ => panic!("Only Preceding start bound is supported"),
    }

    match window_frame.end_bound {
        WindowFrameBound::CurrentRow => {}
        _ => panic!("Only CurrentRow end bound is supported"),
    }

    // Following Flink's approach: validate ORDER BY columns for streaming
    for sort_expr in expr.order_by() {
        if sort_expr.expr.nullable(input_schema).unwrap() {
            panic!("ORDER BY should not contain nullable exprs")
        }
        
        // We don't support ORDER BY DESC yet - requires different window boundary logic
        if sort_expr.options.descending {
            panic!("ORDER BY DESC is not supported yet")
        }
    }
}