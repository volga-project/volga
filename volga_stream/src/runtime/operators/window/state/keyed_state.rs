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
    pub start: usize // all windows end with current, so not tracking it
}

impl WindowState {
    pub fn new(accumulator: Box<dyn Accumulator>, expr: Arc<dyn WindowExpr>, start: usize) -> Self {
        Self {
            accumulator,
            expr,
            start,
        }
    }
}

#[derive(Debug)]
pub enum WindowsState {
    Local(LocalWindowsState),
    Remote(RemoteWindowsState),
}