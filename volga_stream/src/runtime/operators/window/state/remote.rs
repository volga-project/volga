use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::WindowExpr;
use anyhow::Result;

#[derive(Debug)]
pub struct RemoteWindowsState {
    // TODO: Add internal state
}

impl RemoteWindowsState {
    pub fn create(_window_expr: Arc<dyn WindowExpr>, _schema: SchemaRef) -> Result<Self> {
        // TODO: Implementation for remote state
        Ok(Self {})
    }
}