use std::sync::Arc;

use crate::runtime::operators::window::window_operator_state::WindowId;
use crate::runtime::operators::window::{AggregatorType, TileConfig};
use datafusion::physical_plan::WindowExpr;

#[derive(Debug, Clone)]
pub struct WindowConfig {
    pub window_id: WindowId,
    pub window_expr: Arc<dyn WindowExpr>,
    pub tiling: Option<TileConfig>,
    pub aggregator_type: AggregatorType,
    pub exclude_current_row: Option<bool>, // request-mode only
}

