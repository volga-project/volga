use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::windows::BoundedWindowAggExec;

use crate::runtime::operators::window::aggregates::get_aggregate_type;
use crate::runtime::operators::window::shared::config::WindowConfig;
use crate::runtime::operators::window::shared::output::create_output_schema;
use crate::runtime::operators::window::window_operator_state::WindowId;
use crate::runtime::operators::window::window_tuning::WindowOperatorSpec;
use crate::runtime::operators::window::TileConfig;

/// Pad/fill per-window tiling from `tiling_configs` + `spec.tiling` default.
pub fn resolve_tiling_configs(
    n_windows: usize,
    tiling_configs: &[Option<TileConfig>],
    spec: &WindowOperatorSpec,
) -> Vec<Option<TileConfig>> {
    let mut out = tiling_configs.to_vec();
    out.resize(n_windows, None);
    if let Some(default) = &spec.tiling {
        for t in &mut out {
            if t.is_none() {
                *t = Some(default.clone());
            }
        }
    }
    out
}

pub fn build_window_operator_parts(
    is_request_operator: bool,
    window_exec: &Arc<BoundedWindowAggExec>,
    tiling_configs: &Vec<Option<TileConfig>>,
) -> (usize, BTreeMap<WindowId, WindowConfig>, SchemaRef, SchemaRef) {
    let ts_column_index = window_exec.window_expr()[0].order_by()[0]
        .expr
        .as_any()
        .downcast_ref::<Column>()
        .expect("Expected Column expression in ORDER BY")
        .index();

    let mut windows = BTreeMap::new();
    for (window_id, window_expr) in window_exec.window_expr().iter().enumerate() {
        let exclude_current_row = if is_request_operator {
            // TODO get from SQL, only for request operator
            Some(false)
        } else {
            None
        };
        windows.insert(
            window_id,
            WindowConfig {
                window_id,
                window_expr: window_expr.clone(),
                tiling: tiling_configs.get(window_id).and_then(|config| config.clone()),
                aggregator_type: get_aggregate_type(window_expr),
                exclude_current_row,
            },
        );
    }

    let input_schema = window_exec.input().schema();
    let output_schema = create_output_schema(&input_schema, &window_exec.window_expr());

    (ts_column_index, windows, input_schema, output_schema)
}
