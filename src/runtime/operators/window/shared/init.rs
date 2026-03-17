use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use tokio_rayon::rayon::{ThreadPool, ThreadPoolBuilder};

use crate::runtime::operators::window::aggregates::get_aggregate_type;
use crate::runtime::operators::window::shared::config::WindowConfig;
use crate::runtime::operators::window::shared::output::create_output_schema;
use crate::runtime::operators::window::window_operator_state::WindowId;
use crate::runtime::operators::window::TileConfig;

pub fn build_window_operator_parts(
    is_request_operator: bool,
    window_exec: &Arc<BoundedWindowAggExec>,
    tiling_configs: &Vec<Option<TileConfig>>,
    parallelize: bool,
) -> (usize, BTreeMap<WindowId, WindowConfig>, SchemaRef, SchemaRef, Option<ThreadPool>) {
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

    let thread_pool = if parallelize {
        Some(
            ThreadPoolBuilder::new()
                .num_threads(std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
                .build()
                .expect("Failed to create thread pool"),
        )
    } else {
        None
    };

    (ts_column_index, windows, input_schema, output_schema, thread_pool)
}

