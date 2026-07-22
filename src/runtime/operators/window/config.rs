//! Per-window eval config + WO/WRO construction from a DataFusion window exec.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaBuilder, SchemaRef};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::physical_plan::WindowExpr;

use crate::runtime::operators::window::aggregates::get_aggregate_type;
use crate::runtime::operators::window::window_operator_state::WindowId;
use crate::runtime::operators::window::window_tuning::WindowOperatorSpec;
use crate::runtime::operators::window::{AggregatorType, TileConfig};

#[derive(Debug, Clone)]
pub struct WindowConfig {
    pub window_id: WindowId,
    pub window_expr: Arc<dyn WindowExpr>,
    pub tiling: Option<TileConfig>,
    pub aggregator_type: AggregatorType,
    /// WRO only: skip request args; store window still through `T`. `None` for WO.
    pub exclude_current_row: Option<bool>,
}

/// Schemas + per-window configs built from a `BoundedWindowAggExec`.
pub struct BuiltWindows {
    pub ts_column_index: usize,
    pub windows: BTreeMap<WindowId, WindowConfig>,
    pub input_schema: SchemaRef,
    pub output_schema: SchemaRef,
}

impl BuiltWindows {
    pub fn for_wo(
        window_exec: &Arc<BoundedWindowAggExec>,
        tiling_overrides: &[Option<TileConfig>],
        spec: &WindowOperatorSpec,
    ) -> Self {
        let tiling = spec.resolve_tiling(window_exec.window_expr().len(), tiling_overrides);
        Self::build(window_exec, &tiling, None)
    }

    pub fn for_wro(
        window_exec: &Arc<BoundedWindowAggExec>,
        tiling_overrides: &[Option<TileConfig>],
        spec: &WindowOperatorSpec,
        exclude_current_row: bool,
    ) -> Self {
        let tiling = spec.resolve_tiling(window_exec.window_expr().len(), tiling_overrides);
        Self::build(window_exec, &tiling, Some(exclude_current_row))
    }

    fn build(
        window_exec: &Arc<BoundedWindowAggExec>,
        tiling_configs: &[Option<TileConfig>],
        exclude_current_row: Option<bool>,
    ) -> Self {
        let ts_column_index = window_exec.window_expr()[0].order_by()[0]
            .expr
            .as_any()
            .downcast_ref::<Column>()
            .expect("Expected Column expression in ORDER BY")
            .index();

        let mut windows = BTreeMap::new();
        for (window_id, window_expr) in window_exec.window_expr().iter().enumerate() {
            windows.insert(
                window_id,
                WindowConfig {
                    window_id,
                    window_expr: window_expr.clone(),
                    tiling: tiling_configs.get(window_id).and_then(|c| c.clone()),
                    aggregator_type: get_aggregate_type(window_expr),
                    exclude_current_row,
                },
            );
        }

        let input_schema = window_exec.input().schema();
        let output_schema = create_output_schema(&input_schema, window_exec.window_expr());

        Self {
            ts_column_index,
            windows,
            input_schema,
            output_schema,
        }
    }
}

fn create_output_schema(
    input_schema: &Schema,
    window_exprs: &[Arc<dyn WindowExpr>],
) -> Arc<Schema> {
    let capacity = input_schema.fields().len() + window_exprs.len();
    let mut builder = SchemaBuilder::with_capacity(capacity);
    builder.extend(input_schema.fields().iter().cloned());
    for expr in window_exprs {
        builder.push(expr.field().expect("Should be able to get field"));
    }
    Arc::new(builder.finish().with_metadata(input_schema.metadata().clone()))
}
