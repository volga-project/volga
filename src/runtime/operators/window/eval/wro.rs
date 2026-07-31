//! WRO point-lookup entrypoint.
//!
//! Points are rebuilt from one exact coherent raw+tile read.

use std::collections::BTreeMap;

use anyhow::Result;
use arrow::array::{ArrayRef, RecordBatch};
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::config::WindowConfig;
use crate::runtime::operators::window::frame_utils::{get_window_length_ms, require_range_frame};
use crate::runtime::operators::window::model::{Cursor, WindowId};
use crate::runtime::operators::window::store::{PartitionKey, WindowData, WindowRequestStore};

use super::coverage_plan::{merge_raw_runs, merge_tile_runs};
use super::eval_plan::{append_coverage_runs, plan_rebuilds, EvalPlan};
use super::rebuild::eval_rebuild;

pub async fn evaluate_points(
    store: &dyn WindowRequestStore,
    partition: &PartitionKey,
    window_configs: &BTreeMap<WindowId, WindowConfig>,
    point_timestamps: &[i64],
    request_batch: &RecordBatch,
    ts_column_index: usize,
) -> Result<Vec<Vec<ScalarValue>>> {
    if point_timestamps.is_empty() {
        return Ok(window_configs.keys().map(|_| Vec::new()).collect());
    }

    let mut raw_runs = Vec::new();
    let mut tile_runs = Vec::new();
    let mut plans = BTreeMap::new();
    for (window_id, cfg) in window_configs {
        require_range_frame(cfg.window_expr.get_window_frame());
        let wl = get_window_length_ms(cfg.window_expr.get_window_frame());
        let ends: Vec<_> = point_timestamps
            .iter()
            .map(|&ts| Cursor::new(ts, u64::MAX))
            .collect();
        let window_plans = plan_rebuilds(&ends, wl, cfg.tiling.as_ref());
        append_coverage_runs(&window_plans, &mut raw_runs, &mut tile_runs);
        plans.insert(*window_id, window_plans);
    }
    let raw_runs = merge_raw_runs(raw_runs);
    let tile_runs = merge_tile_runs(tile_runs);
    let data = store
        .load_window_data(partition, &raw_runs, &tile_runs)
        .await?;

    Ok(window_configs
        .iter()
        .map(|(window_id, cfg)| {
            rebuild_points(
                *window_id,
                cfg,
                request_batch,
                plans.get(window_id).expect("WRO plan"),
                ts_column_index,
                &data,
            )
        })
        .collect())
}

fn rebuild_points(
    window_id: WindowId,
    cfg: &WindowConfig,
    request_batch: &RecordBatch,
    plans: &[EvalPlan],
    ts_column_index: usize,
    data: &WindowData,
) -> Vec<ScalarValue> {
    let view = data.for_window(window_id, ts_column_index, &cfg.window_expr);
    let include_request_row = !cfg.exclude_current_row;
    let request_args = include_request_row.then(|| {
        cfg.window_expr
            .evaluate_args(request_batch)
            .expect("evaluate request args")
    });
    plans
        .iter()
        .enumerate()
        .map(|(row, unit)| {
            let row_args: Option<Vec<ArrayRef>> = request_args
                .as_ref()
                .map(|args| args.iter().map(|array| array.slice(row, 1)).collect());
            eval_rebuild(
                &cfg.window_expr,
                &view,
                view.nav.seek_le(unit.end),
                unit.coverage.as_ref().expect("rebuild coverage"),
                row_args.as_deref(),
            )
        })
        .collect()
}
