//! In-memory tile updates for ingest. No KV IO — caller plans/loads, then applies, then writes.

use arrow::array::{Int64Array, RecordBatch, TimestampMillisecondArray};
use datafusion::physical_plan::WindowExpr;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::runtime::operators::window::aggregates::merge_accumulator_state;
use crate::runtime::operators::window::create_window_aggregator;
use crate::runtime::operators::window::window_operator_state::WindowId;

use super::plan::{plan_update_runs, TileScanRun};
use super::{TileConfig, TileState, TimeGranularity, WindowTiles};

/// `(granularity, tile_start)` — SortedKV tile identity.
pub type TileKey = (TimeGranularity, i64);

/// [`TileScanRun`]s for tiles this batch will update (all configured granularities).
pub fn plan_update_runs_for_batch(
    config: &TileConfig,
    batch: &RecordBatch,
    ts_column_index: usize,
) -> Vec<TileScanRun> {
    if batch.num_rows() == 0 {
        return Vec::new();
    }
    let ts_int64 = ts_column_as_i64(batch, ts_column_index);
    plan_update_runs(config, (0..ts_int64.len()).map(|i| ts_int64.value(i)))
}

/// Merge `batch` into already-loaded `WindowTiles` for one window (in-memory only).
pub fn apply_batch_to_tiles(
    by_key: &mut BTreeMap<TileKey, WindowTiles>,
    window_id: WindowId,
    config: &TileConfig,
    window_expr: &Arc<dyn WindowExpr>,
    batch: &RecordBatch,
    ts_column_index: usize,
) {
    if batch.num_rows() == 0 {
        return;
    }
    let ts_int64 = ts_column_as_i64(batch, ts_column_index);
    for &gran in &config.granularities {
        apply_sorted_for_granularity(by_key, window_id, window_expr, batch, &ts_int64, gran);
    }
}

fn apply_sorted_for_granularity(
    by_key: &mut BTreeMap<TileKey, WindowTiles>,
    window_id: WindowId,
    window_expr: &Arc<dyn WindowExpr>,
    batch: &RecordBatch,
    ts_int64: &Int64Array,
    gran: TimeGranularity,
) {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return;
    }
    let mut start = 0usize;
    let mut tile_start = gran.start(ts_int64.value(0));
    for i in 1..num_rows {
        let next_start = gran.start(ts_int64.value(i));
        if next_start != tile_start {
            apply_slice(
                by_key,
                window_id,
                window_expr,
                gran,
                tile_start,
                &batch.slice(start, i - start),
            );
            start = i;
            tile_start = next_start;
        }
    }
    apply_slice(
        by_key,
        window_id,
        window_expr,
        gran,
        tile_start,
        &batch.slice(start, num_rows - start),
    );
}

fn apply_slice(
    by_key: &mut BTreeMap<TileKey, WindowTiles>,
    window_id: WindowId,
    window_expr: &Arc<dyn WindowExpr>,
    gran: TimeGranularity,
    tile_start: i64,
    slice: &RecordBatch,
) {
    if slice.num_rows() == 0 {
        return;
    }
    let window_tiles = by_key
        .get_mut(&(gran, tile_start))
        .expect("tile must be loaded before apply");
    let state = window_tiles.windows.entry(window_id).or_default();
    update_state_with_batch(window_expr, state, slice);
}

fn update_state_with_batch(
    window_expr: &Arc<dyn WindowExpr>,
    state: &mut TileState,
    batch: &RecordBatch,
) {
    let mut acc = create_window_aggregator(window_expr);
    if let Some(existing) = &state.accumulator_state {
        merge_accumulator_state(acc.as_mut(), existing.as_ref());
    }
    let args = window_expr
        .evaluate_args(batch)
        .expect("evaluate_args for tile update");
    acc.update_batch(&args).expect("update tile state");
    state.accumulator_state = Some(acc.state().expect("tile state"));
    state.entry_count = state
        .entry_count
        .saturating_add(batch.num_rows() as u64);
}

fn ts_column_as_i64(batch: &RecordBatch, ts_column_index: usize) -> Int64Array {
    use arrow::compute::kernels::cast::cast;
    use arrow::datatypes::DataType;

    let ts_array = batch
        .column(ts_column_index)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("Timestamp column should be TimestampMillisecondArray");
    let casted = cast(ts_array, &DataType::Int64).expect("cast ts to Int64");
    casted
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .clone()
}
