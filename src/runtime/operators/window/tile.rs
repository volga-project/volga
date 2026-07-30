//! Tile configuration, projection, and WO ingest updates.

use arrow::array::{Int64Array, RecordBatch, TimestampMillisecondArray};
use datafusion::physical_plan::WindowExpr;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::runtime::operators::window::aggs::merge_accumulator_state;
use crate::runtime::operators::window::create_window_accumulator;
use crate::runtime::operators::window::model::{
    Tile, TileMap, TileRun, TileState, TimeGranularity, Timestamp, WindowId, WindowTiles,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TileConfig {
    pub granularities: Vec<TimeGranularity>,
}

impl TileConfig {
    pub fn new(mut granularities: Vec<TimeGranularity>) -> Result<Self, String> {
        granularities.sort();
        for i in 1..granularities.len() {
            if !granularities[i].is_multiple_of(&granularities[i - 1]) {
                return Err(format!(
                    "Granularity {:?} must be a multiple of {:?}",
                    granularities[i],
                    granularities[i - 1]
                ));
            }
        }
        Ok(Self { granularities })
    }

    pub fn default_config() -> Self {
        Self::new(vec![
            TimeGranularity::Minutes(1),
            TimeGranularity::Minutes(5),
            TimeGranularity::Hours(1),
            TimeGranularity::Days(1),
        ])
        .expect("Default config should be valid")
    }

    pub fn min_granularity(&self) -> TimeGranularity {
        *self
            .granularities
            .first()
            .expect("TileConfig must have at least one granularity")
    }
}

/// Project one window's tiles from loaded shared tile states.
pub fn project_tiles(by_key: &TileMap, window_id: WindowId) -> Vec<Tile> {
    let mut out = Vec::new();
    for ((gran, tile_start), window_tiles) in by_key {
        if let Some(state) = window_tiles.windows.get(&window_id) {
            out.push(Tile::new(*tile_start, *gran, state.clone()));
        }
    }
    out.sort_by_key(|tile| (tile.tile_start, tile.granularity.to_millis()));
    out
}

/// `(granularity, tile_start)` tile identity.
pub type TileKey = (TimeGranularity, i64);

/// One run per configured tile bucket touched by `timestamps`.
pub fn plan_update_runs(
    config: &TileConfig,
    timestamps: impl IntoIterator<Item = Timestamp>,
) -> Vec<TileRun> {
    let mut by_gran: BTreeMap<TimeGranularity, BTreeSet<Timestamp>> = BTreeMap::new();
    for ts in timestamps {
        for &gran in &config.granularities {
            by_gran.entry(gran).or_default().insert(gran.start(ts));
        }
    }

    let mut runs = Vec::new();
    for (gran, starts) in by_gran {
        let step = gran.to_millis();
        for start in starts {
            runs.push(TileRun {
                granularity: gran,
                start_ts: start,
                end_ts_exclusive: start.saturating_add(step),
            });
        }
    }
    runs
}

/// [`TileRun`]s for tiles this batch will update (all configured granularities).
pub fn plan_update_runs_for_batch(
    config: &TileConfig,
    batch: &RecordBatch,
    ts_column_index: usize,
) -> Vec<TileRun> {
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
    let mut acc = create_window_accumulator(window_expr);
    if let Some(existing) = &state.accumulator_state {
        merge_accumulator_state(acc.as_mut(), existing.as_ref());
    }
    let args = window_expr
        .evaluate_args(batch)
        .expect("evaluate_args for tile update");
    acc.update_batch(&args).expect("update tile state");
    state.accumulator_state = Some(acc.state().expect("tile state"));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tile_config_validation() {
        assert!(TileConfig::new(vec![
            TimeGranularity::Minutes(1),
            TimeGranularity::Minutes(5),
            TimeGranularity::Hours(1),
        ])
        .is_ok());
        assert!(TileConfig::new(vec![
            TimeGranularity::Minutes(5),
            TimeGranularity::Minutes(7),
        ])
        .is_err());
    }

    #[test]
    fn plan_update_runs_one_per_gran_per_bucket() {
        let m1 = TimeGranularity::Minutes(1);
        let m5 = TimeGranularity::Minutes(5);
        let config = TileConfig::new(vec![m1, m5]).unwrap();

        assert!(plan_update_runs(&config, std::iter::empty::<i64>()).is_empty());

        let runs = plan_update_runs(&config, [90_000, 90_000, 6 * 60_000]);
        assert_eq!(runs.len(), 4);
        assert!(runs
            .iter()
            .any(|r| r.granularity == m1 && r.start_ts == 60_000));
        assert!(runs
            .iter()
            .any(|r| r.granularity == m1 && r.start_ts == 6 * 60_000));
        assert!(runs.iter().any(|r| r.granularity == m5 && r.start_ts == 0));
        assert!(runs
            .iter()
            .any(|r| r.granularity == m5 && r.start_ts == 5 * 60_000));
        for run in runs {
            assert_eq!(
                run.end_ts_exclusive,
                run.start_ts + run.granularity.to_millis()
            );
        }
    }
}
