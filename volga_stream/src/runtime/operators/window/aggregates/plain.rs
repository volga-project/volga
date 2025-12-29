use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::ArrayRef;
use async_trait::async_trait;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use tokio_rayon::rayon::ThreadPool;

use crate::runtime::operators::window::index::BucketIndex;
use crate::runtime::operators::window::index::window_logic;
use crate::runtime::operators::window::tiles::Tile;
use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::runtime::operators::window::{Cursor, Tiles};
use crate::storage::batch_store::Timestamp;

use super::{Aggregation, AggregatorType, BucketRange};
use super::{WindowAggregator, create_window_aggregator, merge_accumulator_state};
use crate::runtime::operators::window::index::{RowPtr, SortedRangeIndex};

#[path = "plain_range.rs"]
mod plain_range;
#[path = "plain_points.rs"]
mod plain_points;

pub use super::VirtualPoint;

#[derive(Debug, Clone, Copy)]
pub struct CursorBounds {
    pub prev: Option<Cursor>,
    pub new: Cursor,
}

#[derive(Debug)]
pub enum PlainAggregation {
    Range(plain_range::PlainRangeAggregation),
    Points(plain_points::PlainPointsAggregation),
}

impl PlainAggregation {
    pub fn for_range(
        entry_range: BucketRange,
        batch_index: &BucketIndex,
        window_expr: Arc<dyn WindowExpr>,
        tiles: Option<Tiles>,
        ts_column_index: usize,
        window_id: usize,
        bounds: Option<CursorBounds>,
    ) -> Self {
        Self::Range(plain_range::PlainRangeAggregation::new(
            entry_range,
            batch_index,
            window_expr,
            tiles,
            ts_column_index,
            window_id,
            bounds,
        ))
    }

    pub fn for_points(
        points: Vec<VirtualPoint>,
        batch_index: &BucketIndex,
        window_expr: Arc<dyn WindowExpr>,
        tiles: Option<Tiles>,
        ts_column_index: usize,
        window_id: usize,
    ) -> Self {
        Self::Points(plain_points::PlainPointsAggregation::new(
            points,
            batch_index,
            window_expr,
            tiles,
            ts_column_index,
            window_id,
        ))
    }
}

#[async_trait]
impl Aggregation for PlainAggregation {
    fn window_expr(&self) -> &Arc<dyn WindowExpr> {
        match self {
            PlainAggregation::Range(r) => r.window_expr(),
            PlainAggregation::Points(p) => p.window_expr(),
        }
    }

    fn aggregator_type(&self) -> AggregatorType {
        AggregatorType::PlainAccumulator
    }

    fn get_data_requests(
        &self,
        exclude_current_row: Option<bool>,
    ) -> Vec<crate::runtime::operators::window::index::DataRequest> {
        match self {
            PlainAggregation::Range(r) => r.get_data_requests(exclude_current_row),
            PlainAggregation::Points(p) => p.get_data_requests(exclude_current_row),
        }
    }

    async fn produce_aggregates_from_ranges(
        &self,
        sorted_ranges: &[crate::runtime::operators::window::index::SortedRangeView],
        thread_pool: Option<&ThreadPool>,
        exclude_current_row: Option<bool>,
    ) -> (Vec<ScalarValue>, Option<AccumulatorState>) {
        match self {
            PlainAggregation::Range(r) => {
                r.produce_aggregates_from_ranges(sorted_ranges, thread_pool, exclude_current_row)
                    .await
            }
            PlainAggregation::Points(p) => {
                p.produce_aggregates_from_ranges(sorted_ranges, thread_pool, exclude_current_row)
                    .await
            }
        }
    }
}

// ----------------- shared helpers (range + points) -----------------

fn eval_stored_window(
    window_expr: &Arc<dyn WindowExpr>,
    row_index: &SortedRangeIndex,
    start: RowPtr,
    end: RowPtr,
    tiles: Option<&Tiles>,
) -> ScalarValue {
    let mut accumulator = match create_window_aggregator(window_expr) {
        WindowAggregator::Accumulator(accumulator) => accumulator,
        WindowAggregator::Evaluator(_) => panic!("PlainAggregation should not use evaluator"),
    };
    apply_stored_window(accumulator.as_mut(), row_index, start, end, tiles);
    accumulator.evaluate().expect("evaluate failed")
}

fn apply_stored_window(
    accumulator: &mut dyn datafusion::logical_expr::Accumulator,
    row_index: &SortedRangeIndex,
    start: RowPtr,
    end: RowPtr,
    tiles: Option<&Tiles>,
) {
    let (front_args, middle_tiles, back_args) = window_slices(row_index, start, end, tiles);

    if !front_args.is_empty() {
        accumulator.update_batch(&front_args).expect("update_batch failed");
    }
    for tile in middle_tiles {
        if let Some(tile_state) = tile.accumulator_state {
            merge_accumulator_state(accumulator, tile_state.as_ref());
        }
    }
    if !back_args.is_empty() {
        accumulator.update_batch(&back_args).expect("update_batch failed");
    }
}

fn window_slices(
    row_index: &SortedRangeIndex,
    start: RowPtr,
    end: RowPtr,
    tiles: Option<&Tiles>,
) -> (Vec<ArrayRef>, Vec<Tile>, Vec<ArrayRef>) {
    if let Some(tiles) = tiles {
        if let Some(split) = window_logic::tiled_split(row_index, start, end, tiles) {
            let front_args = if split.front_end >= start {
                row_index.get_args_in_range(&start, &split.front_end)
            } else {
                vec![]
            };
            let back_args = if split.back_start <= end {
                row_index.get_args_in_range(&split.back_start, &end)
            } else {
                vec![]
            };
            return (front_args, split.tiles, back_args);
        }
    }
    (row_index.get_args_in_range(&start, &end), vec![], vec![])
}

pub(super) fn last_row_le_ts(idx: &SortedRangeIndex, ts: Timestamp) -> Option<RowPtr> {
    if idx.is_empty() {
        return None;
    }
    let first_ts = idx.get_timestamp(&idx.first_pos());
    if first_ts > ts {
        return None;
    }
    match idx.seek_rowpos_gt(Cursor::new(ts, u64::MAX)) {
        Some(after) => idx.prev_pos(after),
        None => Some(idx.last_pos()),
    }
}
