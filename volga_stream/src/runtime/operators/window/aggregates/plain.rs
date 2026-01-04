use std::sync::Arc;

use arrow::array::ArrayRef;
use async_trait::async_trait;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use tokio_rayon::rayon::ThreadPool;

use crate::runtime::operators::window::state::index::BucketIndex;
use crate::runtime::operators::window::state::index::window_logic;
use crate::runtime::operators::window::state::tiles::Tile;
use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::runtime::operators::window::{Cursor, Tiles};
use crate::storage::batch_store::Timestamp;

use super::{Aggregation, AggregatorType, BucketRange};
use super::{WindowAggregator, create_window_aggregator, merge_accumulator_state};
use crate::runtime::operators::window::state::index::{RowPtr, SortedRangeIndex};

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
        batch_index: &BucketIndex,
        window_expr: Arc<dyn WindowExpr>,
        tiles: Option<Tiles>,
        bounds: CursorBounds,
    ) -> Self {
        Self::Range(plain_range::PlainRangeAggregation::new(
            batch_index,
            window_expr,
            tiles,
            bounds,
        ))
    }

    pub fn for_points(
        points: Vec<VirtualPoint>,
        batch_index: &BucketIndex,
        window_expr: Arc<dyn WindowExpr>,
        tiles: Option<Tiles>,
        exclude_current_row: bool,
    ) -> Self {
        Self::Points(plain_points::PlainPointsAggregation::new(
            points,
            batch_index,
            window_expr,
            tiles,
            exclude_current_row,
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

    fn get_data_requests(&self) -> Vec<crate::runtime::operators::window::state::index::DataRequest> {
        match self {
            PlainAggregation::Range(r) => r.get_data_requests(),
            PlainAggregation::Points(p) => p.get_data_requests(),
        }
    }

    async fn produce_aggregates_from_ranges(
        &self,
        sorted_ranges: &[crate::runtime::operators::window::state::index::SortedRangeView],
        thread_pool: Option<&ThreadPool>,
    ) -> (Vec<ScalarValue>, Option<AccumulatorState>) {
        match self {
            PlainAggregation::Range(r) => {
                r.produce_aggregates_from_ranges(sorted_ranges, thread_pool)
                    .await
            }
            PlainAggregation::Points(p) => {
                p.produce_aggregates_from_ranges(sorted_ranges, thread_pool)
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

// TODO we should avoid calling tiled_split on each iteration
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

// (helper removed; points impl uses a local version)
