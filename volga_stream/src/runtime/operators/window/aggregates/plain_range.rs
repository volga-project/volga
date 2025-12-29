use std::sync::Arc;

use async_trait::async_trait;
use datafusion::logical_expr::WindowFrameUnits;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggregates::{Aggregation, BucketRange};
use crate::runtime::operators::window::index::{BucketIndex, DataBounds, DataRequest, SortedRangeIndex, SortedRangeView, get_window_length_ms, get_window_size_rows};
use crate::runtime::operators::window::index::window_logic;
use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::runtime::operators::window::{RowPtr, Tiles, TimeGranularity};
use crate::runtime::operators::window::Cursor;

use super::{CursorBounds, eval_stored_window};

#[derive(Debug)]
pub struct PlainRangeAggregation {
    entry_range: BucketRange,
    window_range: BucketRange,
    bounds: Option<CursorBounds>,
    window_expr: Arc<dyn WindowExpr>,
    tiles: Option<Tiles>,
    window_id: usize,
    bucket_granularity: TimeGranularity,
}

impl PlainRangeAggregation {
    pub fn new(
        entry_range: BucketRange,
        bucket_index: &BucketIndex,
        window_expr: Arc<dyn WindowExpr>,
        tiles: Option<Tiles>,
        _ts_column_index: usize,
        window_id: usize,
        bounds: Option<CursorBounds>,
    ) -> Self {
        let window_frame = window_expr.get_window_frame();
        let window_range = match window_frame.units {
            WindowFrameUnits::Range => {
                let wl = get_window_length_ms(window_frame);
                bucket_index.get_relevant_range_for_range_windows(entry_range, wl)
            }
            WindowFrameUnits::Rows => {
                let ws = get_window_size_rows(window_frame);
                bucket_index.get_relevant_range_for_rows_windows(entry_range, ws)
            }
            _ => entry_range,
        };

        Self {
            entry_range,
            window_range,
            bounds,
            window_expr,
            tiles,
            window_id,
            bucket_granularity: bucket_index.bucket_granularity(),
        }
    }
}

#[async_trait]
impl Aggregation for PlainRangeAggregation {
    fn window_expr(&self) -> &Arc<dyn WindowExpr> {
        &self.window_expr
    }

    fn aggregator_type(&self) -> crate::runtime::operators::window::AggregatorType {
        crate::runtime::operators::window::AggregatorType::PlainAccumulator
    }

    fn get_data_requests(&self, _exclude_current_row: Option<bool>) -> Vec<DataRequest> {
        let window_frame = self.window_expr.get_window_frame();
        let wl = get_window_length_ms(window_frame);
        let ws = get_window_size_rows(window_frame);

        let bounds = match self.bounds {
            None => DataBounds::All,
            Some(CursorBounds { prev, new }) => {
                if window_frame.units == WindowFrameUnits::Range {
                    let start_ts = prev
                        .map(|p| p.ts.saturating_sub(wl))
                        .unwrap_or(i64::MIN);
                    DataBounds::Time {
                        start_ts,
                        end_ts: new.ts,
                    }
                } else if window_frame.units == WindowFrameUnits::Rows && ws > 0 {
                    DataBounds::All
                } else {
                    DataBounds::All
                }
            }
        };

        vec![DataRequest {
            bucket_range: self.window_range,
            bounds,
        }]
    }

    async fn produce_aggregates_from_ranges(
        &self,
        sorted_ranges: &[SortedRangeView],
        _thread_pool: Option<&tokio_rayon::rayon::ThreadPool>,
        exclude_current_row: Option<bool>,
    ) -> (Vec<ScalarValue>, Option<AccumulatorState>) {
        let exclude_current_row = exclude_current_row.unwrap_or(false);
        let Some(view) = sorted_ranges.first() else {
            return (vec![], None);
        };
        let idx = SortedRangeIndex::new(view);
        (
            run_range(
                &idx,
                self.entry_range,
                self.window_range,
                self.bounds,
                &self.window_expr,
                self.tiles.as_ref(),
                self.bucket_granularity,
                exclude_current_row,
            ),
            None,
        )
    }
}

fn run_range(
    idx: &SortedRangeIndex<'_>,
    entry_range: BucketRange,
    window_range: BucketRange,
    bounds: Option<CursorBounds>,
    window_expr: &Arc<dyn WindowExpr>,
    tiles: Option<&Tiles>,
    bucket_granularity: TimeGranularity,
    _exclude_current_row: bool,
) -> Vec<ScalarValue> {
    if idx.is_empty() {
        return vec![];
    }

    let window_frame = window_expr.get_window_frame();
    let is_rows = window_frame.units == WindowFrameUnits::Rows;
    let window_length = get_window_length_ms(window_frame);
    let window_size = get_window_size_rows(window_frame);
    let spec = if is_rows {
        window_logic::WindowSpec::Rows { size: window_size }
    } else {
        window_logic::WindowSpec::Range { length_ms: window_length }
    };

    let mut out: Vec<ScalarValue> = Vec::new();

    let mut emit = |window_end: RowPtr| {
        // Only emit for entry buckets.
        if window_end.bucket_ts < entry_range.start || window_end.bucket_ts > entry_range.end {
            return;
        }

        let unclamped_start = window_logic::window_start_unclamped(idx, window_end, spec);
        let window_start = if unclamped_start.bucket_ts < window_range.start {
            RowPtr::new(window_range.start, 0)
        } else {
            unclamped_start
        };
        out.push(eval_stored_window(window_expr, idx, window_start, window_end, tiles));
    };

    match bounds {
        None => {
            for_each_rowptr_in_range(idx, entry_range, bucket_granularity, |p| emit(p));
        }
        Some(CursorBounds { prev, new }) => {
            for_each_rowptr_in_cursor_delta(idx, prev, new, |p| emit(p));
        }
    }

    out
}

fn for_each_rowptr_in_range(
    idx: &SortedRangeIndex<'_>,
    entry_range: BucketRange,
    bucket_granularity: TimeGranularity,
    mut f: impl FnMut(RowPtr),
) {
    let mut bucket_ts = entry_range.start;
    while bucket_ts <= entry_range.end {
        let rows = idx.bucket_size(bucket_ts);
        for row in 0..rows {
            f(RowPtr::new(bucket_ts, row));
        }
        bucket_ts = bucket_granularity.next_start(bucket_ts);
    }
}

fn for_each_rowptr_in_cursor_delta(
    row_index: &SortedRangeIndex<'_>,
    prev: Option<Cursor>,
    new: Cursor,
    mut f: impl FnMut(RowPtr),
) {
    let Some(mut pos) = window_logic::first_update_pos(row_index, prev) else {
        return;
    };

    let end = match row_index.seek_rowpos_gt(new) {
        Some(after) => match row_index.prev_pos(after) {
            Some(p) => p,
            None => return,
        },
        None => row_index.last_pos(),
    };

    if end < pos {
        return;
    }
    loop {
        f(pos);
        if pos == end {
            break;
        }
        pos = row_index.next_pos(pos).expect("end should be reachable");
    }
}

