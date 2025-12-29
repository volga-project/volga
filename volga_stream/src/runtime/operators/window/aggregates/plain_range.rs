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
    #[allow(dead_code)]
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

    // TODO if we use tiles we should exclude data covered by them
    fn get_data_requests(&self, _exclude_current_row: Option<bool>) -> Vec<DataRequest> {
        let window_frame = self.window_expr.get_window_frame();
        let wl = if window_frame.units == WindowFrameUnits::Range {
            get_window_length_ms(window_frame)
        } else {
            0
        };
        let ws = if window_frame.units == WindowFrameUnits::Rows {
            get_window_size_rows(window_frame)
        } else {
            0
        };

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
    let window_size = if is_rows { get_window_size_rows(window_frame) } else { 0 };
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

#[cfg(test)]
mod tests {
    use arrow::array::RecordBatch;

    use super::*;
    use crate::runtime::operators::window::aggregates::test_utils;
    use crate::runtime::operators::window::index::BucketIndex;
    use crate::runtime::operators::window::index::SortedRangeView;
    use crate::runtime::operators::window::PlainAggregation;
    use crate::runtime::operators::window::tiles::TileConfig;
    use crate::runtime::operators::window::TimeGranularity;
    use crate::storage::batch_store::BatchId;

    fn assert_f64s(vals: &[ScalarValue], expected: &[f64]) {
        assert_eq!(vals.len(), expected.len());
        for (i, (v, e)) in vals.iter().zip(expected.iter()).enumerate() {
            let ScalarValue::Float64(Some(got)) = v else {
                panic!("expected Float64 at {i}, got {v:?}");
            };
            assert!((got - e).abs() < 1e-9, "mismatch at {i}: got={got} expected={e}");
        }
    }

    #[tokio::test]
    async fn test_plain_range_sum_range_window() {
        let sql = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
        let window_expr = test_utils::window_expr_from_sql(sql).await;
        let gran = TimeGranularity::Seconds(1);

        let b1 = test_utils::batch(&[(1000, 10.0, "A", 0), (1500, 30.0, "A", 1)]);
        let b2 = test_utils::batch(&[(2000, 20.0, "A", 2)]);

        let mut bucket_index = BucketIndex::new(gran);
        bucket_index.insert_batch(
            BatchId::new(0, 1000, 0),
            Cursor::new(1000, 0),
            Cursor::new(1500, 1),
            2,
        );
        bucket_index.insert_batch(
            BatchId::new(0, 2000, 0),
            Cursor::new(2000, 2),
            Cursor::new(2000, 2),
            1,
        );

        let entry_range = BucketRange::new(1000, 2000);
        let agg = PlainAggregation::for_range(entry_range, &bucket_index, window_expr.clone(), None, 0, 0, None);
        let requests = agg.get_data_requests(None);
        assert_eq!(requests.len(), 1);

        let view: SortedRangeView =
            test_utils::make_view(gran, requests[0], vec![(1000, b1), (2000, b2)], &window_expr);

        let (vals, _) = agg.produce_aggregates_from_ranges(&[view], None, Some(false)).await;
        assert_f64s(&vals, &[10.0, 40.0, 60.0]);
    }

    #[tokio::test]
    async fn test_plain_range_range_window_uses_tiles() {
        let sql = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '4000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
        let window_expr = test_utils::window_expr_from_sql(sql).await;
        let gran = TimeGranularity::Seconds(1);

        // Dense stored data so Tiles have no gaps for the covered range.
        let rows: Vec<(i64, f64, &str, u64)> = vec![
            (0, 1.0, "A", 0),
            (1000, 2.0, "A", 1),
            (2000, 3.0, "A", 2),
            (3000, 4.0, "A", 3),
            (4000, 5.0, "A", 4),
            (5000, 6.0, "A", 5),
            (6000, 7.0, "A", 6),
        ];
        let all = test_utils::batch(&rows);

        let mut bucket_index = BucketIndex::new(gran);
        let mut buckets: Vec<(i64, RecordBatch)> = Vec::new();
        for (i, (ts, v, pk, seq)) in rows.iter().copied().enumerate() {
            let bucket_ts = gran.start(ts);
            let b = test_utils::batch(&[(ts, v, pk, seq)]);
            bucket_index.insert_batch(
                BatchId::new(0, bucket_ts, i as u64),
                Cursor::new(ts, seq),
                Cursor::new(ts, seq),
                1,
            );
            buckets.push((bucket_ts, b));
        }

        let mut tiles = Tiles::new(
            TileConfig::new(vec![TimeGranularity::Seconds(1)]).expect("tile config"),
            window_expr.clone(),
        );
        tiles.add_batch(&all, 0);

        // Only emit for entry buckets [4000..6000].
        let entry_range = BucketRange::new(4000, 6000);

        let agg_no_tiles =
            PlainAggregation::for_range(entry_range, &bucket_index, window_expr.clone(), None, 0, 0, None);
        let agg_tiles = PlainAggregation::for_range(
            entry_range,
            &bucket_index,
            window_expr.clone(),
            Some(tiles),
            0,
            0,
            None,
        );

        let req1 = agg_no_tiles.get_data_requests(None);
        assert_eq!(req1.len(), 1);
        let view1: SortedRangeView =
            test_utils::make_view(gran, req1[0], buckets.clone(), &window_expr);
        let (vals1, _) = agg_no_tiles.produce_aggregates_from_ranges(&[view1], None, Some(false)).await;

        let req2 = agg_tiles.get_data_requests(None);
        assert_eq!(req2.len(), 1);
        let view2: SortedRangeView =
            test_utils::make_view(gran, req2[0], buckets.clone(), &window_expr);
        let (vals2, _) = agg_tiles.produce_aggregates_from_ranges(&[view2], None, Some(false)).await;

        // At ends 4000/5000/6000 with 4s range:
        // - 4000: 0..4000 => 1+2+3+4+5=15
        // - 5000: 1000..5000 => 2+3+4+5+6=20
        // - 6000: 2000..6000 => 3+4+5+6+7=25
        assert_f64s(&vals1, &[15.0, 20.0, 25.0]);
        assert_eq!(vals1, vals2);
    }
}

