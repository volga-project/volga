use std::sync::Arc;

use async_trait::async_trait;
use datafusion::logical_expr::WindowFrameUnits;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use std::collections::HashMap;

use crate::runtime::operators::window::aggregates::{Aggregation, BucketRange};
use crate::runtime::operators::window::index::{
    BucketIndex, DataBounds, DataRequest, SortedRangeBucket, SortedRangeIndex, SortedRangeView,
    get_window_length_ms, get_window_size_rows, window_logic,
};
use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::runtime::operators::window::{Cursor, Tiles, TimeGranularity};

use super::{CursorBounds, eval_stored_window};

fn build_data_requests(
    bucket_index: &BucketIndex,
    window_expr: &Arc<dyn WindowExpr>,
    tiles: Option<&Tiles>,
    bounds: CursorBounds,
    bucket_granularity: TimeGranularity,
) -> Vec<DataRequest> {
    let CursorBounds { prev, new } = bounds;
    let Some(entry_span) = bucket_index.delta_span(prev, new) else {
        return vec![];
    };

    let window_frame = window_expr.get_window_frame();
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

    let window_span = match window_frame.units {
        WindowFrameUnits::Range => bucket_index.bucket_span_for_range_window(entry_span, wl),
        WindowFrameUnits::Rows => bucket_index.bucket_span_for_rows_window(entry_span, ws),
        _ => entry_span,
    };

    let req_bounds = if window_frame.units == WindowFrameUnits::Range {
        let start_ts = prev.map(|p| p.ts.saturating_sub(wl)).unwrap_or(i64::MIN);
        DataBounds::Time {
            start_ts,
            end_ts: new.ts,
        }
    } else {
        DataBounds::All
    };

    if window_frame.units == WindowFrameUnits::Range {
        if let (DataBounds::Time { start_ts, end_ts }, Some(tiles)) = (req_bounds, tiles) {
            // For streaming plain range we must *always* load the update span buckets to emit per-row outputs.
            // Tiles can only reduce history IO, not the update buckets themselves.
            let mut reqs: Vec<DataRequest> = vec![DataRequest {
                bucket_range: entry_span,
                bounds: DataBounds::All,
            }];

            if start_ts != i64::MIN {
                let (_tiles_inside, gaps) = tiles.coverage_gaps(start_ts, end_ts);
                for (gs, ge) in gaps {
                    if gs >= ge {
                        continue;
                    }
                    let br_start = bucket_granularity.start(gs);
                    let br_end = bucket_granularity.start(ge);
                    let br = BucketRange::new(br_start.max(window_span.start), br_end.min(window_span.end));
                    if br.start <= br.end {
                        reqs.push(DataRequest {
                            bucket_range: br,
                            bounds: DataBounds::Time {
                                start_ts: gs,
                                end_ts: ge,
                            },
                        });
                    }
                }
            }

            reqs.sort_by_key(|r| (r.bucket_range.start, r.bucket_range.end));
            reqs.dedup_by_key(|r| (r.bucket_range.start, r.bucket_range.end));
            return reqs;
        }
    }

    vec![DataRequest {
        bucket_range: window_span,
        bounds: req_bounds,
    }]
}

#[derive(Debug)]
pub struct PlainRangeAggregation {
    bounds: CursorBounds,
    window_expr: Arc<dyn WindowExpr>,
    tiles: Option<Tiles>,
    #[allow(dead_code)]
    window_id: usize,
    bucket_granularity: TimeGranularity,
    data_requests: Vec<DataRequest>,
}

impl PlainRangeAggregation {
    pub fn new(
        bucket_index: &BucketIndex,
        window_expr: Arc<dyn WindowExpr>,
        tiles: Option<Tiles>,
        _ts_column_index: usize,
        window_id: usize,
        bounds: CursorBounds,
    ) -> Self {
        let bucket_granularity = bucket_index.bucket_granularity();
        let data_requests = build_data_requests(bucket_index, &window_expr, tiles.as_ref(), bounds, bucket_granularity);
        Self {
            bounds,
            window_expr,
            tiles,
            window_id,
            bucket_granularity,
            data_requests,
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

    fn get_data_requests(&self) -> Vec<DataRequest> {
        self.data_requests.clone()
    }

    async fn produce_aggregates_from_ranges(
        &self,
        sorted_ranges: &[SortedRangeView],
        _thread_pool: Option<&tokio_rayon::rayon::ThreadPool>,
    ) -> (Vec<ScalarValue>, Option<AccumulatorState>) {
        let Some(first_view) = sorted_ranges.first() else {
            return (vec![], None);
        };

        let view = if sorted_ranges.len() == 1 {
            first_view.clone()
        } else {
            // Merge buckets from multiple views (update span + raw gaps) into a single logical view.
            let mut buckets: HashMap<i64, SortedRangeBucket> = HashMap::new();
            let mut min_ts: Option<i64> = None;
            let mut max_ts: Option<i64> = None;
            for v in sorted_ranges {
                for (ts, b) in v.buckets().iter() {
                    min_ts = Some(min_ts.map(|m| m.min(*ts)).unwrap_or(*ts));
                    max_ts = Some(max_ts.map(|m| m.max(*ts)).unwrap_or(*ts));
                    buckets.insert(*ts, b.clone());
                }
            }

            let req = DataRequest {
                bucket_range: BucketRange::new(
                    min_ts.unwrap_or(first_view.bucket_range().start),
                    max_ts.unwrap_or(first_view.bucket_range().end),
                ),
                bounds: DataBounds::All,
            };
            SortedRangeView::new(
                req,
                first_view.bucket_granularity(),
                Cursor::new(i64::MIN, 0),
                Cursor::new(i64::MAX, u64::MAX),
                buckets,
            )
        };

        let idx = SortedRangeIndex::new(&view);
        if idx.is_empty() {
            return (vec![], None);
        }

        let CursorBounds { prev, new } = self.bounds;
        let Some(mut pos) = window_logic::first_update_pos(&idx, prev) else {
            return (vec![], None);
        };

        let end_pos = {
            let first_row_pos = idx.get_row_pos(&idx.first_pos());
            if new < first_row_pos {
                return (vec![], None);
            }
            match idx.seek_rowpos_gt(new) {
                Some(after_end) => idx
                    .prev_pos(after_end)
                    .expect("seek_rowpos_gt returned first row; guarded above"),
                None => idx.last_pos(),
            }
        };

        if end_pos < pos {
            return (vec![], None);
        }

        let window_frame = self.window_expr.get_window_frame();
        let is_rows = window_frame.units == WindowFrameUnits::Rows;
        let window_length = get_window_length_ms(window_frame);
        let window_size = if is_rows { get_window_size_rows(window_frame) } else { 0 };
        let spec = if is_rows {
            window_logic::WindowSpec::Rows { size: window_size }
        } else {
            window_logic::WindowSpec::Range { length_ms: window_length }
        };

        let mut out: Vec<ScalarValue> = Vec::new();
        loop {
            let start = window_logic::window_start_unclamped(&idx, pos, spec);
            let v = eval_stored_window(
                &self.window_expr,
                &idx,
                start,
                pos,
                self.tiles.as_ref(),
            );
            out.push(v);

            if pos == end_pos {
                break;
            }
            pos = idx.next_pos(pos).expect("end_pos should be reachable");
        }

        (out, None)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::RecordBatch;

    use super::*;
    use crate::runtime::operators::window::Cursor;
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

        let bounds = crate::runtime::operators::window::aggregates::plain::CursorBounds {
            prev: None,
            new: Cursor::new(2000, 2),
        };
        let agg = PlainAggregation::for_range(&bucket_index, window_expr.clone(), None, 0, 0, bounds);
        let requests = agg.get_data_requests();
        assert_eq!(requests.len(), 1);

        let view: SortedRangeView =
            test_utils::make_view(gran, requests[0], vec![(1000, b1), (2000, b2)], &window_expr);

        let (vals, _) = agg.produce_aggregates_from_ranges(&[view], None).await;
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
        );
        tiles.add_batch(&all, &window_expr,0);

        let bounds = crate::runtime::operators::window::aggregates::plain::CursorBounds {
            prev: Some(Cursor::new(3000, 3)),
            new: Cursor::new(6000, 6),
        };

        let agg_no_tiles =
            PlainAggregation::for_range(&bucket_index, window_expr.clone(), None, 0, 0, bounds);
        let agg_tiles =
            PlainAggregation::for_range(&bucket_index, window_expr.clone(), Some(tiles), 0, 0, bounds);

        let req1 = agg_no_tiles.get_data_requests();
        assert_eq!(req1.len(), 1);
        let view1: SortedRangeView =
            test_utils::make_view(gran, req1[0], buckets.clone(), &window_expr);
        let (vals1, _) = agg_no_tiles.produce_aggregates_from_ranges(&[view1], None).await;

        let req2 = agg_tiles.get_data_requests();
        assert_eq!(req2.len(), 2, "tiles should reduce history IO into separate requests");
        let views2: Vec<SortedRangeView> = req2
            .iter()
            .map(|r| test_utils::make_view(gran, *r, buckets.clone(), &window_expr))
            .collect();
        let (vals2, _) = agg_tiles.produce_aggregates_from_ranges(&views2, None).await;

        // At ends 4000/5000/6000 with 4s range:
        // - 4000: 0..4000 => 1+2+3+4+5=15
        // - 5000: 1000..5000 => 2+3+4+5+6=20
        // - 6000: 2000..6000 => 3+4+5+6+7=25
        assert_f64s(&vals1, &[15.0, 20.0, 25.0]);
        assert_eq!(vals1, vals2);
    }
}

