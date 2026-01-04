use std::sync::Arc;

use async_trait::async_trait;
use datafusion::logical_expr::WindowFrameUnits;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use tokio_rayon::rayon::ThreadPool;
use crate::runtime::operators::window::aggregates::{Aggregation, BucketRange};
use crate::runtime::operators::window::state::index::{BucketIndex, get_window_length_ms, get_window_size_rows};
use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::runtime::operators::window::{RowPtr, Tiles, WindowAggregator, create_window_aggregator};
use crate::storage::batch_store::Timestamp;

use crate::runtime::operators::window::aggregates::VirtualPoint;
use crate::runtime::operators::window::state::index::{DataBounds, DataRequest, SortedRangeIndex, SortedRangeView};
use crate::runtime::operators::window::Cursor;
use crate::runtime::operators::window::aggregates::point_request_merge::{merge_planned_ranges, PlannedRange};
use crate::runtime::operators::window::state::tiles::Tile;

#[derive(Debug, Clone)]
enum PointDataPlan {
    Whole { req_idx: usize },
    RangeGaps { gaps: Vec<GapSlice>, tiles: Vec<Tile> },
}

#[derive(Debug, Clone)]
struct GapSlice {
    req_idx: usize,
    start_ts: Timestamp,
    end_ts: Timestamp,
}

#[derive(Debug, Clone)]
struct PointPlan {
    point: VirtualPoint,
    plan: PointDataPlan,
}

#[derive(Debug)]
pub struct PlainPointsAggregation {
    plans: Vec<PointPlan>,
    data_requests: Vec<DataRequest>,
    window_expr: Arc<dyn WindowExpr>,
    exclude_current_row: bool,
}

impl PlainPointsAggregation {

    fn tiles_strictly_inside(tiles: &Tiles, start_ts: Timestamp, end_ts: Timestamp) -> Vec<Tile> {
        let mut tiles_inside = tiles.get_tiles_for_range(start_ts, end_ts);
        tiles_inside.retain(|t| t.tile_start > start_ts && t.tile_end <= end_ts);
        tiles_inside.sort_by_key(|t| (t.tile_start, t.tile_end));
        tiles_inside
    }

    /// Raw timestamp gaps excluding tile-covered interior.
    ///
    /// Returned gaps are inclusive ranges `[start_ts..=end_ts]`.
    fn raw_gaps_excluding_tiles(
        start_ts: Timestamp,
        end_ts: Timestamp,
        tiles_inside: &[Tile],
    ) -> Vec<(Timestamp, Timestamp)> {
        if start_ts > end_ts {
            return Vec::new();
        }
        if tiles_inside.is_empty() {
            return vec![(start_ts, end_ts)];
        }

        let mut gaps: Vec<(Timestamp, Timestamp)> = Vec::new();
        let mut cur = start_ts;
        for t in tiles_inside {
            // Exclude tile_start itself (duplicates may exist at boundaries).
            let gap_end = t.tile_start.saturating_sub(1);
            if cur <= gap_end {
                gaps.push((cur, gap_end));
            }
            // Include tile_end (tiles are half-open [start,end)).
            cur = cur.max(t.tile_end);
        }
        if cur <= end_ts {
            gaps.push((cur, end_ts));
        }
        gaps
    }

    fn plan_range_points(
        points: Vec<VirtualPoint>,
        bucket_index: &BucketIndex,
        bucket_granularity: crate::runtime::operators::window::TimeGranularity,
        window_length_ms: i64,
        tiles: Option<&Tiles>,
    ) -> (Vec<PointPlan>, Vec<DataRequest>) {
        #[derive(Clone)]
        struct TmpPointGaps {
            point: VirtualPoint,
            tiles_inside: Vec<Tile>,
            gaps: Vec<(usize, Timestamp, Timestamp)>, // (orig_gap_idx, start_ts, end_ts_inclusive)
        }

        let mut planned_gaps: Vec<PlannedRange> = Vec::new();
        let mut tmp: Vec<TmpPointGaps> = Vec::with_capacity(points.len());
        let mut next_orig_gap_idx: usize = 0;

        for p in points.into_iter() {
            let start_ts = p.ts.saturating_sub(window_length_ms);
            let end_ts = p.ts;
            let bucket_ts = bucket_granularity.start(p.ts);
            let update = BucketRange::new(bucket_ts, bucket_ts);
            let window_range = bucket_index.bucket_span_for_range_window(update, window_length_ms);

            let tiles_inside = tiles
                .map(|t| Self::tiles_strictly_inside(t, start_ts, end_ts))
                .unwrap_or_default();
            let raw_gaps = Self::raw_gaps_excluding_tiles(start_ts, end_ts, &tiles_inside);

            let mut gaps: Vec<(usize, Timestamp, Timestamp)> = Vec::new();
            for (gs, ge) in raw_gaps {
                gaps.push((next_orig_gap_idx, gs, ge));

                let br_start = bucket_granularity.start(gs);
                let br_end = bucket_granularity.start(ge);
                let br = BucketRange::new(
                    br_start.max(window_range.start),
                    br_end.min(window_range.end),
                );
                if br.start <= br.end {
                    planned_gaps.push(PlannedRange {
                        orig_idx: next_orig_gap_idx,
                        bucket_range: br,
                        bounds: DataBounds::Time {
                            start_ts: gs,
                            end_ts: ge,
                        },
                        sort_ts: p.ts,
                    });
                }
                next_orig_gap_idx += 1;
            }

            tmp.push(TmpPointGaps {
                point: p,
                tiles_inside,
                gaps,
            });
        }

        let (data_requests, req_idx_by_gap_orig) =
            merge_planned_ranges(bucket_granularity, next_orig_gap_idx, planned_gaps);

        let plans: Vec<PointPlan> = tmp
            .into_iter()
            .map(|t| {
                let mut gap_slices: Vec<GapSlice> = Vec::new();
                for (orig_gap_idx, s, e) in t.gaps {
                    let Some(req_idx) = req_idx_by_gap_orig
                        .get(orig_gap_idx)
                        .copied()
                        .flatten()
                    else {
                        continue;
                    };
                    gap_slices.push(GapSlice {
                        req_idx,
                        start_ts: s,
                        end_ts: e,
                    });
                }
                PointPlan {
                    point: t.point,
                    plan: PointDataPlan::RangeGaps {
                        gaps: gap_slices,
                        tiles: t.tiles_inside,
                    },
                }
            })
            .collect();

        (plans, data_requests)
    }

    fn plan_rows_points(
        points: Vec<VirtualPoint>,
        bucket_index: &BucketIndex,
        bucket_granularity: crate::runtime::operators::window::TimeGranularity,
        window_size_rows: usize,
    ) -> (Vec<PointPlan>, Vec<DataRequest>) {
        #[derive(Clone)]
        struct TmpByOrig {
            window_range: BucketRange,
            bounds: DataBounds,
            point: VirtualPoint,
        }

        let mut tmp_by_orig: Vec<TmpByOrig> = Vec::with_capacity(points.len());
        for p in points.into_iter() {
            let bucket_ts = bucket_granularity.start(p.ts);
            let update = BucketRange::new(bucket_ts, bucket_ts);
            let window_range = bucket_index.bucket_span_for_rows_window(update, window_size_rows);
            tmp_by_orig.push(TmpByOrig {
                window_range,
                bounds: DataBounds::All,
                point: p,
            });
        }

        let planned: Vec<PlannedRange> = tmp_by_orig
            .iter()
            .enumerate()
            .map(|(orig_idx, t)| PlannedRange {
                orig_idx,
                bucket_range: t.window_range,
                bounds: t.bounds,
                sort_ts: t.point.ts,
            })
            .collect();

        let (data_requests, req_idx_by_orig) =
            merge_planned_ranges(bucket_granularity, tmp_by_orig.len(), planned);

        let plans: Vec<PointPlan> = tmp_by_orig
            .into_iter()
            .enumerate()
            .map(|(orig_idx, t)| {
                let req_idx = req_idx_by_orig[orig_idx].expect("req idx must exist");
                PointPlan {
                    point: t.point,
                    plan: PointDataPlan::Whole { req_idx },
                }
            })
            .collect();

        (plans, data_requests)
    }

    pub fn new(
        points: Vec<VirtualPoint>,
        bucket_index: &BucketIndex,
        window_expr: Arc<dyn WindowExpr>,
        tiles: Option<Tiles>,
        exclude_current_row: bool,
    ) -> Self {
        let window_frame = window_expr.get_window_frame();
        let bucket_granularity = bucket_index.bucket_granularity();
        let wl = get_window_length_ms(window_frame);
        let ws = if window_frame.units == WindowFrameUnits::Rows {
            get_window_size_rows(window_frame)
        } else {
            0
        };

        let (plans, data_requests) = match window_frame.units {
            WindowFrameUnits::Range => Self::plan_range_points(
                points,
                bucket_index,
                bucket_granularity,
                wl,
                tiles.as_ref(),
            ),
            WindowFrameUnits::Rows => Self::plan_rows_points(points, bucket_index, bucket_granularity, ws),
            _ => Self::plan_rows_points(points, bucket_index, bucket_granularity, 0),
        };

        Self {
            plans,
            data_requests,
            window_expr,
            exclude_current_row,
        }
    }
}

#[async_trait]
impl Aggregation for PlainPointsAggregation {
    fn window_expr(&self) -> &Arc<dyn WindowExpr> {
        &self.window_expr
    }

    fn aggregator_type(&self) -> crate::runtime::operators::window::AggregatorType {
        crate::runtime::operators::window::AggregatorType::PlainAccumulator
    }

    // TODO move data requests gen logic here, not need to have self.data_requests
    fn get_data_requests(&self) -> Vec<DataRequest> {
        self.data_requests.clone()
    }

    async fn produce_aggregates_from_ranges(
        &self,
        sorted_ranges: &[SortedRangeView],
        _thread_pool: Option<&ThreadPool>,
    ) -> (Vec<ScalarValue>, Option<AccumulatorState>) {
        let include_virtual = !self.exclude_current_row;

        let window_frame = self.window_expr.get_window_frame();
        let _window_length = get_window_length_ms(window_frame);
        let window_size = if window_frame.units == WindowFrameUnits::Rows {
            get_window_size_rows(window_frame)
        } else {
            0
        };

        let indices: Vec<SortedRangeIndex<'_>> = sorted_ranges
            .iter()
            .map(SortedRangeIndex::new)
            .collect();

        let mut out: Vec<ScalarValue> = Vec::with_capacity(self.plans.len());
        for plan in &self.plans {
            let mut accumulator = match create_window_aggregator(&self.window_expr) {
                WindowAggregator::Accumulator(accumulator) => accumulator,
                WindowAggregator::Evaluator(_) => panic!("PlainAggregation should not use evaluator"),
            };

            match (&plan.plan, window_frame.units) {
                (PointDataPlan::RangeGaps { gaps, tiles }, WindowFrameUnits::Range) => {
                    // Update from raw gaps only (no tile-covered interior loaded).
                    for g in gaps {
                        let idx = &indices[g.req_idx];
                        if idx.is_empty() {
                            continue;
                        }
                        let start = idx.seek_ts_ge(g.start_ts);
                        let end = last_row_le_ts_in_range(idx, g.end_ts);
                        if let (Some(start), Some(end)) = (start, end) {
                            if start <= end {
                                let args = idx.get_args_in_range(&start, &end);
                                if !args.is_empty() {
                                    accumulator.update_batch(&args).expect("update_batch failed");
                                }
                            }
                        }
                    }
                    // Merge tile accumulator state for tiles strictly inside the window.
                    for t in tiles {
                        if let Some(tile_state) = &t.accumulator_state {
                            crate::runtime::operators::window::aggregates::merge_accumulator_state(
                                accumulator.as_mut(),
                                tile_state.as_ref(),
                            );
                        }
                    }
                }
                (PointDataPlan::Whole { req_idx }, WindowFrameUnits::Rows) => {
                    let idx = &indices[*req_idx];
                    if !idx.is_empty() {
                        let Some(end) = last_row_le_ts_in_range(idx, plan.point.ts) else {
                            // no stored rows
                            goto_virtual(&mut *accumulator, include_virtual, &plan.point);
                            out.push(accumulator.evaluate().expect("evaluate failed"));
                            continue;
                        };
                        let stored_rows = if include_virtual {
                            window_size.saturating_sub(1)
                        } else {
                            window_size
                        };
                        if stored_rows > 0 {
                            let start = idx.pos_n_rows(&end, stored_rows.saturating_sub(1), true);
                            if start <= end {
                                let args = idx.get_args_in_range(&start, &end);
                                if !args.is_empty() {
                                    accumulator.update_batch(&args).expect("update_batch failed");
                                }
                            }
                        }
                    }
                }
                _ => {}
            };

            goto_virtual(&mut *accumulator, include_virtual, &plan.point);
            out.push(accumulator.evaluate().expect("evaluate failed"));
        }

        (out, None)
    }
}

fn goto_virtual(acc: &mut dyn datafusion::logical_expr::Accumulator, include_virtual: bool, p: &VirtualPoint) {
    if include_virtual {
        if let Some(args) = p.args.as_ref() {
            acc.update_batch(args.as_ref()).expect("update_batch failed");
        }
    }
}

fn last_row_le_ts_in_range(idx: &SortedRangeIndex<'_>, ts: Timestamp) -> Option<RowPtr> {
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

#[cfg(test)]
mod tests {
    use arrow::array::RecordBatch;

    use super::*;
    use crate::runtime::operators::window::aggregates::test_utils;
    use crate::runtime::operators::window::state::index::BucketIndex;
    use crate::runtime::operators::window::state::tiles::TileConfig;
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
    async fn test_plain_points_sum_range_window_includes_virtual() {
        let sql = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
        let window_expr = test_utils::window_expr_from_sql(sql).await;
        let gran = TimeGranularity::Seconds(1);

        // Stored data: one row at t=1000.
        let stored = test_utils::batch(&[(1000, 10.0, "A", 0)]);
        let mut bucket_index = BucketIndex::new(gran);
        bucket_index.insert_batch(
            BatchId::new(0, 1000, 0),
            Cursor::new(1000, 0),
            Cursor::new(1000, 0),
            1,
        );

        let p1_args = window_expr
            .evaluate_args(&test_utils::one_row_batch(1500, 30.0, "A", 1))
            .expect("eval args");
        let p2_args = window_expr
            .evaluate_args(&test_utils::one_row_batch(2000, 20.0, "A", 2))
            .expect("eval args");
        let points = vec![
            VirtualPoint { ts: 1500, args: Some(Arc::new(p1_args)) },
            VirtualPoint { ts: 2000, args: Some(Arc::new(p2_args)) },
        ];

        let agg = crate::runtime::operators::window::aggregates::plain::PlainAggregation::for_points(
            points,
            &bucket_index,
            window_expr.clone(),
            None,
            false,
        );
        let requests = agg.get_data_requests();
        let views: Vec<_> = requests
            .iter()
            .map(|r| test_utils::make_view(gran, *r, vec![(1000, stored.clone())], &window_expr))
            .collect();

        let (vals, _) = agg.produce_aggregates_from_ranges(&views, None).await;
        assert_f64s(&vals, &[40.0, 30.0]);
    }

    #[tokio::test]
    async fn test_plain_points_sum_rows_window_includes_virtual() {
        let sql = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_3
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
)"#;
        let window_expr = test_utils::window_expr_from_sql(sql).await;
        let gran = TimeGranularity::Seconds(1);

        // Stored rows: t=1000,1500.
        let stored = test_utils::batch(&[(1000, 10.0, "A", 0), (1500, 30.0, "A", 1)]);
        let mut bucket_index = BucketIndex::new(gran);
        bucket_index.insert_batch(
            BatchId::new(0, 1000, 0),
            Cursor::new(1000, 0),
            Cursor::new(1500, 1),
            2,
        );

        let p_args = window_expr
            .evaluate_args(&test_utils::one_row_batch(2000, 20.0, "A", 2))
            .expect("eval args");
        let points = vec![VirtualPoint { ts: 2000, args: Some(Arc::new(p_args)) }];

        let agg = crate::runtime::operators::window::aggregates::plain::PlainAggregation::for_points(
            points,
            &bucket_index,
            window_expr.clone(),
            None,
            false,
        );
        let requests = agg.get_data_requests();
        let views: Vec<_> = requests
            .iter()
            .map(|r| test_utils::make_view(gran, *r, vec![(1000, stored.clone())], &window_expr))
            .collect();

        let (vals, _) = agg.produce_aggregates_from_ranges(&views, None).await;
        assert_f64s(&vals, &[60.0]);
    }

    #[tokio::test]
    async fn test_plain_points_range_window_uses_tiles() {
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
        tiles.add_batch(&all, &window_expr, 0);

        let p_args = window_expr
            .evaluate_args(&test_utils::one_row_batch(6500, 10.0, "A", 99))
            .expect("eval args");
        let points = vec![VirtualPoint {
            ts: 6500,
            args: Some(Arc::new(p_args)),
        }];

        let agg_no_tiles = crate::runtime::operators::window::aggregates::plain::PlainAggregation::for_points(
            points.clone(),
            &bucket_index,
            window_expr.clone(),
            None,
            false,
        );
        let agg_tiles = crate::runtime::operators::window::aggregates::plain::PlainAggregation::for_points(
            points,
            &bucket_index,
            window_expr.clone(),
            Some(tiles),
            false,
        );

        let reqs1 = agg_no_tiles.get_data_requests();
        assert_eq!(reqs1.len(), 1);
        let views1: Vec<_> = reqs1
            .iter()
            .map(|r| test_utils::make_view(gran, *r, buckets.clone(), &window_expr))
            .collect();
        let (vals1, _) = agg_no_tiles
            .produce_aggregates_from_ranges(&views1, None)
            .await;

        let reqs2 = agg_tiles.get_data_requests();
        // With tiles, we only load raw gaps at the window boundaries.
        assert_eq!(reqs2.len(), 2);
        let views2: Vec<_> = reqs2
            .iter()
            .map(|r| test_utils::make_view(gran, *r, buckets.clone(), &window_expr))
            .collect();
        let (vals2, _) = agg_tiles
            .produce_aggregates_from_ranges(&views2, None)
            .await;

        // Stored rows in [2500..6500] are ts=3000,4000,5000,6000 => 4+5+6+7=22; plus virtual 10 => 32.
        assert_f64s(&vals1, &[32.0]);
        assert_eq!(vals1, vals2);
    }

    #[tokio::test]
    async fn test_plain_points_range_window_planner_without_tiles_is_stable() {
        let sql = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '4000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
        let window_expr = test_utils::window_expr_from_sql(sql).await;
        let gran = TimeGranularity::Seconds(1);

        // Sparse stored data.
        let rows: Vec<(i64, f64, &str, u64)> = vec![
            (1000, 2.0, "A", 1),
            (3000, 4.0, "A", 3),
            (6000, 7.0, "A", 6),
        ];
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

        let p_args = window_expr
            .evaluate_args(&test_utils::one_row_batch(6500, 10.0, "A", 99))
            .expect("eval args");
        let points = vec![VirtualPoint {
            ts: 6500,
            args: Some(Arc::new(p_args)),
        }];

        let agg_no_tiles = crate::runtime::operators::window::aggregates::plain::PlainAggregation::for_points(
            points,
            &bucket_index,
            window_expr.clone(),
            None,
            false,
        );
        let reqs = agg_no_tiles.get_data_requests();
        assert_eq!(reqs.len(), 1);
        assert!(
            matches!(reqs[0].bounds, DataBounds::Time { .. }),
            "range windows should use time bounds"
        );

        let views: Vec<_> = reqs
            .iter()
            .map(|r| test_utils::make_view(gran, *r, buckets.clone(), &window_expr))
            .collect();
        let (vals, _) = agg_no_tiles.produce_aggregates_from_ranges(&views, None).await;

        // Stored rows in [2500..6500] are ts=3000,6000 => 4+7=11; plus virtual 10 => 21.
        assert_f64s(&vals, &[21.0]);
    }
}