use std::sync::Arc;

use async_trait::async_trait;
use datafusion::logical_expr::WindowFrameUnits;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::index::{get_window_size_rows, window_logic};
use crate::runtime::operators::window::index::{
    get_window_length_ms,
    BucketIndex,
    DataBounds,
    DataRequest,
    SortedRangeIndex,
    SortedRangeView,
};
use crate::runtime::operators::window::tiles::TimeGranularity;
use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::runtime::operators::window::{Cursor, RowPtr};

use super::super::{create_window_aggregator, merge_accumulator_state, WindowAggregator};
use super::{Aggregation, AggregatorType, BucketRange};

#[derive(Debug)]
pub struct RetractableRangeAggregation {
    pub bucket_index: BucketIndex,
    pub update_range: Option<BucketRange>,
    pub retract_range: Option<BucketRange>,
    pub prev_processed_until: Option<Cursor>,
    pub new_processed_until: Cursor,
    pub window_expr: Arc<dyn WindowExpr>,
    pub accumulator_state: Option<AccumulatorState>,
    pub ts_column_index: usize,
    pub window_id: usize,
    pub bucket_granularity: TimeGranularity,
}

impl RetractableRangeAggregation {
    pub fn new(
        window_id: usize,
        prev_processed_until: Option<Cursor>,
        new_processed_until: Cursor,
        bucket_index: &BucketIndex,
        window_expr: Arc<dyn WindowExpr>,
        accumulator_state: Option<AccumulatorState>,
        ts_column_index: usize,
        bucket_granularity: TimeGranularity,
    ) -> Self {
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

        let update_range = bucket_index.delta_span(prev_processed_until, new_processed_until);
        let retract_range = match update_range {
            None => None,
            Some(update_range) => {
                let prev = prev_processed_until.unwrap_or(Cursor::new(i64::MIN, 0));
                if prev.ts == i64::MIN {
                    Some(update_range)
                } else {
                    match window_frame.units {
                        WindowFrameUnits::Range => {
                            let prev_start_ts = prev.ts.saturating_sub(wl);
                            let new_start_ts = new_processed_until.ts.saturating_sub(wl);
                            if new_start_ts <= prev_start_ts {
                                None
                            } else {
                                let start_bucket_ts = bucket_granularity.start(prev_start_ts);
                                let end_bucket_ts =
                                    bucket_granularity.start(new_start_ts.saturating_sub(1));
                                if end_bucket_ts < start_bucket_ts {
                                    None
                                } else {
                                    let buckets =
                                        bucket_index.query_buckets_in_range(start_bucket_ts, end_bucket_ts);
                                    if buckets.is_empty() {
                                        None
                                    } else {
                                        Some(BucketRange::new(
                                            buckets.first().unwrap().timestamp,
                                            buckets.last().unwrap().timestamp,
                                        ))
                                    }
                                }
                            }
                        }
                        WindowFrameUnits::Rows => {
                            let new_start_bucket_ts = update_range.start;
                            let all_ts = bucket_index.bucket_timestamps();
                            let first_ts = all_ts.first().copied().unwrap_or(new_start_bucket_ts);
                            let buckets = bucket_index.query_buckets_in_range(first_ts, new_start_bucket_ts);
                            if buckets.is_empty() {
                                None
                            } else {
                                let mut rows_counted = 0usize;
                                let mut start_ts = new_start_bucket_ts;
                                for b in buckets.iter().rev() {
                                    start_ts = b.timestamp;
                                    rows_counted += b.row_count;
                                    if rows_counted >= ws {
                                        break;
                                    }
                                }
                                Some(BucketRange::new(start_ts, update_range.end))
                            }
                        }
                        _ => Some(update_range),
                    }
                }
            }
        };

        Self {
            bucket_index: bucket_index.clone(),
            update_range,
            retract_range,
            prev_processed_until,
            new_processed_until,
            window_expr,
            accumulator_state,
            ts_column_index,
            window_id,
            bucket_granularity,
        }
    }
}

#[async_trait]
impl Aggregation for RetractableRangeAggregation {
    fn window_expr(&self) -> &Arc<dyn WindowExpr> {
        &self.window_expr
    }

    fn aggregator_type(&self) -> AggregatorType {
        AggregatorType::RetractableAccumulator
    }

    fn get_data_requests(&self) -> Vec<DataRequest> {
        let Some(update) = self.update_range else {
            return vec![];
        };
        let window_frame = self.window_expr.get_window_frame();
        let is_range_window = window_frame.units == WindowFrameUnits::Range;
        let wl = if window_frame.units == WindowFrameUnits::Range {
            get_window_length_ms(window_frame)
        } else {
            0
        };

        let bounds = if is_range_window {
            let start_ts = self
                .prev_processed_until
                .map(|p| p.ts.saturating_sub(wl))
                .unwrap_or(i64::MIN);
            DataBounds::Time {
                start_ts,
                end_ts: self.new_processed_until.ts,
            }
        } else {
            // Streaming ROWS needs all data in the update/retract span (per-row evaluation).
            DataBounds::All
        };

        match self.retract_range {
            None => vec![DataRequest {
                bucket_range: update,
                bounds,
            }],
            Some(retract) => {
                // Edge loading: if disjoint, request two views. If overlapping, request one view.
                if retract.end < update.start || update.end < retract.start {
                    vec![
                        DataRequest {
                            bucket_range: retract,
                            bounds,
                        },
                        DataRequest {
                            bucket_range: update,
                            bounds,
                        },
                    ]
                } else {
                    vec![DataRequest {
                        bucket_range: BucketRange::new(
                            update.start.min(retract.start),
                            update.end.max(retract.end),
                        ),
                        bounds,
                    }]
                }
            }
        }
    }

    async fn produce_aggregates_from_ranges(
        &self,
        sorted_ranges: &[SortedRangeView],
        _thread_pool: Option<&tokio_rayon::rayon::ThreadPool>,
    ) -> (Vec<ScalarValue>, Option<AccumulatorState>) {
        let window_frame = self.window_expr.get_window_frame();
        let is_rows_window = window_frame.units == WindowFrameUnits::Rows;
        let window_length = get_window_length_ms(window_frame);
        let window_size = if is_rows_window { get_window_size_rows(window_frame) } else { 0 };

        let Some(update_range) = self.update_range else {
            return (vec![], self.accumulator_state.clone());
        };
        if sorted_ranges.is_empty() {
            return (vec![], self.accumulator_state.clone());
        }

        let find_view = |r: BucketRange| -> &SortedRangeView {
            sorted_ranges
                .iter()
                .find(|v| {
                    let vr = v.bucket_range();
                    vr.start <= r.start && vr.end >= r.end
                })
                .unwrap_or_else(|| panic!("No SortedRangeView covers bucket_range {r:?}"))
        };

        let updates_view = find_view(update_range);
        let updates = SortedRangeIndex::new_in_bucket_range(updates_view, update_range);
        let retracts = self.retract_range.map(|r| {
            let v = find_view(r);
            SortedRangeIndex::new_in_bucket_range(v, r)
        });

        let (aggs, state) = run_retractable_accumulator(
            &self.window_expr,
            &updates,
            retracts.as_ref(),
            self.prev_processed_until,
            self.new_processed_until,
            self.accumulator_state.as_ref(),
            is_rows_window,
            window_length,
            window_size,
        );
        (aggs, Some(state))
    }
}

fn run_retractable_accumulator(
    window_expr: &Arc<dyn WindowExpr>,
    updates: &SortedRangeIndex<'_>,
    retracts: Option<&SortedRangeIndex<'_>>,
    prev_processed_until: Option<Cursor>,
    new_processed_until: Cursor,
    accumulator_state: Option<&AccumulatorState>,
    is_rows_window: bool,
    window_length: i64,
    window_size: usize,
) -> (Vec<ScalarValue>, AccumulatorState) {
    let mut accumulator = match create_window_aggregator(window_expr) {
        WindowAggregator::Accumulator(accumulator) => accumulator,
        WindowAggregator::Evaluator(_) => {
            panic!("Evaluator is not supported for retractable accumulator")
        }
    };

    if let Some(accumulator_state) = accumulator_state {
        merge_accumulator_state(accumulator.as_mut(), accumulator_state);
    }

    if updates.is_empty() {
        return (
            vec![],
            accumulator
                .state()
                .expect("Should be able to get accumulator state"),
        );
    }

    // Find update start (first row with Cursor > prev_processed_until).
    let update_pos = window_logic::first_update_pos(updates, prev_processed_until);
    let Some(mut update_pos) = update_pos else {
        return (
            vec![],
            accumulator
                .state()
                .expect("Should be able to get accumulator state"),
        );
    };

    // Cap updates by `new_processed_until` (Cursor boundary), not by `updates.last_pos()`.
    let end_pos = {
        let first_row_pos = updates.get_row_pos(&updates.first_pos());
        if new_processed_until < first_row_pos {
            return (
                vec![],
                accumulator
                    .state()
                    .expect("Should be able to get accumulator state"),
            );
        }

        match updates.seek_rowpos_gt(new_processed_until) {
            Some(after_end) => updates
                .prev_pos(after_end)
                .expect("seek_rowpos_gt returned first row; guarded above"),
            None => updates.last_pos(),
        }
    };

    if end_pos < update_pos {
        return (
            vec![],
            accumulator
                .state()
                .expect("Should be able to get accumulator state"),
        );
    }

    let num_updates = updates.count_between(&update_pos, &end_pos);

    let mut results = Vec::with_capacity(num_updates);

    let first_run = prev_processed_until.is_none();

    // RANGE retract pointer.
    let mut range_retract_pos: Option<RowPtr> = None;
    if !is_rows_window {
        if let Some(retracts) = retracts {
            range_retract_pos = if first_run {
                Some(retracts.first_pos())
            } else {
                Some(window_logic::initial_retract_pos_range(
                    retracts,
                    prev_processed_until.expect("Should have previous processed until"),
                    window_length,
                ))
            };
        }
    }

    // ROWS state: retract pointer + current window size (fill tracking).
    let mut rows_window_count: usize = 0;
    let mut rows_retract_pos: Option<RowPtr> = None;
    if is_rows_window && window_size > 0 {
        rows_retract_pos = Some(update_pos);
        if let (Some(prev), Some(retracts)) = (prev_processed_until, retracts) {
            if let Some(processed_pos) = retracts.seek_rowpos_eq(prev) {
                let total_stored = retracts.count_between(&retracts.first_pos(), &processed_pos);
                rows_window_count = total_stored.min(window_size);
                if rows_window_count > 0 {
                    rows_retract_pos = Some(retracts.pos_n_rows(&processed_pos, rows_window_count - 1, true));
                }
            }
        }
    }

    loop {
        let update_args = updates.get_row_args(&update_pos);
        accumulator
            .update_batch(&update_args)
            .expect("Should be able to update accumulator");

        if is_rows_window {
            if window_size > 0 {
                if rows_window_count < window_size {
                    rows_window_count += 1;
                } else if let (Some(retracts), Some(rp)) = (retracts, rows_retract_pos) {
                    // Never retract the current update row on first run (retracts may overlap updates).
                    let update_cursor = updates.get_row_pos(&update_pos);
                    if retracts.get_row_pos(&rp) < update_cursor {
                        let retract_args = retracts.get_row_args(&rp);
                        accumulator
                            .retract_batch(&retract_args)
                            .expect("Should be able to retract from accumulator");
                        rows_retract_pos = retracts.next_pos(rp);
                    }
                }
            }
        } else {
            let update_ts = updates.get_timestamp(&update_pos);
            let window_start = update_ts - window_length;

            if let Some(retracts) = retracts {
                // Retract all rows strictly before the current window start.
                //
                // On first run `retracts` is the same range as `updates`, so we must not retract the
                // current row (it has just been updated into the accumulator).
                while let Some(rp) = range_retract_pos {
                    if first_run && rp == update_pos {
                        break;
                    }
                    if retracts.get_timestamp(&rp) >= window_start {
                        break;
                    }
                    let retract_args = retracts.get_row_args(&rp);
                    accumulator
                        .retract_batch(&retract_args)
                        .expect("Should be able to retract from accumulator");
                    range_retract_pos = retracts.next_pos(rp);
                }
            }
        }

        let result = accumulator
            .evaluate()
            .expect("Should be able to evaluate accumulator");
        results.push(result);

        if update_pos == end_pos {
            break;
        }
        update_pos = updates
            .next_pos(update_pos)
            .expect("end_pos should be reachable from update_pos");
    }

    (
        results,
        accumulator
            .state()
            .expect("Should be able to get accumulator state"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::operators::window::aggregates::test_utils;
    use crate::runtime::operators::window::index::BucketIndex;
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

    fn eval_state(window_expr: &Arc<dyn WindowExpr>, state: &AccumulatorState) -> f64 {
        let mut acc = match create_window_aggregator(window_expr) {
            WindowAggregator::Accumulator(a) => a,
            WindowAggregator::Evaluator(_) => panic!("not supported"),
        };
        merge_accumulator_state(acc.as_mut(), state);
        let ScalarValue::Float64(Some(v)) = acc.evaluate().expect("evaluate failed") else {
            panic!("expected Float64");
        };
        v
    }
    
    #[tokio::test]
    async fn test_retractable_rows_first_run_retracts_within_updates_after_fill() {
        let sql = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
)"#;
        let window_expr = test_utils::window_expr_from_sql(sql).await;
        let gran = TimeGranularity::Seconds(1);

        let b1000 = test_utils::batch(&[
            (1000, 10.0, "A", 0),
            (1100, 20.0, "A", 1),
            (1200, 30.0, "A", 2),
            (1300, 40.0, "A", 3),
        ]);

        let mut bucket_index = BucketIndex::new(gran);
        bucket_index.insert_batch(
            BatchId::new(0, 1000, 0),
            Cursor::new(1000, 0),
            Cursor::new(1300, 3),
            4,
        );

        let new_pos = bucket_index.max_pos_seen();
        let agg = RetractableRangeAggregation::new(
            0,
            None,
            new_pos,
            &bucket_index,
            window_expr.clone(),
            None,
            0,
            gran,
        );

        let reqs = agg.get_data_requests();
        assert_eq!(reqs.len(), 1);
        let view = test_utils::make_view(gran, reqs[0], vec![(1000, b1000)], &window_expr);

        let (vals, state) = agg.produce_aggregates_from_ranges(&[view], None).await;
        assert_f64s(&vals, &[10.0, 30.0, 60.0, 90.0]);
        let state = state.expect("state");
        assert!((eval_state(&window_expr, &state) - 90.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_retractable_rows_multi_update_step_retracts_across_buckets() {
        let sql = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
)"#;
        let window_expr = test_utils::window_expr_from_sql(sql).await;
        let gran = TimeGranularity::Seconds(1);

        let mut bucket_index = BucketIndex::new(gran);

        // Step 1: 3 rows (fills the window).
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

        let b1000 = test_utils::batch(&[(1000, 10.0, "A", 0), (1500, 20.0, "A", 1)]);
        let b2000 = test_utils::batch(&[(2000, 30.0, "A", 2)]);

        let new1 = bucket_index.max_pos_seen();
        let agg1 = RetractableRangeAggregation::new(
            0,
            None,
            new1,
            &bucket_index,
            window_expr.clone(),
            None,
            0,
            gran,
        );
        let reqs1 = agg1.get_data_requests();
        assert_eq!(reqs1.len(), 1);
        let view1 = test_utils::make_view(gran, reqs1[0], vec![(1000, b1000), (2000, b2000)], &window_expr);

        let (vals1, state1) = agg1.produce_aggregates_from_ranges(&[view1], None).await;
        assert_f64s(&vals1, &[10.0, 30.0, 60.0]);
        let state1 = state1.expect("state1");

        // Step 2: 4 more rows in one slide step.
        bucket_index.insert_batch(
            BatchId::new(0, 2000, 1),
            Cursor::new(2500, 3),
            Cursor::new(2500, 3),
            1,
        );
        bucket_index.insert_batch(
            BatchId::new(0, 3000, 0),
            Cursor::new(3000, 4),
            Cursor::new(3500, 5),
            2,
        );
        bucket_index.insert_batch(
            BatchId::new(0, 4000, 0),
            Cursor::new(4000, 6),
            Cursor::new(4000, 6),
            1,
        );

        let b2000_all = test_utils::batch(&[(2000, 30.0, "A", 2), (2500, 40.0, "A", 3)]);
        let b3000 = test_utils::batch(&[(3000, 50.0, "A", 4), (3500, 60.0, "A", 5)]);
        let b4000 = test_utils::batch(&[(4000, 70.0, "A", 6)]);

        let prev = Some(new1);
        let new2 = bucket_index.max_pos_seen();
        let agg2 = RetractableRangeAggregation::new(
            0,
            prev,
            new2,
            &bucket_index,
            window_expr.clone(),
            Some(state1.clone()),
            0,
            gran,
        );

        // For ROWS our planning creates overlapping retract/update candidate spans, so we should
        // still get a single request (union).
        let reqs2 = agg2.get_data_requests();
        assert_eq!(reqs2.len(), 1);

        let view2 = test_utils::make_view(
            gran,
            reqs2[0],
            vec![
                (1000, test_utils::batch(&[(1000, 10.0, "A", 0), (1500, 20.0, "A", 1)])),
                (2000, b2000_all),
                (3000, b3000),
                (4000, b4000),
            ],
            &window_expr,
        );

        let (vals2, state2) = agg2.produce_aggregates_from_ranges(&[view2], None).await;
        assert_f64s(&vals2, &[90.0, 120.0, 150.0, 180.0]);
        let state2 = state2.expect("state2");
        assert!((eval_state(&window_expr, &state2) - 180.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_retractable_range_sum_range_window_two_steps() {
        let sql = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
        let window_expr = test_utils::window_expr_from_sql(sql).await;
        let gran = TimeGranularity::Seconds(1);

        // Step 1: rows up to t=2000.
        let b1000 = test_utils::batch(&[(1000, 10.0, "A", 0), (1500, 30.0, "A", 1)]);
        let b2000 = test_utils::batch(&[(2000, 20.0, "A", 2)]);

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

        let new1 = bucket_index.max_pos_seen();
        let agg1 = RetractableRangeAggregation::new(
            0,
            None,
            new1,
            &bucket_index,
            window_expr.clone(),
            None,
            0,
            gran,
        );
        let reqs1 = agg1.get_data_requests();
        assert_eq!(reqs1.len(), 1);
        let view1 =
            test_utils::make_view(gran, reqs1[0], vec![(1000, b1000), (2000, b2000)], &window_expr);
        let (vals1, state1) = agg1.produce_aggregates_from_ranges(&[view1], None).await;
        assert_f64s(&vals1, &[10.0, 40.0, 60.0]);
        let state1 = state1.expect("state1");
        assert!((eval_state(&window_expr, &state1) - 60.0).abs() < 1e-9);

        // Step 2: add t=3200 and slide again from prev processed.
        let b3000 = test_utils::batch(&[(3200, 5.0, "A", 3)]);
        bucket_index.insert_batch(
            BatchId::new(0, 3000, 0),
            Cursor::new(3200, 3),
            Cursor::new(3200, 3),
            1,
        );

        let prev = Some(new1);
        let new2 = bucket_index.max_pos_seen();
        let agg2 = RetractableRangeAggregation::new(
            0,
            prev,
            new2,
            &bucket_index,
            window_expr.clone(),
            Some(state1.clone()),
            0,
            gran,
        );
        let reqs2 = agg2.get_data_requests();
        assert_eq!(reqs2.len(), 2, "expected edge loading (retract + update)");
        let view2_retract = test_utils::make_view(
            gran,
            reqs2[0],
            vec![
                (
                    1000,
                    test_utils::batch(&[(1000, 10.0, "A", 0), (1500, 30.0, "A", 1)]),
                ),
                (2000, test_utils::batch(&[(2000, 20.0, "A", 2)])),
            ],
            &window_expr,
        );
        let view2_update =
            test_utils::make_view(gran, reqs2[1], vec![(3000, b3000)], &window_expr);

        let (vals2, state2) = agg2
            .produce_aggregates_from_ranges(&[view2_retract, view2_update], None)
            .await;
        assert_f64s(&vals2, &[55.0]);
        let state2 = state2.expect("state2");
        assert!((eval_state(&window_expr, &state2) - 55.0).abs() < 1e-9);
    }
}

