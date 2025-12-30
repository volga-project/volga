use std::sync::Arc;

use datafusion::logical_expr::WindowFrameUnits;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use tokio_rayon::rayon::ThreadPool;

use crate::runtime::operators::window::index::{get_window_length_ms, get_window_size_rows, BucketIndex};
use crate::runtime::operators::window::tiles::TimeGranularity;
use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::runtime::operators::window::Cursor;

use super::super::{create_window_aggregator, merge_accumulator_state, WindowAggregator};
use super::VirtualPoint;
use super::BucketRange;
use crate::runtime::operators::window::index::{DataBounds, DataRequest, SortedRangeIndex, SortedRangeView};

#[derive(Debug)]
pub struct RetractablePointsAggregation {
    window_expr: Arc<dyn WindowExpr>,
    #[allow(dead_code)]
    window_id: usize,
    #[allow(dead_code)]
    bucket_granularity: TimeGranularity,
    base_window_range: BucketRange,
    processed_until: Cursor,
    base_accumulator_state: AccumulatorState,
    points: Vec<VirtualPoint>,
}

impl RetractablePointsAggregation {
    pub fn new(
        points: Vec<VirtualPoint>,
        bucket_index: &BucketIndex,
        window_expr: Arc<dyn WindowExpr>,
        ts_column_index: usize,
        window_id: usize,
        processed_until: Option<Cursor>,
        accumulator_state: Option<AccumulatorState>,
    ) -> Self {
        let _ = ts_column_index;
        let bucket_granularity = bucket_index.bucket_granularity();
        let processed_until = processed_until.expect("Retractable points require processed_until");
        let base_accumulator_state =
            accumulator_state.expect("Retractable points require accumulator_state");

        if points.iter().any(|p| p.ts < processed_until.ts) {
            panic!("RetractablePointsAggregation expects only non-late points (ts >= processed_until)");
        }

        let window_frame = window_expr.get_window_frame();
        let base_bucket_ts = bucket_granularity.start(processed_until.ts);
        let base_update = BucketRange::new(base_bucket_ts, base_bucket_ts);
        let base_window_range = match window_frame.units {
            WindowFrameUnits::Range => {
                let wl = get_window_length_ms(window_frame);
                Some(bucket_index.get_relevant_range_for_range_windows(base_update, wl))
            }
            WindowFrameUnits::Rows => {
                let ws = get_window_size_rows(window_frame);
                Some(bucket_index.get_relevant_range_for_rows_windows(base_update, ws))
            }
            _ => Some(base_update),
        };
        let base_window_range = base_window_range.expect("base_window_range should exist");

        Self {
            window_expr,
            window_id,
            bucket_granularity,
            base_window_range,
            processed_until,
            base_accumulator_state,
            points,
        }
    }

    pub(super) fn window_expr(&self) -> &Arc<dyn WindowExpr> {
        &self.window_expr
    }

    // we should only load retratcs range, not whole base window range
    pub(super) fn get_data_requests(&self, exclude_current_row: Option<bool>) -> Vec<DataRequest> {
        let processed_until = self.processed_until;
        let base_window_range = self.base_window_range;
        let include_virtual = !exclude_current_row.expect("exclude_current_row should exist");

        if !include_virtual {
            return vec![];
        }

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

        let max_point_ts = self.points.iter().map(|p| p.ts).max().unwrap_or(processed_until.ts);
        let bounds = if window_frame.units == WindowFrameUnits::Range {
            DataBounds::Time {
                // We must load enough history to be able to retract rows that are already included
                // in the stored accumulator state when sliding from `processed_until` to points.
                // That history starts at the base window start at `processed_until`.
                start_ts: processed_until.ts.saturating_sub(wl),
                end_ts: max_point_ts,
            }
        } else if window_frame.units == WindowFrameUnits::Rows && ws > 0 {
            // We need the full base-window span and all rows until the farthest point,
            // because we apply stored updates between processed_until and each point.
            DataBounds::All
        } else {
            DataBounds::All
        };

        vec![DataRequest {
            bucket_range: base_window_range,
            bounds,
        }]
    }

    pub(super) async fn produce_aggregates_from_ranges(
        &self,
        sorted_ranges: &[SortedRangeView],
        _thread_pool: Option<&ThreadPool>,
        exclude_current_row: Option<bool>,
    ) -> (Vec<ScalarValue>, Option<AccumulatorState>) {
        let processed_until = self.processed_until;
        let base_state = &self.base_accumulator_state;
        let include_virtual = !exclude_current_row.expect("exclude_current_row should exist");
        let window_frame = self.window_expr.get_window_frame();

        if !include_virtual {
            let mut acc = match create_window_aggregator(&self.window_expr) {
                WindowAggregator::Accumulator(accumulator) => accumulator,
                WindowAggregator::Evaluator(_) => panic!("Evaluator not supported"),
            };
            merge_accumulator_state(acc.as_mut(), base_state);
            let v = acc.evaluate().expect("evaluate failed");
            return (vec![v; self.points.len()], None);
        }

        if sorted_ranges.len() != 1 {
            panic!("RetractablePointsAggregation expects exactly one SortedRangeView");
        }
        let idx = SortedRangeIndex::new(&sorted_ranges[0]);

        if idx.is_empty() {
            let vals = self
                .points
                .iter()
                .map(|p| {
                    let mut acc = match create_window_aggregator(&self.window_expr) {
                        WindowAggregator::Accumulator(accumulator) => accumulator,
                        WindowAggregator::Evaluator(_) => panic!("Evaluator not supported"),
                    };
                    merge_accumulator_state(acc.as_mut(), base_state);
                    if let Some(args) = &p.args {
                        acc.update_batch(args.as_ref()).expect("update_batch failed");
                    }
                    acc.evaluate().expect("evaluate failed")
                })
                .collect();
            return (vals, None);
        }

        let processed_pos = idx
            .seek_rowpos_eq(processed_until)
            .unwrap_or_else(|| idx.last_pos());

        match window_frame.units {
            WindowFrameUnits::Rows => {
                let window_size = get_window_size_rows(window_frame);
                let total_stored = idx.count_between(&idx.first_pos(), &processed_pos);
                let base_window_stored = total_stored.min(window_size);

                // Oldest row currently in the base window state.
                let base_window_start = if base_window_stored > 0 {
                    idx.pos_n_rows(&processed_pos, base_window_stored - 1, true)
                } else {
                    idx.first_pos()
                };

                let vals = self
                    .points
                    .iter()
                    .map(|p| {
                        let mut acc = match create_window_aggregator(&self.window_expr) {
                            WindowAggregator::Accumulator(accumulator) => accumulator,
                            WindowAggregator::Evaluator(_) => panic!("Evaluator not supported"),
                        };
                        merge_accumulator_state(acc.as_mut(), base_state);

                        let mut window_start = base_window_start;
                        let mut window_count = base_window_stored;

                        // Apply real stored rows between processed_until and this point.
                        let mut pos_opt = idx.next_pos(processed_pos);
                        while let Some(pos) = pos_opt {
                            if idx.get_timestamp(&pos) > p.ts {
                                break;
                            }

                            if window_size > 0 && window_count == window_size {
                                let args = idx.get_row_args(&window_start);
                                acc.retract_batch(&args).expect("retract_batch failed");
                                window_start = idx
                                    .next_pos(window_start)
                                    .unwrap_or_else(|| window_start);
                            } else {
                                window_count += 1;
                            }

                            let args = idx.get_row_args(&pos);
                            acc.update_batch(&args).expect("update_batch failed");

                            pos_opt = idx.next_pos(pos);
                        }

                        // Apply virtual point (as an extra row at p.ts).
                        if include_virtual {
                            if window_size > 0 && window_count == window_size {
                                let args = idx.get_row_args(&window_start);
                                acc.retract_batch(&args).expect("retract_batch failed");
                            } else {
                                // window not full; no retract needed
                            }

                            if let Some(args) = &p.args {
                                acc.update_batch(args.as_ref()).expect("update_batch failed");
                            }
                        }

                        acc.evaluate().expect("evaluate failed")
                    })
                    .collect();
                (vals, None)
            }
            WindowFrameUnits::Range => {
                let window_length = get_window_length_ms(window_frame);
                let base_start = processed_until.ts.saturating_sub(window_length);
                let base_retract_start = idx.seek_ts_ge(base_start).unwrap_or_else(|| idx.first_pos());

                let vals = self
                    .points
                    .iter()
                    .map(|p| {
                        let mut acc = match create_window_aggregator(&self.window_expr) {
                            WindowAggregator::Accumulator(accumulator) => accumulator,
                            WindowAggregator::Evaluator(_) => panic!("Evaluator not supported"),
                        };
                        merge_accumulator_state(acc.as_mut(), base_state);

                        let mut retract_pos = base_retract_start;

                        // Apply real stored rows between processed_until and this point.
                        let mut pos_opt = idx.next_pos(processed_pos);
                        while let Some(pos) = pos_opt {
                            let ts = idx.get_timestamp(&pos);
                            if ts > p.ts {
                                break;
                            }

                            let new_start = ts.saturating_sub(window_length);
                            loop {
                                if idx.get_timestamp(&retract_pos) >= new_start {
                                    break;
                                }
                                let args = idx.get_row_args(&retract_pos);
                                acc.retract_batch(&args).expect("retract_batch failed");
                                let Some(next) = idx.next_pos(retract_pos) else { break };
                                retract_pos = next;
                            }

                            let args = idx.get_row_args(&pos);
                            acc.update_batch(&args).expect("update_batch failed");

                            pos_opt = idx.next_pos(pos);
                        }

                        // Apply virtual point.
                        if include_virtual {
                            let new_start = p.ts.saturating_sub(window_length);
                            loop {
                                if idx.get_timestamp(&retract_pos) >= new_start {
                                    break;
                                }
                                let args = idx.get_row_args(&retract_pos);
                                acc.retract_batch(&args).expect("retract_batch failed");
                                let Some(next) = idx.next_pos(retract_pos) else { break };
                                retract_pos = next;
                            }

                            if let Some(args) = &p.args {
                                acc.update_batch(args.as_ref()).expect("update_batch failed");
                            }
                        }

                        acc.evaluate().expect("evaluate failed")
                    })
                    .collect();
                (vals, None)
            }
            _ => (self.points.iter().map(|_| ScalarValue::Null).collect(), None),
        }
    }
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

    fn sum_state(window_expr: &Arc<dyn WindowExpr>, stored_batch: &arrow::record_batch::RecordBatch) -> AccumulatorState {
        let mut acc = match create_window_aggregator(window_expr) {
            WindowAggregator::Accumulator(a) => a,
            WindowAggregator::Evaluator(_) => panic!("not supported"),
        };
        let args = window_expr.evaluate_args(stored_batch).expect("eval args");
        acc.update_batch(&args).expect("update_batch failed");
        acc.state().expect("state failed")
    }

    #[tokio::test]
    async fn test_retractable_points_sum_range_window() {
        let sql = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
        let window_expr = test_utils::window_expr_from_sql(sql).await;
        let gran = TimeGranularity::Seconds(1);

        // Stored up to processed_until=2000.
        let stored = test_utils::batch(&[(1000, 10.0, "A", 0), (1500, 30.0, "A", 1), (2000, 20.0, "A", 2)]);
        let base_state = sum_state(&window_expr, &stored);
        let processed_until = Cursor::new(2000, 2);

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

        let p_args = window_expr
            .evaluate_args(&test_utils::one_row_batch(3200, 5.0, "A", 3))
            .expect("eval args");
        let points = vec![VirtualPoint { ts: 3200, args: Some(Arc::new(p_args)) }];

        let agg = RetractablePointsAggregation::new(
            points,
            &bucket_index,
            window_expr.clone(),
            0,
            0,
            Some(processed_until),
            Some(base_state.clone()),
        );
        let requests = agg.get_data_requests(Some(false));
        assert_eq!(requests.len(), 1);
        let view = test_utils::make_view(
            gran,
            requests[0],
            vec![
                (1000, test_utils::batch(&[(1000, 10.0, "A", 0), (1500, 30.0, "A", 1)])),
                (2000, test_utils::batch(&[(2000, 20.0, "A", 2)])),
            ],
            &window_expr,
        );

        let (vals, _) = agg.produce_aggregates_from_ranges(&[view], None, Some(false)).await;
        assert_f64s(&vals, &[55.0]);
    }

    #[tokio::test]
    async fn test_retractable_points_sum_rows_window() {
        let sql = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_3
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
)"#;
        let window_expr = test_utils::window_expr_from_sql(sql).await;
        let gran = TimeGranularity::Seconds(1);

        // Exactly 3 stored rows -> base_state represents a full window.
        let stored = test_utils::batch(&[(1000, 10.0, "A", 0), (1500, 30.0, "A", 1), (2000, 20.0, "A", 2)]);
        let base_state = sum_state(&window_expr, &stored);
        let processed_until = Cursor::new(2000, 2);

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

        let p_args = window_expr
            .evaluate_args(&test_utils::one_row_batch(2500, 5.0, "A", 3))
            .expect("eval args");
        let points = vec![VirtualPoint { ts: 2500, args: Some(Arc::new(p_args)) }];

        let agg = RetractablePointsAggregation::new(
            points,
            &bucket_index,
            window_expr.clone(),
            0,
            0,
            Some(processed_until),
            Some(base_state.clone()),
        );
        let requests = agg.get_data_requests(Some(false));
        assert_eq!(requests.len(), 1);
        let view = test_utils::make_view(
            gran,
            requests[0],
            vec![
                (1000, test_utils::batch(&[(1000, 10.0, "A", 0), (1500, 30.0, "A", 1)])),
                (2000, test_utils::batch(&[(2000, 20.0, "A", 2)])),
            ],
            &window_expr,
        );

        let (vals, _) = agg.produce_aggregates_from_ranges(&[view], None, Some(false)).await;
        assert_f64s(&vals, &[55.0]);
    }
}

