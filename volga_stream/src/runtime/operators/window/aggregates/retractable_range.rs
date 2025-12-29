use std::sync::Arc;

use async_trait::async_trait;
use datafusion::logical_expr::WindowFrameUnits;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::index::{get_window_size_rows, window_logic};
use crate::runtime::operators::window::index::{
    get_window_length_ms,
    DataBounds,
    DataRequest,
    SlideRangeInfo,
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
    pub update_range: Option<BucketRange>,
    pub retract_range: Option<BucketRange>,
    pub row_distance: usize,
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
        slide_info: &SlideRangeInfo,
        prev_processed_until: Option<Cursor>,
        window_expr: Arc<dyn WindowExpr>,
        accumulator_state: Option<AccumulatorState>,
        ts_column_index: usize,
        bucket_granularity: TimeGranularity,
    ) -> Self {
        Self {
            update_range: slide_info.update_range,
            retract_range: slide_info.retract_range,
            row_distance: slide_info.row_distance,
            prev_processed_until,
            new_processed_until: slide_info.new_processed_until,
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

    fn get_data_requests(&self, _exclude_current_row: Option<bool>) -> Vec<DataRequest> {
        let Some(update) = self.update_range else {
            return vec![];
        };
        let bucket_range = if let Some(retract) = self.retract_range {
            BucketRange::new(update.start.min(retract.start), update.end.max(retract.end))
        } else {
            update
        };
        let window_frame = self.window_expr.get_window_frame();
        let wl = if window_frame.units == WindowFrameUnits::Range {
            get_window_length_ms(window_frame)
        } else {
            0
        };

        let bounds = if window_frame.units == WindowFrameUnits::Range {
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

        vec![DataRequest { bucket_range, bounds }]
    }

    async fn produce_aggregates_from_ranges(
        &self,
        sorted_ranges: &[SortedRangeView],
        _thread_pool: Option<&tokio_rayon::rayon::ThreadPool>,
        _exclude_current_row: Option<bool>,
    ) -> (Vec<ScalarValue>, Option<AccumulatorState>) {
        let window_frame = self.window_expr.get_window_frame();
        let is_rows_window = window_frame.units == WindowFrameUnits::Rows;
        let window_length = get_window_length_ms(window_frame);
        let window_size = if is_rows_window { get_window_size_rows(window_frame) } else { 0 };

        let Some(update_range) = self.update_range else {
            return (vec![], self.accumulator_state.clone());
        };
        let Some(view) = sorted_ranges.first() else {
            return (vec![], self.accumulator_state.clone());
        };

        let updates = SortedRangeIndex::new_in_bucket_range(view, update_range);
        let retracts = self
            .retract_range
            .map(|r| SortedRangeIndex::new_in_bucket_range(view, r));

        let (aggs, state) = run_retractable_accumulator(
            &self.window_expr,
            &updates,
            retracts.as_ref(),
            self.row_distance,
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
    row_distance: usize,
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

    // Retract pointers (when a retracts index exists).
    // For first-run we retract within the same range as updates (planning sets retract_range=update_range).
    let mut rows_retract_pos: Option<RowPtr> = None;
    let mut rows_seen: usize = 0;
    let mut range_retract_pos: Option<RowPtr> = None;

    if let Some(retracts) = retracts {
        if is_rows_window {
            rows_retract_pos = if first_run {
                Some(retracts.first_pos())
            } else {
                Some(window_logic::initial_retract_pos_rows(
                    retracts,
                    updates,
                    update_pos,
                    window_size,
                    row_distance,
                    num_updates,
                ))
            };
        } else {
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

    loop {
        let update_args = updates.get_row_args(&update_pos);
        accumulator
            .update_batch(&update_args)
            .expect("Should be able to update accumulator");

        if is_rows_window {
            if let Some(retracts) = retracts {
                if first_run {
                    // First run: retract within the same stream once the ROWS window is full.
                    rows_seen += 1;
                    if rows_seen > window_size {
                        if let Some(rp) = rows_retract_pos {
                            let retract_args = retracts.get_row_args(&rp);
                            accumulator
                                .retract_batch(&retract_args)
                                .expect("Should be able to retract from accumulator");
                            rows_retract_pos = retracts.next_pos(rp);
                        }
                    }
                } else {
                    // Normal case: exactly one retract per update, from the precomputed retract range.
                    let rp = rows_retract_pos.expect("rows retract_pos must be set");
                    let retract_args = retracts.get_row_args(&rp);
                    accumulator
                        .retract_batch(&retract_args)
                        .expect("Should be able to retract from accumulator");
                    if update_pos != end_pos {
                        rows_retract_pos =
                            Some(retracts.next_pos(rp).expect("must have next retract pos"));
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
    async fn test_retractable_range_sum_range_window_two_steps() {
        let sql = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '2000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
        let window_expr = test_utils::window_expr_from_sql(sql).await;
        let window_frame = window_expr.get_window_frame().clone();
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

        let slide1 = bucket_index.slide_window(&window_frame, None).expect("slide1");
        let agg1 = RetractableRangeAggregation::new(
            0,
            &slide1,
            None,
            window_expr.clone(),
            None,
            0,
            gran,
        );
        let req1 = agg1.get_data_requests(None)[0];
        let view1 = test_utils::make_view(gran, req1, vec![(1000, b1000), (2000, b2000)], &window_expr);
        let (vals1, state1) = agg1.produce_aggregates_from_ranges(&[view1], None, None).await;
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

        let prev = Some(slide1.new_processed_until);
        let slide2 = bucket_index.slide_window(&window_frame, prev).expect("slide2");
        let agg2 = RetractableRangeAggregation::new(
            0,
            &slide2,
            prev,
            window_expr.clone(),
            Some(state1.clone()),
            0,
            gran,
        );
        let req2 = agg2.get_data_requests(None)[0];
        let view2 = test_utils::make_view(
            gran,
            req2,
            vec![
                (1000, test_utils::batch(&[(1000, 10.0, "A", 0), (1500, 30.0, "A", 1)])),
                (2000, test_utils::batch(&[(2000, 20.0, "A", 2)])),
                (3000, b3000),
            ],
            &window_expr,
        );

        let (vals2, state2) = agg2.produce_aggregates_from_ranges(&[view2], None, None).await;
        assert_f64s(&vals2, &[55.0]);
        let state2 = state2.expect("state2");
        assert!((eval_state(&window_expr, &state2) - 55.0).abs() < 1e-9);
    }
}

