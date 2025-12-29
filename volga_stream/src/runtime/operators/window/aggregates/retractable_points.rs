use std::collections::HashMap;
use std::sync::Arc;

use datafusion::logical_expr::WindowFrameUnits;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use tokio_rayon::rayon::ThreadPool;

use crate::runtime::operators::window::index::{get_window_length_ms, get_window_size_rows, BucketIndex};
use crate::runtime::operators::window::tiles::TimeGranularity;
use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::runtime::operators::window::Cursor;
use crate::storage::batch_store::Timestamp;

use super::super::{create_window_aggregator, merge_accumulator_state, WindowAggregator};
use super::VirtualPoint;
use super::BucketRange;
use crate::runtime::operators::window::index::{DataBounds, DataRequest, SortedRangeIndex, SortedRangeView};
use crate::runtime::operators::window::RowPtr;

#[derive(Debug)]
pub struct RetractablePointsAggregation {
    window_expr: Arc<dyn WindowExpr>,
    window_id: usize,
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

    pub(super) fn get_data_requests(&self, exclude_current_row: Option<bool>) -> Vec<DataRequest> {
        let processed_until = self.processed_until;
        let base_window_range = self.base_window_range;
        let include_virtual = !exclude_current_row.expect("exclude_current_row should exist");

        if !include_virtual {
            return vec![];
        }

        let window_frame = self.window_expr.get_window_frame();
        let wl = get_window_length_ms(window_frame);
        let ws = get_window_size_rows(window_frame);

        let bounds = if window_frame.units == WindowFrameUnits::Range {
            let min_start_ts = self
                .points
                .iter()
                .map(|p| p.ts.saturating_sub(wl))
                .min()
                .unwrap_or(i64::MIN);
            DataBounds::Time {
                start_ts: min_start_ts,
                end_ts: processed_until.ts,
            }
        } else if window_frame.units == WindowFrameUnits::Rows && ws > 0 {
            DataBounds::RowsTail {
                end_ts: processed_until.ts,
                rows: ws,
            }
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
                let retract_one =
                    include_virtual && base_window_stored == window_size && window_size > 0;
                let retract_pos = if retract_one {
                    idx.pos_n_rows(&processed_pos, window_size.saturating_sub(1), true)
                } else {
                    RowPtr::new(0, 0)
                };

                let vals = self.points
                    .iter()
                    .map(|p| {
                        let mut acc = match create_window_aggregator(&self.window_expr) {
                            WindowAggregator::Accumulator(accumulator) => accumulator,
                            WindowAggregator::Evaluator(_) => panic!("Evaluator not supported"),
                        };
                        merge_accumulator_state(acc.as_mut(), base_state);

                        if retract_one {
                            let args = idx.get_row_args(&retract_pos);
                            acc.retract_batch(&args).expect("retract_batch failed");
                        }

                        if include_virtual {
                            if let Some(args) = &p.args {
                                acc.update_batch(args.as_ref()).expect("update_batch failed");
                            }
                        }

                        acc.evaluate().expect("evaluate failed")
                    })
                    .collect()
                ;
                (vals, None)
            }
            WindowFrameUnits::Range => {
                let window_length = get_window_length_ms(window_frame);
                let prev_start = processed_until.ts.saturating_sub(window_length);
                let prev_retract_pos =
                    idx.seek_ts_ge(prev_start).unwrap_or_else(|| idx.first_pos());

                let vals = self.points
                    .iter()
                    .map(|p| {
                        let mut acc = match create_window_aggregator(&self.window_expr) {
                            WindowAggregator::Accumulator(accumulator) => accumulator,
                            WindowAggregator::Evaluator(_) => panic!("Evaluator not supported"),
                        };
                        merge_accumulator_state(acc.as_mut(), base_state);

                        let new_start = p.ts.saturating_sub(window_length);
                        if new_start > prev_start {
                            let mut pos = prev_retract_pos;
                            loop {
                                if idx.get_timestamp(&pos) >= new_start {
                                    break;
                                }
                                let args = idx.get_row_args(&pos);
                                acc.retract_batch(&args).expect("retract_batch failed");
                                let Some(next) = idx.next_pos(pos) else { break };
                                pos = next;
                            }
                        }

                        if include_virtual {
                            if let Some(args) = &p.args {
                                acc.update_batch(args.as_ref()).expect("update_batch failed");
                            }
                        }

                        acc.evaluate().expect("evaluate failed")
                    })
                    .collect()
                ;
                (vals, None)
            }
            _ => (self.points.iter().map(|_| ScalarValue::Null).collect(), None),
        }
    }
}

