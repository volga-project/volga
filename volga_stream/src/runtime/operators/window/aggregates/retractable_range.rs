use std::sync::Arc;

use async_trait::async_trait;
use datafusion::logical_expr::WindowFrameUnits;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::index::window_logic;
use crate::runtime::operators::window::index::{
    get_window_length_ms,
    get_window_size_rows,
    DataBounds,
    DataRequest,
    SlideRangeInfo,
    SortedRangeIndex,
    SortedRangeView,
};
use crate::runtime::operators::window::tiles::TimeGranularity;
use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::runtime::operators::window::{Cursor, RowPtr, Tiles};

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
        let wl = get_window_length_ms(window_frame);
        let ws = get_window_size_rows(window_frame);

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
        let window_size = get_window_size_rows(window_frame);

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

        let (aggs, state) = run_retractable_accumulator_from_indices(
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

fn run_retractable_accumulator_from_indices(
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

    let mut retract_pos = match (retracts, is_rows_window) {
        (None, _) => RowPtr::new(0, 0),
        (Some(retracts), true) => window_logic::initial_retract_pos_rows(
            retracts,
            updates,
            update_pos,
            window_size,
            row_distance,
            num_updates,
        ),
        (Some(retracts), false) => window_logic::initial_retract_pos_range(
            retracts,
            prev_processed_until.expect("Should have previous processed until"),
            window_length,
        ),
    };

    let mut results = Vec::with_capacity(num_updates);

    loop {
        let update_args = updates.get_row_args(&update_pos);
        accumulator
            .update_batch(&update_args)
            .expect("Should be able to update accumulator");

        if is_rows_window {
            if let Some(retracts) = retracts {
                let retract_args = retracts.get_row_args(&retract_pos);
                accumulator
                    .retract_batch(&retract_args)
                    .expect("Should be able to retract from accumulator");

                if update_pos != end_pos {
                    retract_pos = retracts
                        .next_pos(retract_pos)
                        .expect("Retracts should have a next position for each update");
                }
            }
        } else {
            let update_ts = updates.get_timestamp(&update_pos);
            let window_start = update_ts - window_length;

            if let Some(retracts) = retracts {
                loop {
                    let retract_ts = retracts.get_timestamp(&retract_pos);
                    if retract_ts < window_start {
                        let retract_args = retracts.get_row_args(&retract_pos);
                        accumulator
                            .retract_batch(&retract_args)
                            .expect("Should be able to retract from accumulator");
                        let Some(next) = retracts.next_pos(retract_pos) else { break };
                        retract_pos = next;
                    } else {
                        break;
                    }
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

