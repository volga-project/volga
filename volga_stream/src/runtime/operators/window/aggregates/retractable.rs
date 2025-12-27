use std::sync::Arc;

use datafusion::logical_expr::WindowFrameUnits;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use async_trait::async_trait;

use tokio_rayon::rayon::ThreadPool;
use tokio_rayon::AsyncThreadPool;

use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::runtime::operators::window::index::{SlideInfo, get_window_length_ms, get_window_size_rows};
use crate::runtime::operators::window::Tiles;
use crate::runtime::operators::window::Cursor;
use crate::storage::batch_store::Timestamp;
use crate::runtime::operators::window::index::window_logic;
use crate::runtime::operators::window::tiles::TimeGranularity;

use super::{Aggregation, AggregatorType, WindowAggregator, create_window_aggregator, merge_accumulator_state};
use crate::runtime::operators::window::index::SortedBucketView;
use super::BucketRange;
use std::collections::HashMap;
use crate::runtime::operators::window::{RowIndex, RowPtr};

#[derive(Debug)]
pub struct RetractableAggregation {
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

impl RetractableAggregation {
    /// Create from SlideInfo
    pub fn from_slide_info(
        window_id: usize,
        slide_info: &SlideInfo,
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
    
    fn relevant_bucket_ranges(&self) -> Vec<BucketRange> {
        let mut out = Vec::new();
        if let Some(r) = self.update_range {
            out.push(r);
        }
        if let Some(r) = self.retract_range {
            out.push(r);
        }
        out
    }
}

#[async_trait]
impl Aggregation for RetractableAggregation {
    async fn produce_aggregates(
        &self,
        sorted_bucket_view: &HashMap<Timestamp, SortedBucketView>,
        thread_pool: Option<&ThreadPool>,
        exclude_current_row: Option<bool>,
    ) -> (Vec<ScalarValue>, Option<AccumulatorState>) {
        let parallelize = thread_pool.is_some();

        let (aggregates, accumulator_state) = if parallelize {
            run_retractable_accumulator_parallel(
                thread_pool.expect("ThreadPool should exist"),
                self.window_expr.clone(),
                self.window_id,
                self.update_range,
                self.retract_range,
                Arc::new(sorted_bucket_view.clone()),
                self.row_distance,
                self.prev_processed_until,
                self.new_processed_until,
                self.accumulator_state.clone(),
                self.ts_column_index,
                self.bucket_granularity,
                exclude_current_row,
            ).await
        } else {
            run_retractable_accumulator(
                &self.window_expr,
                self.window_id,
                self.update_range,
                self.retract_range,
                sorted_bucket_view,
                self.row_distance,
                self.prev_processed_until,
                self.new_processed_until,
                self.accumulator_state.as_ref(),
                self.ts_column_index,
                self.bucket_granularity,
                exclude_current_row,
            )
        };
        
        (aggregates, Some(accumulator_state))
    }
    
    fn window_expr(&self) -> &Arc<dyn WindowExpr> {
        &self.window_expr
    }
    
    fn tiles(&self) -> Option<&Tiles> {
        None
    }
    
    fn aggregator_type(&self) -> AggregatorType {
        AggregatorType::RetractableAccumulator
    }

    fn get_relevant_buckets(&self) -> Vec<BucketRange> {
        self.relevant_bucket_ranges()
    }
}

async fn run_retractable_accumulator_parallel(
    thread_pool: &ThreadPool,
    window_expr: Arc<dyn WindowExpr>, 
    window_id: usize,
    update_range: Option<BucketRange>,
    retract_range: Option<BucketRange>,
    sorted_bucket_view: Arc<HashMap<Timestamp, SortedBucketView>>,
    row_distance: usize,
    prev_processed_until: Option<Cursor>,
    new_processed_until: Cursor,
    previous_accumulator_state: Option<AccumulatorState>,
    ts_column_index: usize,
    bucket_granularity: TimeGranularity,
    exclude_current_row: Option<bool>,
) -> (Vec<ScalarValue>, AccumulatorState) {
    thread_pool.spawn_fifo_async(move || {
        run_retractable_accumulator(
            &window_expr,
            window_id,
            update_range,
            retract_range,
            &sorted_bucket_view,
            row_distance,
            prev_processed_until,
            new_processed_until,
            previous_accumulator_state.as_ref(),
            ts_column_index,
            bucket_granularity,
            exclude_current_row,
        )
    }).await
}

fn run_retractable_accumulator(
    window_expr: &Arc<dyn WindowExpr>, 
    window_id: usize,
    update_range: Option<BucketRange>,
    retract_range: Option<BucketRange>,
    sorted_bucket_view: &HashMap<Timestamp, SortedBucketView>,
    row_distance: usize,
    prev_processed_until: Option<Cursor>,
    new_processed_until: Cursor,
    accumulator_state: Option<&AccumulatorState>,
    ts_column_index: usize,
    bucket_granularity: TimeGranularity,
    _exclude_current_row: Option<bool>,
) -> (Vec<ScalarValue>, AccumulatorState) {
    let mut accumulator = match create_window_aggregator(window_expr) {
        WindowAggregator::Accumulator(accumulator) => accumulator,
        WindowAggregator::Evaluator(_) => panic!("Evaluator is not supported for retractable accumulator"),
    };

    if let Some(accumulator_state) = accumulator_state {
        merge_accumulator_state(accumulator.as_mut(), accumulator_state);
    }

    let window_frame = window_expr.get_window_frame();
    let is_rows_window = window_frame.units == WindowFrameUnits::Rows;
    let window_length = get_window_length_ms(window_frame);

    let Some(update_range) = update_range else {
        return (vec![], accumulator.state().expect("Should be able to get accumulator state"));
    };

    let updates = RowIndex::new(
        update_range,
        sorted_bucket_view,
        window_id,
        bucket_granularity,
    );
    
    if updates.is_empty() {
        return (vec![], accumulator.state().expect("Should be able to get accumulator state"));
    }

    let retracts =
        retract_range.map(|r| RowIndex::new(r, sorted_bucket_view, window_id, bucket_granularity));

    // Find update start (first row with RowPos > processed_until).
    let update_pos = window_logic::first_update_pos(&updates, prev_processed_until);
    let Some(mut update_pos) = update_pos else {
        return (vec![], accumulator.state().expect("Should be able to get accumulator state"));
    };

    // Cap updates by `new_processed_until` (Cursor boundary), not by `updates.last_pos()`.
    let end_pos = {
        // If there's a first row > new_processed_until, there are no updates.
        let first_row_pos = updates.get_row_pos(&updates.first_pos());
        if new_processed_until < first_row_pos {
            return (vec![], accumulator.state().expect("Should be able to get accumulator state"));
        }

        // Find the first row with Cursor > new_processed_until, then step back one.
        match updates.seek_rowpos_gt(new_processed_until) {
            Some(after_end) => updates
                .prev_pos(after_end)
                .expect("seek_rowpos_gt returned the first row; should be guarded above"),
            None => updates.last_pos(),
        }
    };

    // If the computed end is before the start (e.g. prev_processed_until already at/after new boundary),
    // there is nothing to do.
    if end_pos < update_pos {
        return (vec![], accumulator.state().expect("Should be able to get accumulator state"));
    }

    let num_updates = updates.count_between(&update_pos, &end_pos);

    // Calculate retract bounds
    let mut retract_pos = match (&retracts, is_rows_window) {
        (None, _) => RowPtr::new(0, 0),
        (Some(retracts), true) => {
            // ROWS: calculate start position for retracts
            let window_size = get_window_size_rows(window_frame);
            window_logic::initial_retract_pos_rows(
                retracts,
                &updates,
                update_pos,
                window_size,
                row_distance,
                num_updates,
            )
        }
        (Some(retracts), false) => {
            // RANGE: find start position
            window_logic::initial_retract_pos_range(
                retracts,
                prev_processed_until.expect("Should be able to get previous processed until"),
                window_length,
            )
        }
    };

    let mut results = Vec::with_capacity(num_updates);

    // Main loop
    loop {
        // Apply update
        let update_args = updates.get_row_args(&update_pos);
        accumulator.update_batch(&update_args)
            .expect("Should be able to update accumulator");

        // Apply retracts
        if is_rows_window {
            // ROWS: 1-to-1 retract
            if let Some(retracts) = &retracts {
                let retract_args = retracts.get_row_args(&retract_pos);
                accumulator
                    .retract_batch(&retract_args)
                    .expect("Should be able to retract from accumulator");

                let has_more_updates = update_pos != end_pos;
                if has_more_updates {
                    retract_pos = retracts
                        .next_pos(retract_pos)
                        .expect("Retracts should have a next position for each update");
                }
            }
        } else {
            // RANGE: retract while timestamp < window_start
            let update_ts = updates.get_timestamp(&update_pos);
            let window_start = update_ts - window_length;
            
            if let Some(retracts) = &retracts {
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

        let result = accumulator.evaluate()
            .expect("Should be able to evaluate accumulator");
        results.push(result);

        if update_pos == end_pos {
            break;
        }
        update_pos = updates
            .next_pos(update_pos)
            .expect("end_pos should be reachable from update_pos");
    }

    (results, accumulator.state().expect("Should be able to get accumulator state"))
}
