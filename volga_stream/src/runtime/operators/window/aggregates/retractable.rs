use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::logical_expr::WindowFrameUnits;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use async_trait::async_trait;

use tokio_rayon::rayon::ThreadPool;
use tokio_rayon::AsyncThreadPool;

use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::runtime::operators::window::batch_index::{Bucket, SlideInfo, get_window_length_ms, get_window_size_rows};
use crate::runtime::operators::window::Tiles;
use crate::storage::batch_store::{BatchId, Timestamp};
use indexmap::IndexSet;

use super::{Aggregation, AggregatorType, WindowAggregator, create_window_aggregator, merge_accumulator_state};
use super::utils::{EntriesRange, Pos, sort_buckets};

#[derive(Debug)]
pub struct RetractableAggregation {
    pub update_buckets: Vec<Bucket>,
    pub retract_buckets: Vec<Bucket>,
    pub row_distance: usize,
    pub prev_end_timestamp: Timestamp,
    pub new_end_timestamp: Timestamp,
    pub window_expr: Arc<dyn WindowExpr>,
    pub accumulator_state: Option<AccumulatorState>,
    pub ts_column_index: usize,
}

impl RetractableAggregation {
    /// Create from SlideInfo
    pub fn from_slide_info(
        slide_info: &SlideInfo,
        prev_end_timestamp: Timestamp,
        window_expr: Arc<dyn WindowExpr>,
        accumulator_state: Option<AccumulatorState>,
        ts_column_index: usize,
    ) -> Self {
        Self {
            update_buckets: slide_info.update_buckets.iter().map(|b| (*b).clone()).collect(),
            retract_buckets: slide_info.retract_buckets.iter().map(|b| (*b).clone()).collect(),
            row_distance: slide_info.row_distance,
            prev_end_timestamp,
            new_end_timestamp: slide_info.new_end_timestamp,
            window_expr,
            accumulator_state,
            ts_column_index,
        }
    }
    
    fn get_all_batch_ids(&self) -> IndexSet<BatchId> {
        let mut batch_ids = IndexSet::new();
        for bucket in &self.update_buckets {
            for metadata in &bucket.batches {
                batch_ids.insert(metadata.batch_id);
            }
        }
        for bucket in &self.retract_buckets {
            for metadata in &bucket.batches {
                batch_ids.insert(metadata.batch_id);
            }
        }
        batch_ids
    }
}

#[async_trait]
impl Aggregation for RetractableAggregation {
    async fn produce_aggregates(
        &self,
        batches: &HashMap<BatchId, RecordBatch>,
        thread_pool: Option<&ThreadPool>,
        exclude_current_row: Option<bool>,
    ) -> (Vec<ScalarValue>, Option<AccumulatorState>) {
        let parallelize = thread_pool.is_some();

        let (aggregates, accumulator_state) = if parallelize {
            run_retractable_accumulator_parallel(
                thread_pool.expect("ThreadPool should exist"),
                self.window_expr.clone(),
                self.update_buckets.clone(),
                self.retract_buckets.clone(),
                self.row_distance,
                self.prev_end_timestamp,
                self.new_end_timestamp,
                Arc::new(batches.clone()),
                self.accumulator_state.clone(),
                self.ts_column_index,
                exclude_current_row,
            ).await
        } else {
            run_retractable_accumulator(
                &self.window_expr,
                &self.update_buckets,
                &self.retract_buckets,
                self.row_distance,
                self.prev_end_timestamp,
                self.new_end_timestamp,
                batches,
                self.accumulator_state.as_ref(),
                self.ts_column_index,
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
}

async fn run_retractable_accumulator_parallel(
    thread_pool: &ThreadPool,
    window_expr: Arc<dyn WindowExpr>, 
    update_buckets: Vec<Bucket>,
    retract_buckets: Vec<Bucket>,
    row_distance: usize,
    prev_end_timestamp: Timestamp,
    new_end_timestamp: Timestamp,
    batches: Arc<HashMap<BatchId, RecordBatch>>,
    previous_accumulator_state: Option<AccumulatorState>,
    ts_column_index: usize,
    exclude_current_row: Option<bool>,
) -> (Vec<ScalarValue>, AccumulatorState) {
    thread_pool.spawn_fifo_async(move || {
        run_retractable_accumulator(
            &window_expr,
            &update_buckets,
            &retract_buckets,
            row_distance,
            prev_end_timestamp,
            new_end_timestamp,
            &batches,
            previous_accumulator_state.as_ref(),
            ts_column_index,
            exclude_current_row,
        )
    }).await
}

fn run_retractable_accumulator(
    window_expr: &Arc<dyn WindowExpr>, 
    update_buckets: &[Bucket],
    retract_buckets: &[Bucket],
    row_distance: usize,
    prev_end_timestamp: Timestamp,
    _new_end_timestamp: Timestamp,
    batches: &HashMap<BatchId, RecordBatch>,
    accumulator_state: Option<&AccumulatorState>,
    ts_column_index: usize,
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

    // Sort update batches and pre-evaluate args
    let update_batches = sort_buckets(update_buckets, batches, ts_column_index);
    let updates = EntriesRange::new(&update_batches, window_expr, ts_column_index);
    
    if updates.is_empty() {
        return (vec![], accumulator.state().expect("Should be able to get accumulator state"));
    }

    // Sort retract batches and pre-evaluate args
    let retract_batches = sort_buckets(retract_buckets, batches, ts_column_index);
    let retracts = EntriesRange::new(&retract_batches, window_expr, ts_column_index);

    // Find update start (first row after prev_end_timestamp)
    let mut update_pos = updates.find_first(prev_end_timestamp, false, &updates.first_pos());
    let num_updates = updates.count_between(&update_pos, &updates.last_pos());

    // Calculate retract bounds
    let mut retract_pos = if retracts.is_empty() {
        Pos::new(0, 0)
    } else if is_rows_window {
        // ROWS: calculate start position for retracts
        let window_size = get_window_size_rows(window_frame);
        let update_start_offset = updates.count_between(&updates.first_pos(), &update_pos) - 1;

        let retract_end_offset = window_size.saturating_sub(row_distance + update_start_offset);
        let retract_start_offset = retract_end_offset + num_updates;
        
        retracts.pos_from_end(retract_start_offset)
    } else {
        // RANGE: find start position
        let prev_start_ts = prev_end_timestamp - window_length;
        retracts.find_first(prev_start_ts, true, &retracts.first_pos())
    };

    let mut results = Vec::with_capacity(num_updates);

    // Main loop
    while updates.can_advance(&update_pos) {
        // Apply update
        let update_args = updates.get_row_args(&update_pos);
        accumulator.update_batch(&update_args)
            .expect("Should be able to update accumulator");

        // Apply retracts
        if is_rows_window {
            // ROWS: 1-to-1 retract
            if !retracts.can_advance(&retract_pos) {
                panic!("Retracts should be able to advance when updates can advance");
            }
            let retract_args = retracts.get_row_args(&retract_pos);
            accumulator.retract_batch(&retract_args)
                .expect("Should be able to retract from accumulator");
            retracts.advance(&mut retract_pos);
        } else {
            // RANGE: retract while timestamp < window_start
            let update_ts = updates.get_timestamp(&update_pos);
            let window_start = update_ts - window_length;
            
            while retracts.can_advance(&retract_pos) {
                let retract_ts = retracts.get_timestamp(&retract_pos);
                if retract_ts < window_start {
                    let retract_args = retracts.get_row_args(&retract_pos);
                    accumulator.retract_batch(&retract_args)
                        .expect("Should be able to retract from accumulator");
                    retracts.advance(&mut retract_pos);
                } else {
                    break;
                }
            }
        }

        let result = accumulator.evaluate()
            .expect("Should be able to evaluate accumulator");
        results.push(result);

        updates.advance(&mut update_pos);
    }

    (results, accumulator.state().expect("Should be able to get accumulator state"))
}
