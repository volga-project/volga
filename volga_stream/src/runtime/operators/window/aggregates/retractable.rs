
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use async_trait::async_trait;

use tokio_rayon::rayon::ThreadPool;
use tokio_rayon::AsyncThreadPool;

use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::runtime::operators::window::time_entries::TimeIdx;
use crate::runtime::operators::window::Tiles;
use crate::storage::batch_store::BatchId;
use indexmap::IndexSet;

use super::{Aggregation, AggregatorType, WindowAggregator, create_window_aggregator, merge_accumulator_state};
use super::arrow_utils::evaluate_batches_and_map_results;

#[derive(Debug)]
pub struct RetractableAggregation {
    pub entries: Vec<TimeIdx>,
    pub window_expr: Arc<dyn WindowExpr>,
    pub tiles: Option<Tiles>,
    pub retracts: Option<Vec<Vec<TimeIdx>>>,
    pub accumulator_state: Option<AccumulatorState>,
}

impl RetractableAggregation {
    pub fn new(
        entries: Vec<TimeIdx>,
        window_expr: Arc<dyn WindowExpr>,
        tiles: Option<Tiles>,
        retracts: Option<Vec<Vec<TimeIdx>>>,
        accumulator_state: Option<AccumulatorState>,
    ) -> Self {
        Self {
            entries,
            window_expr,
            tiles,
            retracts,
            accumulator_state,
        }
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
        let mut updates = Some(self.entries.clone());

        // in request mode, we expect entries to be a single value vec - virtual current row.
        // include it in aggregation if needed, otherwise empty updates, only retracts
        if exclude_current_row.is_some() && exclude_current_row.unwrap() {
            updates = None;
        }
        let (aggregates, accumulator_state) = if parallelize {
            run_retractable_accumulator_parallel(
                thread_pool.expect("ThreadPool should exist"),
                self.window_expr.clone(),
                updates,
                self.retracts.clone(),
                Arc::new(batches.clone()),
                self.accumulator_state.clone()
            ).await
        } else {
            run_retractable_accumulator(
                &self.window_expr,
                updates,
                self.retracts.clone(),
                batches,
                self.accumulator_state.as_ref()
            )
        };
        
        (aggregates, Some(accumulator_state))
    }
    
    fn entries(&self) -> &[TimeIdx] {
        &self.entries
    }
    
    fn window_expr(&self) -> &Arc<dyn WindowExpr> {
        &self.window_expr
    }
    
    fn tiles(&self) -> Option<&Tiles> {
        self.tiles.as_ref()
    }
    
    fn aggregator_type(&self) -> AggregatorType {
        AggregatorType::RetractableAccumulator
    }
    
    fn get_batches_to_load(&self) -> IndexSet<BatchId> {
        let mut batches_to_load = IndexSet::new();
        
        // for retractables, we do not need to scan whole window on each update
        for i in 0..self.entries.len() {
            batches_to_load.insert(self.entries[i].batch_id);
            if let Some(ref retracts) = self.retracts {
                for retract_idx in &retracts[i] {
                    batches_to_load.insert(retract_idx.batch_id);
                }
            }
        }
        
        batches_to_load
    }
}

async fn run_retractable_accumulator_parallel(
    thread_pool: &ThreadPool,
    window_expr: Arc<dyn WindowExpr>, 
    updates: Option<Vec<TimeIdx>>, 
    retracts: Option<Vec<Vec<TimeIdx>>>, 
    batches: Arc<HashMap<BatchId, RecordBatch>>,
    previous_accumulator_state: Option<AccumulatorState>
) -> (Vec<ScalarValue>, AccumulatorState) {
    thread_pool.spawn_fifo_async(move || {
        run_retractable_accumulator(&window_expr, updates, retracts, &batches, previous_accumulator_state.as_ref())
    }).await
}

// for multiple entries (updates) and optional retracts, produces aggregated values 
// by incrementally applying (and retracting) updates to accumulator
fn run_retractable_accumulator(
    window_expr: &Arc<dyn WindowExpr>, 
    updates: Option<Vec<TimeIdx>>, 
    retracts: Option<Vec<Vec<TimeIdx>>>, 
    batches: &HashMap<BatchId, RecordBatch>,
    accumulator_state: Option<&AccumulatorState>
) -> (Vec<ScalarValue>, AccumulatorState) {
    let mut accumulator = match create_window_aggregator(&window_expr) {
        WindowAggregator::Accumulator(accumulator) => accumulator,
        WindowAggregator::Evaluator(_) => panic!("Evaluator is not supported for retractable accumulator"),
    };

    if let Some(accumulator_state) = accumulator_state {
        merge_accumulator_state(accumulator.as_mut(), accumulator_state);
    }

    let mut results = Vec::new();

    if let Some(updates) = updates {
        // Prepare batch-wise update args
        let update_args = get_update_args(&updates, batches, window_expr)
            .expect("Should be able to get update args");
        
        // Prepare batch-wise retract args if retracts exist
        let retract_args = if let Some(ref retracts) = retracts {
            get_retract_args(retracts, batches, window_expr)
                .expect("Should be able to get retract args")
        } else {
            vec![Vec::new(); updates.len()]
        };

        // Process each update with corresponding retracts
        for i in 0..updates.len() {
            // Apply update
            accumulator.update_batch(&update_args[i]).expect("Should be able to update accumulator");

            // Apply retracts for this update
            if !retract_args[i].is_empty() {
                accumulator.retract_batch(&retract_args[i]).expect("Should be able to retract from accumulator");
            }

            let result = accumulator.evaluate().expect("Should be able to evaluate accumulator");
            results.push(result);
        }
    } else {
        // Special case - we have a single retracts range to run
        let retracts_vec = retracts.expect("Retracts should exist");
        let retract_idxs = retracts_vec.first().expect("Should have at least one retract");
        
        if !retract_idxs.is_empty() {
            let retract_args = get_update_args(retract_idxs, batches, window_expr)
                .expect("Should be able to prepare batch-wise retract args");
            
            for retract_arg in retract_args {
                if !retract_arg.is_empty() {
                    accumulator.retract_batch(&retract_arg).expect("Should be able to retract from accumulator");
                }
            }
        }
        
        let result = accumulator.evaluate().expect("Should be able to evaluate accumulator");
        results.push(result);
    }

    (
        results,
        accumulator
            .state()
            .expect("Should be able to get accumulator state")
    )
}

fn get_update_args(
    updates: &[TimeIdx],
    batches: &HashMap<BatchId, RecordBatch>,
    window_expr: &Arc<dyn WindowExpr>
) -> Result<Vec<Vec<arrow::array::ArrayRef>>, Box<dyn std::error::Error>> {
    use std::collections::HashMap;

    // Group updates by batch_id
    let mut indices_per_batch: HashMap<BatchId, Vec<(usize, usize)>> = HashMap::new();
    
    for (pos, time_idx) in updates.iter().enumerate() {
        indices_per_batch
            .entry(time_idx.batch_id)
            .or_default()
            .push((pos, time_idx.row_idx));
    }

    let mut result = vec![Vec::new(); updates.len()];

    evaluate_batches_and_map_results(
        indices_per_batch,
        batches,
        window_expr,
        |original_pos, single_row_args| {
            result[original_pos] = single_row_args;
        }
    )?;

    Ok(result)
}

fn get_retract_args(
    retracts: &[Vec<TimeIdx>], // retracts per update
    batches: &HashMap<BatchId, RecordBatch>,
    window_expr: &Arc<dyn WindowExpr>
) -> Result<Vec<Vec<arrow::array::ArrayRef>>, Box<dyn std::error::Error>> {
    use std::collections::HashMap;

    // Group ALL retract indices by batch_id, tracking which update they belong to
    let mut indices_per_batch: HashMap<BatchId, Vec<(usize, usize)>> = HashMap::new(); // (update_idx, row_idx_in_batch)
    
    for (update_idx, retract_indices) in retracts.iter().enumerate() {
        for time_idx in retract_indices.iter() {
            indices_per_batch
                .entry(time_idx.batch_id)
                .or_default()
                .push((update_idx, time_idx.row_idx));
        }
    }

    let mut result: Vec<Vec<arrow::array::ArrayRef>> = vec![Vec::new(); retracts.len()];

    // Collect single row args by update_idx first, then concatenate once per update per batch
    let mut update_single_rows: HashMap<usize, Vec<Vec<arrow::array::ArrayRef>>> = HashMap::new();

    evaluate_batches_and_map_results(
        indices_per_batch,
        batches,
        window_expr,
        |update_idx, single_row_args| {
            update_single_rows.entry(update_idx).or_default().push(single_row_args);
        }
    )?;
    
    // Concatenate all single rows for each update at once
    for (update_idx, single_rows_list) in update_single_rows {
        if !single_rows_list.is_empty() {
            let num_columns = single_rows_list[0].len();
            let mut batch_args_for_update = Vec::with_capacity(num_columns);
            
            for col_idx in 0..num_columns {
                let arrays_to_concat: Vec<&dyn arrow::array::Array> = single_rows_list
                    .iter()
                    .map(|single_row| single_row[col_idx].as_ref())
                    .collect();
                
                let concatenated = arrow::compute::concat(&arrays_to_concat)
                    .expect("Should be able to concatenate arrays");
                batch_args_for_update.push(concatenated);
            }
            
            if result[update_idx].is_empty() {
                result[update_idx] = batch_args_for_update;
            } else {
                panic!("Should not happen, concat should be done before");
            }
        }
    }

    Ok(result)
}