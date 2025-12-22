use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use async_trait::async_trait;

use tokio_rayon::rayon::ThreadPool;
use tokio_rayon::AsyncThreadPool;

use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::runtime::operators::window::tiles::Tile;
use crate::runtime::operators::window::time_entries::{TimeEntries, TimeIdx};
use crate::runtime::operators::window::Tiles;
use crate::storage::batch_store::BatchId;
use indexmap::IndexSet;

use super::{Aggregation, AggregatorType, WindowAggregator, create_window_aggregator, merge_accumulator_state};
use super::arrow_utils::evaluate_batches_and_map_results;

#[derive(Debug)]
pub struct PlainAggregation {
    pub entries: Vec<TimeIdx>,
    pub window_expr: Arc<dyn WindowExpr>,
    pub tiles: Option<Tiles>,
    pub relevant_time_entries: TimeEntries,
    pub batches_to_load: IndexSet<BatchId>,
}

impl PlainAggregation {
    pub fn new(
        entries: Vec<TimeIdx>,
        window_expr: Arc<dyn WindowExpr>,
        tiles: Option<Tiles>,
        time_entries: &TimeEntries,
    ) -> Self {
        let (relevant_time_entries, batches_to_load) = Self::get_relevant_entries(&window_expr, &entries, time_entries, tiles.as_ref());
        Self {
            entries,
            window_expr,
            tiles,
            relevant_time_entries,
            batches_to_load,
        }
    }

    // create a copy of entries needed for this aggregation so we do not copy all the stored data
    fn get_relevant_entries(window_expr: &Arc<dyn WindowExpr>, entries: &Vec<TimeIdx>, time_entries: &TimeEntries, tiles: Option<&Tiles>) -> (TimeEntries, IndexSet<BatchId>) {
        let mut inserted = HashSet::new();

        let relevant_time_entries = TimeEntries::new();
        let mut batches_to_load = IndexSet::new();
        // TODO is there a more efficient way, e.g finding overlaping ranges so we do not go over the same entries multiple times
        for entry in entries {
            let window_start = time_entries.get_window_start(window_expr.get_window_frame(), *entry, true)
                .expect("Time entries should exist");
            
            let (front_padding, _, back_padding) = time_entries.get_entries_in_range(tiles, window_start, *entry);
            for entry in front_padding {
                let k = (entry.timestamp, entry.pos_idx);
                if !inserted.contains(&k) {
                    inserted.insert(k);
                    relevant_time_entries.entries.insert(entry);
                    batches_to_load.insert(entry.batch_id);
                }
            }
            for entry in back_padding {
                let k = (entry.timestamp, entry.pos_idx);
                if !inserted.contains(&k) {
                    inserted.insert(k);
                    relevant_time_entries.entries.insert(entry);
                    batches_to_load.insert(entry.batch_id);
                }
            }
        }
        

        (relevant_time_entries, batches_to_load)
    }    
    
}

#[async_trait]
impl Aggregation for PlainAggregation {
    async fn produce_aggregates(
        &self,
        batches: &HashMap<BatchId, RecordBatch>,
        thread_pool: Option<&ThreadPool>,
        exclude_current_row: Option<bool>,
    ) -> (Vec<ScalarValue>, Option<AccumulatorState>) {
        let parallelize = thread_pool.is_some();
        let mut aggregates = Vec::new();
            
        // plain needs to run aggregation for each entry and corresponding values in a window ending at this entry
        for entry in &self.entries {
            let window_start = self.relevant_time_entries.get_window_start(self.window_expr.get_window_frame(), *entry, true)
                .expect("Time entries should exist");
            let (mut front_padding, middle_tiles, back_padding) = self.relevant_time_entries.get_entries_in_range(self.tiles.as_ref(), window_start, *entry);

            // in request mode, if specified, exclude the virtual current row from aggregation
            if exclude_current_row.is_some() && !exclude_current_row.unwrap() {
                front_padding.push(entry.clone());
            }
            
            let (aggregate_for_entry, _) = if parallelize {
                run_plain_accumulator_parallel(
                    thread_pool.expect("ThreadPool should exist"), 
                    self.window_expr.clone(), 
                    front_padding, 
                    middle_tiles, 
                    back_padding, 
                    batches.clone()
                ).await
            } else {
                run_plain_accumulator(
                    &self.window_expr, 
                    front_padding, 
                    middle_tiles, 
                    back_padding, 
                    batches
                )
            };
            
            aggregates.push(aggregate_for_entry);
        }
        
        (aggregates, None)
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
        AggregatorType::PlainAccumulator
    }
    
    fn get_batches_to_load(&self) -> IndexSet<BatchId> {
        self.batches_to_load.clone()
    }
}

// aggregates a range of values (front_entries+middle_tiles+back_entries) into a single value
// by creating a temporary accumulator, updating it and getting result.
fn run_plain_accumulator(
    window_expr: &Arc<dyn WindowExpr>, 
    front_entries: Vec<TimeIdx>, 
    middle_tiles: Vec<Tile>, 
    back_entries: Vec<TimeIdx>, 
    batches: &HashMap<BatchId, RecordBatch>
) -> (ScalarValue, AccumulatorState) {
    let mut accumulator = match create_window_aggregator(&window_expr) {
        WindowAggregator::Accumulator(accumulator) => accumulator,
        WindowAggregator::Evaluator(_) => panic!("Should not be evaluator"),
    };

    // Process front entries using batch-wise approach
    if !front_entries.is_empty() {
        let front_args = get_plain_args(&front_entries, batches, window_expr)
            .expect("Should be able to get front entry args");
        accumulator.update_batch(&front_args)
            .expect("Should be able to update accumulator with front entries");
    }

    // Process middle tiles
    for tile in middle_tiles {
        if let Some(tile_state) = tile.accumulator_state {
            merge_accumulator_state(accumulator.as_mut(), tile_state.as_ref());
        }
    }

    // Process back entries using batch-wise approach
    if !back_entries.is_empty() {
        let back_args = get_plain_args(&back_entries, batches, window_expr)
            .expect("Should be able to get back entry args");
        accumulator.update_batch(&back_args)
            .expect("Should be able to update accumulator with back entries");
    }

    (
        accumulator.evaluate()
            .expect("Should be able to evaluate accumulator"), 
        accumulator
            .state()
            .expect("Should be able to get accumulator state")
    )
}

async fn run_plain_accumulator_parallel(
    thread_pool: &ThreadPool,
    window_expr: Arc<dyn WindowExpr>, 
    front_entries: Vec<TimeIdx>, 
    middle_tiles: Vec<Tile>, 
    back_entries: Vec<TimeIdx>, 
    batches: HashMap<BatchId, RecordBatch>
) -> (ScalarValue, AccumulatorState) {
    thread_pool.spawn_fifo_async(move || {
        run_plain_accumulator(&window_expr, front_entries, middle_tiles, back_entries, &batches)
    }).await
}


// Get combined arguments for a list of entries - concatenates all entries into single arrays
fn get_plain_args(
    entries: &[TimeIdx],
    batches: &HashMap<BatchId, RecordBatch>,
    window_expr: &Arc<dyn WindowExpr>
) -> Result<Vec<arrow::array::ArrayRef>, Box<dyn std::error::Error>> {
    use std::collections::HashMap;
    
    if entries.is_empty() {
        return Ok(Vec::new());
    }

    // Group entries by batch_id
    let mut indices_per_batch: HashMap<BatchId, Vec<(usize, usize)>> = HashMap::new();
    
    for (pos, time_idx) in entries.iter().enumerate() {
        indices_per_batch
            .entry(time_idx.batch_id)
            .or_default()
            .push((pos, time_idx.row_idx));
    }

    // Collect all individual args in order
    let mut individual_args: Vec<Vec<arrow::array::ArrayRef>> = vec![Vec::new(); entries.len()];

    evaluate_batches_and_map_results(
        indices_per_batch,
        batches,
        window_expr,
        |pos, single_row_args| {
            individual_args[pos] = single_row_args;
        }
    )?;
    
    // If only one entry, return its args directly
    if individual_args.len() == 1 {
        return Ok(individual_args.into_iter().next().unwrap());
    }
    
    // Concatenate all individual args into combined args
    let num_columns = individual_args[0].len();
    let mut combined_args = Vec::with_capacity(num_columns);
    
    for col_idx in 0..num_columns {
        let arrays_to_concat: Vec<&dyn arrow::array::Array> = individual_args
            .iter()
            .map(|args| args[col_idx].as_ref())
            .collect();
        
        let concatenated = arrow::compute::concat(&arrays_to_concat)
            .expect("Should be able to concatenate arrays");
        combined_args.push(concatenated);
    }
    
    Ok(combined_args)
}