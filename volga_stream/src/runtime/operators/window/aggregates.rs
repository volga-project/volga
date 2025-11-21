use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{RecordBatch, UInt64Array};
use datafusion::logical_expr::Accumulator;
use datafusion::physical_expr::window::{PlainAggregateWindowExpr, SlidingAggregateWindowExpr};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregatorType {
    PlainAccumulator, // incremental updates, runs on whole window, supports tiling
    RetractableAccumulator, // incremental updates and retracts, only updates state with new events
    Evaluator, // evaluates whole window
}

pub trait Evaluator: Send + Sync {
    fn evaluate(&self, values: &[ScalarValue]) -> ScalarValue;
    fn name(&self) -> &str;
}

pub struct AggregateRegistry {
    aggregate_types: HashMap<String, AggregatorType>,
    evaluators: HashMap<String, Arc<dyn Evaluator>>,
}

impl Default for AggregateRegistry {
    fn default() -> Self {
        let mut registry = Self {
            aggregate_types: HashMap::new(),
            evaluators: HashMap::new(),
        };
        
        registry.register_supported_aggregates();
        registry
    }
}

impl AggregateRegistry {
    pub fn new() -> Self {
        Self {
            aggregate_types: HashMap::new(),
            evaluators: HashMap::new(),
        }
    }
    
    fn register_supported_aggregates(&mut self) {
        self.register_aggregate("min", AggregatorType::PlainAccumulator);
        self.register_aggregate("max", AggregatorType::PlainAccumulator);

        self.register_aggregate("count", AggregatorType::RetractableAccumulator);
        self.register_aggregate("sum", AggregatorType::RetractableAccumulator);
        self.register_aggregate("avg", AggregatorType::RetractableAccumulator);

        self.register_aggregate("stddev", AggregatorType::RetractableAccumulator);
        self.register_aggregate("stddev_pop", AggregatorType::RetractableAccumulator);
        self.register_aggregate("stddev_samp", AggregatorType::RetractableAccumulator);
        self.register_aggregate("var_pop", AggregatorType::RetractableAccumulator);
        self.register_aggregate("var_samp", AggregatorType::RetractableAccumulator);
        self.register_aggregate("variance", AggregatorType::RetractableAccumulator);
    }
    
    fn register_aggregate(&mut self, name: &str, aggregator_type: AggregatorType) {
        self.aggregate_types.insert(name.to_lowercase(), aggregator_type);
    }
    
    fn _register_evaluator(&mut self, name: &str, evaluator: Arc<dyn Evaluator>) {
        self.aggregate_types.insert(name.to_lowercase(), AggregatorType::Evaluator);
        self.evaluators.insert(name.to_lowercase(), evaluator);
    }
    
    pub fn get_aggregator_type(&self, name: &str) -> Option<AggregatorType> {
        self.aggregate_types.get(&name.to_lowercase()).copied()
    }
    
    pub fn get_evaluator(&self, name: &str) -> Option<Arc<dyn Evaluator>> {
        self.evaluators.get(&name.to_lowercase()).cloned()
    }
    
    pub fn is_supported(&self, name: &str) -> bool {
        self.aggregate_types.contains_key(&name.to_lowercase())
    }
    
    pub fn supported_functions(&self) -> Vec<String> {
        self.aggregate_types.keys().cloned().collect()
    }
}

pub enum WindowAggregator {
    Accumulator(Box<dyn Accumulator>),
    Evaluator(Arc<dyn Evaluator>),
}

pub fn create_window_aggregator(window_expr: &Arc<dyn WindowExpr>) -> WindowAggregator {
    let registry = get_aggregate_registry();
    let agg_expr = extract_aggregate_expr(window_expr);
    let agg_name = agg_expr.fun().name();
    let aggregator_type = registry
        .get_aggregator_type(&agg_name)
        .expect(&format!("Unsupported aggregate function: {}", agg_name));
    
    match aggregator_type {
        AggregatorType::Evaluator => {
            let evaluator = registry
                .get_evaluator(&agg_name)
                .expect(&format!("No evaluator registered for: {}", agg_name));
            WindowAggregator::Evaluator(evaluator)
        }
        AggregatorType::PlainAccumulator => {
            let accumulator = agg_expr.create_accumulator()
                .expect("Failed to create plain accumulator");
            WindowAggregator::Accumulator(accumulator)
        }
        AggregatorType::RetractableAccumulator => {
            let accumulator = agg_expr.create_sliding_accumulator()
                .expect("Failed to create retractable accumulator");
            WindowAggregator::Accumulator(accumulator)
        }
    }
}

pub fn merge_accumulator_state(accumulator: &mut dyn Accumulator, accumulator_state: &AccumulatorState) {
    let state_arrays: Vec<arrow::array::ArrayRef> = accumulator_state
        .iter()
        .map(|scalar| scalar.to_array_of_size(1).expect("Failed to convert scalar to array"))
        .collect();
    
    accumulator.merge_batch(&state_arrays).expect("Failed to merge accumulator state");
}

fn extract_aggregate_expr(
    window_expr: &Arc<dyn WindowExpr>,
) -> &datafusion::physical_expr::aggregate::AggregateFunctionExpr {
    if let Some(plain_expr) = window_expr.as_any().downcast_ref::<PlainAggregateWindowExpr>() {
        return plain_expr.get_aggregate_expr();
    }
    
    if let Some(sliding_expr) = window_expr.as_any().downcast_ref::<SlidingAggregateWindowExpr>() {
        return sliding_expr.get_aggregate_expr();
    }
    
    panic!("Window expression is neither PlainAggregateWindowExpr nor SlidingAggregateWindowExpr")
}

pub fn get_aggregate_registry() -> &'static AggregateRegistry {
    static REGISTRY: std::sync::OnceLock<AggregateRegistry> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(AggregateRegistry::default)
}

pub fn get_aggregate_type(window_expr: &Arc<dyn WindowExpr>) -> AggregatorType {
    let registry = get_aggregate_registry();
    let agg_expr = extract_aggregate_expr(window_expr);
    let agg_name = agg_expr.fun().name();
    registry.get_aggregator_type(&agg_name).expect(&format!("Unsupported aggregate function: {}", agg_name))
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

    fn retract(window_expr: &Arc<dyn WindowExpr>, accumulator: &mut dyn Accumulator, retract_idxs: &Vec<TimeIdx>, batches: &HashMap<BatchId, RecordBatch>) {
        if !retract_idxs.is_empty() {
            // Extract retract rows in original order
            let mut retract_batches = Vec::new();
            for retract_idx in retract_idxs {
                let batch = batches.get(&retract_idx.batch_id).expect("Retract batch should exist");
                let indices_array = UInt64Array::from(vec![retract_idx.row_idx as u64]);
                let retract_batch = arrow::compute::take_record_batch(batch, &indices_array)
                    .expect("Should be able to take row from batch");
                retract_batches.push(retract_batch);
            }

            // Concatenate retract batches if multiple, otherwise use single batch
            let final_retract_batch = if retract_batches.len() == 1 {
                retract_batches.into_iter().next().unwrap()
            } else {
                let schema = retract_batches[0].schema();
                arrow::compute::concat_batches(&schema, &retract_batches)
                    .expect("Should be able to concat retract batches")
            };

            let retract_args = window_expr.evaluate_args(&final_retract_batch)
                .expect("Should be able to evaluate retract args");

            accumulator.retract_batch(&retract_args).expect("Should be able to retract from accumulator");
        }
    }

    let mut accumulator = match create_window_aggregator(&window_expr) {
        WindowAggregator::Accumulator(accumulator) => accumulator,
        WindowAggregator::Evaluator(_) => panic!("Evaluator is not supported for retractable accumulator"),
    };

    if let Some(accumulator_state) = accumulator_state {
        merge_accumulator_state(accumulator.as_mut(), accumulator_state);
    }

    let mut results = Vec::new();

    if let Some(updates) = updates {
        for i in 0..updates.len() {
            let update_idx = &updates[i];
            let retract_idxs = if let Some(ref retracts) = retracts {
                &retracts[i]
            } else {
                &Vec::new()
            };
            let update_batch = batches.get(&update_idx.batch_id).expect(&format!("Update batch should exist for {:?}", update_idx));
            // Extract single row from update batch using the row index
            let update_row_batch = {
                let indices = UInt64Array::from(vec![update_idx.row_idx as u64]);
                arrow::compute::take_record_batch(update_batch, &indices)
                    .expect("Should be able to take row from batch")
            };

            let update_args = window_expr.evaluate_args(&update_row_batch)
                .expect("Should be able to evaluate window args");

            accumulator.update_batch(&update_args).expect("Should be able to update accumulator");

            // Handle retractions
            retract(window_expr, accumulator.as_mut(), retract_idxs, batches);

            let result = accumulator.evaluate().expect("Should be able to evaluate accumulator");
            results.push(result);
        } 
    } else {
        // Special case - we have a single retracts range to run
        let retracts_vec = retracts.expect("Retracts should exist");
        let retract_idxs = retracts_vec.first().expect("Should have at least one retract");
        retract(window_expr, accumulator.as_mut(), retract_idxs, batches);
        let result = accumulator.evaluate().expect("Should be able to evaluate accumulator");
        results.push(result);
    }

    (results, accumulator.state().expect("Should be able to get accumulator state"))
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

    // TODO single update?
    // Process front entries one by one
    for entry in front_entries {
        let batch = batches.get(&entry.batch_id).expect("Batch should exist");
        let indices = UInt64Array::from(vec![entry.row_idx as u64]);
        let entry_batch = arrow::compute::take_record_batch(batch, &indices)
            .expect("Should be able to take row from batch");
        
        let args = window_expr.evaluate_args(&entry_batch)
            .expect("Should be able to evaluate window args");
        accumulator.update_batch(&args)
            .expect("Should be able to update accumulator");
    }

    // Process middle tiles
    for tile in middle_tiles {
        if let Some(tile_state) = tile.accumulator_state {
            merge_accumulator_state(accumulator.as_mut(), tile_state.as_ref());
        }
    }

    // TODO single update?
    // Process back entries one by one
    for entry in back_entries {
        let batch = batches.get(&entry.batch_id).expect("Batch should exist");
        let indices = UInt64Array::from(vec![entry.row_idx as u64]);
        let entry_batch = arrow::compute::take_record_batch(batch, &indices)
            .expect("Should be able to take row from batch");
        
        let args = window_expr.evaluate_args(&entry_batch)
            .expect("Should be able to evaluate window args");
        accumulator.update_batch(&args)
            .expect("Should be able to update accumulator");
    }

    (
        accumulator.evaluate()
            .expect("Should be able to evaluate accumulator"), 
        accumulator.state()
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

fn run_evaluator(
    _window_expr: &Arc<dyn WindowExpr>, 
    _entries: Vec<TimeIdx>, 
    _batches: &HashMap<BatchId, RecordBatch>
) -> ScalarValue {
    panic!("Not implemented");
}

#[async_trait]
pub trait Aggregation: Send + Sync {
    async fn produce_aggregates(
        &self,
        batches: &HashMap<BatchId, RecordBatch>,
        thread_pool: Option<&ThreadPool>,
        exclude_current_row: Option<bool>,
    ) -> (Vec<ScalarValue>, Option<AccumulatorState>);
    
    fn entries(&self) -> &[TimeIdx];
    fn window_expr(&self) -> &Arc<dyn WindowExpr>;
    fn tiles(&self) -> Option<&Tiles>;
    fn aggregator_type(&self) -> AggregatorType;
    
    fn get_batches_to_load(&self) -> IndexSet<BatchId>;
}


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
        // TODO is there a more efficient way, e.g finding overlaping ranges so we do not go over the same entres twice
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
        // let mut batches_to_load = IndexSet::new();
        let _window_frame = self.window_expr.get_window_frame();
        
        // // non-rets (plain and evaluators) need whole window data (excluding tiles) for each update/entry
        // for entry in &self.entries {
        //     let window_start = self.time_entries.get_window_start(window_frame, *entry, true)
        //         .expect("Time entries should exist");
        //     let (front, _tiles, end) = self.time_entries.get_entries_in_range(self.tiles.as_ref(), window_start, *entry);
        //     for entry in front {
        //         batches_to_load.insert(entry.batch_id);
        //     }
        //     for entry in end {
        //         batches_to_load.insert(entry.batch_id);
        //     }
        // }
        // for entry in self.relevant_time_entries.entries.iter() {
        //     batches_to_load.insert(entry.batch_id);
        // }
        
        self.batches_to_load.clone()
    }
}

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

// optimization - plain aggregator and evaluators produce aggregation results
// by running nested for loop for each entry.
// we split entries where possible, so that nested loops can run on different threads
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggParallelismSplit {
    None,
    CpuBased,
    PerEvent,
}

// default to CPU-based splitting
const AGG_PARALLELISM_SPLIT: AggParallelismSplit = AggParallelismSplit::CpuBased;

pub fn split_entries_for_parallelism(entries: &Vec<TimeIdx>) -> Vec<Vec<TimeIdx>> {
    if entries.is_empty() {
        return vec![];
    }
    
    match AGG_PARALLELISM_SPLIT {
        AggParallelismSplit::None => {
            vec![entries.clone()]
        }
        AggParallelismSplit::CpuBased => {
            let num_threads = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
                .max(1);
            
            // For small number of entries, don't split (overhead not worth it)
            if entries.len() < num_threads * 2 {
                return vec![entries.clone()];
            }
            
            // Split into chunks of roughly equal size
            let chunk_size = (entries.len() + num_threads - 1) / num_threads;
            entries.chunks(chunk_size).map(|chunk| chunk.to_vec()).collect()
        }
        AggParallelismSplit::PerEvent => {
            entries.iter().map(|entry| vec![*entry]).collect()
        }
    }
}
