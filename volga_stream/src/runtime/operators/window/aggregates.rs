use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{RecordBatch, UInt64Array};
use crossbeam_skiplist::SkipSet;
use datafusion::logical_expr::Accumulator;
use datafusion::physical_expr::window::{PlainAggregateWindowExpr, SlidingAggregateWindowExpr};
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use tokio_rayon::rayon::{ThreadPool, ThreadPoolBuilder};
use tokio_rayon::AsyncThreadPool;

use crate::runtime::operators::window::state::{AccumulatorState, WindowState};
use crate::runtime::operators::window::tiles::Tile;
use crate::runtime::operators::window::time_index::{get_tiled_range, get_window_start_idx, TimeIdx};
use crate::storage::storage::BatchId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregatorType {
    PlainAccumulator, // incremental updates
    RetractableAccumulator, // incremental updates and retracts
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
        
        registry.register_suppoerted_aggregates();
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
    
    fn register_suppoerted_aggregates(&mut self) {
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
    
    fn register_evaluator(&mut self, name: &str, evaluator: Arc<dyn Evaluator>) {
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
    // let agg_name = window_expr.name();
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

pub fn merge_accumulator_state(accumulator: &mut dyn Accumulator, accumulator_state: AccumulatorState) {
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

pub fn run_retractable_accumulator(
    window_expr: &Arc<dyn WindowExpr>, 
    updates: Vec<TimeIdx>, 
    retracts: Option<Vec<Vec<TimeIdx>>>, 
    batches: &HashMap<BatchId, RecordBatch>,
    accumulator_state: Option<AccumulatorState>
) -> (Vec<ScalarValue>, AccumulatorState) {
    let mut accumulator = match create_window_aggregator(&window_expr) {
        WindowAggregator::Accumulator(accumulator) => accumulator,
        WindowAggregator::Evaluator(_) => panic!("Evaluator is not supported for retractable accumulator"),
    };

    if let Some(accumulator_state) = accumulator_state {
        merge_accumulator_state(accumulator.as_mut(), accumulator_state);
    }

    let mut results = Vec::new();

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

        let result = accumulator.evaluate().expect("Should be able to evaluate accumulator");
        results.push(result);
    }

    (results, accumulator.state().expect("Should be able to get accumulator state"))
}

pub async fn run_retractable_accumulator_parallel(
    thread_pool: &ThreadPool,
    window_expr: Arc<dyn WindowExpr>, 
    updates: Vec<TimeIdx>, 
    retracts: Option<Vec<Vec<TimeIdx>>>, 
    batches: HashMap<BatchId, RecordBatch>,
    previous_accumulator_state: Option<AccumulatorState>
) -> (Vec<ScalarValue>, AccumulatorState) {
    thread_pool.spawn_fifo_async(move || {
        run_retractable_accumulator(&window_expr, updates, retracts, &batches, previous_accumulator_state)
    }).await
}

pub fn run_plain_accumulator(
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
            merge_accumulator_state(accumulator.as_mut(), tile_state);
        }
    }

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

pub async fn run_plain_accumulator_parallel(
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

pub fn run_evaluator(
    _window_expr: &Arc<dyn WindowExpr>, 
    _entries: Vec<TimeIdx>, 
    _batches: &HashMap<BatchId, RecordBatch>
) -> ScalarValue {
    panic!("Not implemented");
}

pub async fn produce_aggregates(
    window_expr: &Arc<dyn WindowExpr>,
    window_state: Option<&WindowState>,
    entries: &Vec<TimeIdx>, 
    time_entries: Arc<SkipSet<TimeIdx>>,
    batches: &HashMap<BatchId, RecordBatch>,
    thread_pool: Option<&ThreadPool>,
    parallelize: bool
) -> Vec<ScalarValue> {
    let mut aggregates = Vec::new();
    // rebuild events, use tiles if possible
    for entry in entries {
        let window_start = get_window_start_idx(window_expr.get_window_frame(), *entry, &time_entries).unwrap_or(time_entries.front().expect("Time entries should exist").value().clone());
        let tiled_range = get_tiled_range(window_state, &time_entries, window_start, *entry);
        let (aggregate_for_entry, _) = if parallelize {
            run_plain_accumulator_parallel(thread_pool.expect("ThreadPool should exist"), window_expr.clone(), tiled_range.0, tiled_range.1, tiled_range.2, batches.clone()).await
        } else {
            run_plain_accumulator(&window_expr.clone(), tiled_range.0, tiled_range.1, tiled_range.2, batches)
        };
        aggregates.push(aggregate_for_entry);
    }

    aggregates
}