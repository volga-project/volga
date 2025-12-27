use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::logical_expr::Accumulator;
use datafusion::physical_expr::window::{PlainAggregateWindowExpr, SlidingAggregateWindowExpr};
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use async_trait::async_trait;

use tokio_rayon::rayon::ThreadPool;

use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::runtime::operators::window::time_entries::TimeIdx;
use crate::runtime::operators::window::Tiles;
use crate::storage::batch_store::{BatchId, Timestamp};
use indexmap::IndexSet;

pub mod arrow_utils;
pub mod evaluator;
pub mod plain;
pub mod retractable;
pub mod utils;

use self::utils::SortedBucketView;

/// A logical bucket interval in bucket timestamp space (inclusive).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketRange {
    pub start: Timestamp,
    pub end: Timestamp,
}

impl BucketRange {
    pub fn new(start: Timestamp, end: Timestamp) -> Self {
        Self { start, end }
    }
}

#[async_trait]
pub trait Aggregation: Send + Sync {
    async fn produce_aggregates(
        &self,
        sorted_bucket_view: &HashMap<Timestamp, SortedBucketView>,
        thread_pool: Option<&ThreadPool>,
        exclude_current_row: Option<bool>,
    ) -> (Vec<ScalarValue>, Option<AccumulatorState>);
    
    fn window_expr(&self) -> &Arc<dyn WindowExpr>;
    fn tiles(&self) -> Option<&Tiles>;
    fn aggregator_type(&self) -> AggregatorType;
    
    // fn get_batches_to_load(&self) -> IndexSet<BatchId>;

    fn get_relevant_buckets(&self) -> Vec<BucketRange>;
}

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

    // println!("Agg type {:?} for {agg_name}", aggregator_type);
    
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
