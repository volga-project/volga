use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::ArrayRef;
use datafusion::logical_expr::Accumulator;
use datafusion::physical_expr::window::{PlainAggregateWindowExpr, SlidingAggregateWindowExpr};
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use async_trait::async_trait;

use tokio_rayon::rayon::ThreadPool;

use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::storage::batch::Timestamp;
use crate::storage::index::{DataRequest, SortedRangeView};
use crate::runtime::operators::window::Cursor;
use crate::runtime::operators::window::cate::types::{AggFlavor, CATE_KINDS};
use crate::runtime::operators::window::top::accumulators::frequency::{
    TOP1_RATIO_NAME, TOPN_FREQUENCY_NAME,
};
use crate::runtime::operators::window::top::accumulators::ratio::{
    TOP_N_KEY_RATIO_CATE_NAME, TOP_N_VALUE_RATIO_CATE_NAME,
};
use crate::runtime::operators::window::top::accumulators::value::TOP_NAME;

pub mod arrow_utils;
pub mod evaluator;
pub mod plain;
pub mod retractable;
pub mod point_request_merge;

pub use crate::storage::index::BucketRange;

/// A single "virtual" point for request-mode window evaluation.
#[derive(Debug, Clone)]
pub struct VirtualPoint {
    pub ts: Timestamp,
    /// Optional pre-evaluated args for this single row (length=1 arrays).
    pub args: Option<Arc<Vec<ArrayRef>>>,
}

#[derive(Debug, Clone)]
pub struct AggregationExecResult {
    pub values: Vec<ScalarValue>,
    pub accumulator_state: Option<AccumulatorState>,
    /// Exact cursor that was actually processed (must point to a real row). Only produced by range aggregations.
    pub processed_pos: Option<Cursor>,
}

#[async_trait]
pub trait Aggregation: Send + Sync {
    fn window_expr(&self) -> &Arc<dyn WindowExpr>;
    fn aggregator_type(&self) -> AggregatorType;

    /// Row-level data requests for request-mode (range views).
    fn get_data_requests(&self) -> Vec<DataRequest>;

    /// Produce aggregates from preloaded `SortedRangeView`s corresponding to `get_data_requests(...)`.
    async fn produce_aggregates_from_ranges(
        &self,
        sorted_ranges: &[SortedRangeView],
        thread_pool: Option<&ThreadPool>,
    ) -> AggregationExecResult {
        let _ = sorted_ranges;
        let _ = thread_pool;
        panic!("Aggregation must implement produce_aggregates_from_ranges")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregatorType {
    PlainAccumulator, // incremental updates, runs on whole window, supports tiling
    RetractableAccumulator, // incremental updates and retracts, only updates state with new events
    Evaluator, // evaluates whole window
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggKind {
    Sum,
    Avg,
    Count,
    Min,
    Max,
    Stddev,
    StddevPop,
    StddevSamp,
    VarPop,
    VarSamp,
    Variance,
}

impl AggKind {
    pub const ALL: [AggKind; 11] = [
        AggKind::Sum,
        AggKind::Avg,
        AggKind::Count,
        AggKind::Min,
        AggKind::Max,
        AggKind::Stddev,
        AggKind::StddevPop,
        AggKind::StddevSamp,
        AggKind::VarPop,
        AggKind::VarSamp,
        AggKind::Variance,
    ];

    pub fn name(&self) -> &'static str {
        match self {
            AggKind::Sum => "sum",
            AggKind::Avg => "avg",
            AggKind::Count => "count",
            AggKind::Min => "min",
            AggKind::Max => "max",
            AggKind::Stddev => "stddev",
            AggKind::StddevPop => "stddev_pop",
            AggKind::StddevSamp => "stddev_samp",
            AggKind::VarPop => "var_pop",
            AggKind::VarSamp => "var_samp",
            AggKind::Variance => "variance",
        }
    }

    pub fn aggregator_type(&self) -> AggregatorType {
        match self {
            AggKind::Min | AggKind::Max => AggregatorType::PlainAccumulator,
            _ => AggregatorType::RetractableAccumulator,
        }
    }
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
        for kind in AggKind::ALL {
            self.register_aggregate(kind.name(), kind.aggregator_type());
        }

        for kind in CATE_KINDS {
            for flavor in AggFlavor::ALL {
                let name = format!("{}{}", kind.name(), flavor.suffix());
                self.register_aggregate(&name, kind.aggregator_type());
            }
        }

        for name in [
            TOP_NAME,
            TOP1_RATIO_NAME,
            TOPN_FREQUENCY_NAME,
            TOP_N_KEY_RATIO_CATE_NAME,
            TOP_N_VALUE_RATIO_CATE_NAME,
        ] {
            self.register_aggregate(name, AggregatorType::RetractableAccumulator);
        }

        for kind in CATE_KINDS {
            let name = format!("top_n_key_{}_cate_where", kind.name());
            self.register_aggregate(&name, kind.aggregator_type());
            let name = format!("top_n_value_{}_cate_where", kind.name());
            self.register_aggregate(&name, kind.aggregator_type());
        }
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

#[cfg(test)]
pub(crate) mod test_utils {
    use std::sync::Arc;
    use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_plan::windows::BoundedWindowAggExec;
    use datafusion::physical_plan::WindowExpr;
    use datafusion::prelude::SessionContext;

    use crate::api::planner::{Planner, PlanningContext};
    use crate::runtime::functions::key_by::key_by_function::extract_datafusion_window_exec;
    use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
    use crate::storage::index::{DataRequest, SortedRangeView, SortedSegment};
    use crate::runtime::operators::window::{Cursor, TimeGranularity, SEQ_NO_COLUMN_NAME};
    use crate::storage::batch::Timestamp;

    pub fn test_schema_with_seq() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("value", DataType::Float64, false),
            Field::new("partition_key", DataType::Utf8, false),
            Field::new(SEQ_NO_COLUMN_NAME, DataType::UInt64, false),
        ]))
    }

    pub fn batch(rows: &[(Timestamp, f64, &str, u64)]) -> RecordBatch {
        let schema = test_schema_with_seq();
        let ts = Arc::new(TimestampMillisecondArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>()));
        let v = Arc::new(Float64Array::from(rows.iter().map(|r| r.1).collect::<Vec<_>>()));
        let pk = Arc::new(StringArray::from(rows.iter().map(|r| r.2).collect::<Vec<_>>()));
        let seq = Arc::new(UInt64Array::from(rows.iter().map(|r| r.3).collect::<Vec<_>>()));
        RecordBatch::try_new(schema, vec![ts, v, pk, seq]).expect("test batch")
    }

    pub fn one_row_batch(ts: Timestamp, value: f64, pk: &str, seq: u64) -> RecordBatch {
        batch(&[(ts, value, pk, seq)])
    }

    pub async fn window_exec_from_sql(sql: &str) -> Arc<BoundedWindowAggExec> {
        let ctx = SessionContext::new();
        let mut planner = Planner::new(PlanningContext::new(ctx));
        planner.register_source(
            "test_table".to_string(),
            SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])),
            test_schema_with_seq(),
        );
        extract_datafusion_window_exec(sql, &mut planner).await
    }

    pub async fn window_expr_from_sql(sql: &str) -> Arc<dyn WindowExpr> {
        let exec = window_exec_from_sql(sql).await;
        exec.window_expr()[0].clone()
    }

    pub fn make_view(
        granularity: TimeGranularity,
        request: DataRequest,
        buckets: Vec<(Timestamp, RecordBatch)>,
        window_expr_for_args: &Arc<dyn WindowExpr>,
    ) -> SortedRangeView {
        let (start, end) = match request.bounds {
            crate::storage::index::DataBounds::All => {
                (Cursor::new(i64::MIN, 0), Cursor::new(i64::MAX, u64::MAX))
            }
            crate::storage::index::DataBounds::Time { start_ts, end_ts } => {
                (Cursor::new(start_ts, 0), Cursor::new(end_ts, u64::MAX))
            }
            crate::storage::index::DataBounds::RowsTail { end_ts, .. } => {
                (Cursor::new(i64::MIN, 0), Cursor::new(end_ts, u64::MAX))
            }
        };

        let mut out: Vec<SortedSegment> = Vec::new();
        for (bucket_ts, b) in buckets {
            let args = window_expr_for_args.evaluate_args(&b).expect("eval args");
            out.push(SortedSegment::new(bucket_ts, b, 0, 3, Arc::new(args)));
        }
        out.sort_by_key(|b| b.bucket_ts());
        SortedRangeView::new(request, granularity, start, end, out, None, None)
    }
}

#[cfg(test)]
mod agg_tests;
