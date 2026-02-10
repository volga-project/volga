use std::sync::Arc;

use async_trait::async_trait;
use datafusion::physical_plan::WindowExpr;
use tokio_rayon::rayon::ThreadPool;

use crate::storage::index::{BucketIndex, DataRequest, SortedRangeView};
use crate::storage::TimeGranularity;
use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::runtime::operators::window::Cursor;

use super::VirtualPoint;
use super::{Aggregation, AggregationExecResult, AggregatorType, BucketRange};

#[path = "retractable_range.rs"]
mod retractable_range;
#[path = "retractable_points.rs"]
mod retractable_points;

use retractable_points::RetractablePointsAggregation;
use retractable_range::RetractableRangeAggregation;

#[derive(Debug)]
pub enum RetractableAggregation {
    Range(RetractableRangeAggregation),
    Points(RetractablePointsAggregation),
}

impl RetractableAggregation {
    pub fn from_range(
        window_id: usize,
        prev_processed_pos: Option<Cursor>,
        advance_to: Cursor,
        bucket_index: &BucketIndex,
        window_expr: Arc<dyn WindowExpr>,
        accumulator_state: Option<AccumulatorState>,
        ts_column_index: usize,
        bucket_granularity: TimeGranularity,
    ) -> Self {
        Self::Range(RetractableRangeAggregation::new(
            window_id,
            prev_processed_pos,
            advance_to,
            bucket_index,
            window_expr,
            accumulator_state,
            ts_column_index,
            bucket_granularity,
        ))
    }

    pub fn for_points(
        points: Vec<VirtualPoint>,
        bucket_index: &BucketIndex,
        window_expr: Arc<dyn WindowExpr>,
        processed_pos: Option<Cursor>,
        accumulator_state: Option<AccumulatorState>,
        exclude_current_row: bool,
    ) -> Self {
        Self::Points(RetractablePointsAggregation::new(
            points,
            bucket_index,
            window_expr,
            processed_pos,
            accumulator_state,
            exclude_current_row,
        ))
    }
}

#[async_trait]
impl Aggregation for RetractableAggregation {
    fn window_expr(&self) -> &Arc<dyn WindowExpr> {
        match self {
            RetractableAggregation::Range(r) => r.window_expr(),
            RetractableAggregation::Points(p) => p.window_expr(),
        }
    }

    fn aggregator_type(&self) -> AggregatorType {
        AggregatorType::RetractableAccumulator
    }

    fn get_data_requests(&self) -> Vec<DataRequest> {
        match self {
            RetractableAggregation::Range(r) => r.get_data_requests(),
            RetractableAggregation::Points(p) => p.get_data_requests(),
        }
    }

    async fn produce_aggregates_from_ranges(
        &self,
        sorted_ranges: &[SortedRangeView],
        thread_pool: Option<&ThreadPool>,
    ) -> AggregationExecResult {
        match self {
            RetractableAggregation::Range(r) => r.produce_aggregates_from_ranges(sorted_ranges, thread_pool).await,
            RetractableAggregation::Points(p) => p.produce_aggregates_from_ranges(sorted_ranges, thread_pool).await,
        }
    }
}
