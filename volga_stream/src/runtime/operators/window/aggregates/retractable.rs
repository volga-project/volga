use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use tokio_rayon::rayon::ThreadPool;

use crate::runtime::operators::window::index::{BucketIndex, SlideRangeInfo};
use crate::runtime::operators::window::tiles::TimeGranularity;
use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::runtime::operators::window::{Cursor, Tiles};
use crate::storage::batch_store::Timestamp;

use super::VirtualPoint;
use super::{Aggregation, AggregatorType, BucketRange};

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
        slide_info: &SlideRangeInfo,
        prev_processed_until: Option<Cursor>,
        window_expr: Arc<dyn WindowExpr>,
        accumulator_state: Option<AccumulatorState>,
        ts_column_index: usize,
        bucket_granularity: TimeGranularity,
    ) -> Self {
        Self::Range(RetractableRangeAggregation::new(
            window_id,
            slide_info,
            prev_processed_until,
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
        ts_column_index: usize,
        window_id: usize,
        processed_until: Option<Cursor>,
        accumulator_state: Option<AccumulatorState>,
    ) -> Self {
        Self::Points(RetractablePointsAggregation::new(
            points,
            bucket_index,
            window_expr,
            ts_column_index,
            window_id,
            processed_until,
            accumulator_state,
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

    fn get_data_requests(
        &self,
        exclude_current_row: Option<bool>,
    ) -> Vec<crate::runtime::operators::window::index::DataRequest> {
        match self {
            RetractableAggregation::Range(r) => r.get_data_requests(exclude_current_row),
            RetractableAggregation::Points(p) => p.get_data_requests(exclude_current_row),
        }
    }

    async fn produce_aggregates_from_ranges(
        &self,
        sorted_ranges: &[crate::runtime::operators::window::index::SortedRangeView],
        thread_pool: Option<&ThreadPool>,
        exclude_current_row: Option<bool>,
    ) -> (Vec<ScalarValue>, Option<AccumulatorState>) {
        match self {
            RetractableAggregation::Range(r) => {
                r.produce_aggregates_from_ranges(sorted_ranges, thread_pool, exclude_current_row)
                    .await
            }
            RetractableAggregation::Points(p) => {
                p.produce_aggregates_from_ranges(sorted_ranges, thread_pool, exclude_current_row)
                    .await
            }
        }
    }
}
