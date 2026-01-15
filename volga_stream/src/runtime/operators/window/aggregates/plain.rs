use std::sync::Arc;

use async_trait::async_trait;
use datafusion::physical_plan::WindowExpr;
use tokio_rayon::rayon::ThreadPool;

use crate::storage::index::{BucketIndex, DataRequest, SortedRangeView};
use crate::runtime::operators::window::{Cursor, Tiles};

use super::{Aggregation, AggregationExecResult, AggregatorType};

#[path = "plain_range.rs"]
mod plain_range;
#[path = "plain_points.rs"]
mod plain_points;
#[path = "plain_range_cache.rs"]
mod plain_range_cache;

pub use super::VirtualPoint;

#[derive(Debug, Clone, Copy)]
pub struct CursorBounds {
    pub prev: Option<Cursor>,
    pub new: Cursor,
}

#[derive(Debug)]
pub enum PlainAggregation {
    Range(plain_range::PlainRangeAggregation),
    Points(plain_points::PlainPointsAggregation),
}

impl PlainAggregation {
    pub fn for_range(
        batch_index: &BucketIndex,
        window_expr: Arc<dyn WindowExpr>,
        tiles: Option<Tiles>,
        bounds: CursorBounds,
    ) -> Self {
        Self::Range(plain_range::PlainRangeAggregation::new(
            batch_index,
            window_expr,
            tiles,
            bounds,
        ))
    }

    pub fn for_points(
        points: Vec<VirtualPoint>,
        batch_index: &BucketIndex,
        window_expr: Arc<dyn WindowExpr>,
        tiles: Option<Tiles>,
        exclude_current_row: bool,
    ) -> Self {
        Self::Points(plain_points::PlainPointsAggregation::new(
            points,
            batch_index,
            window_expr,
            tiles,
            exclude_current_row,
        ))
    }

}

#[async_trait]
impl Aggregation for PlainAggregation {
    fn window_expr(&self) -> &Arc<dyn WindowExpr> {
        match self {
            PlainAggregation::Range(r) => r.window_expr(),
            PlainAggregation::Points(p) => p.window_expr(),
        }
    }

    fn aggregator_type(&self) -> AggregatorType {
        AggregatorType::PlainAccumulator
    }

    fn get_data_requests(&self) -> Vec<DataRequest> {
        match self {
            PlainAggregation::Range(r) => r.get_data_requests(),
            PlainAggregation::Points(p) => p.get_data_requests(),
        }
    }

    async fn produce_aggregates_from_ranges(
        &self,
        sorted_ranges: &[SortedRangeView],
        thread_pool: Option<&ThreadPool>,
    ) -> AggregationExecResult {
        match self {
            PlainAggregation::Range(r) => r.produce_aggregates_from_ranges(sorted_ranges, thread_pool).await,
            PlainAggregation::Points(p) => p.produce_aggregates_from_ranges(sorted_ranges, thread_pool).await,
        }
    }
}

