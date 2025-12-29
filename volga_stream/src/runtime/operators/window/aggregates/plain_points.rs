use std::sync::Arc;

use async_trait::async_trait;
use datafusion::logical_expr::WindowFrameUnits;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use tokio_rayon::rayon::ThreadPool;
use crate::runtime::operators::window::aggregates::{Aggregation, BucketRange};
use crate::runtime::operators::window::index::{BucketIndex, get_window_length_ms, get_window_size_rows};
use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::runtime::operators::window::{RowPtr, Tiles, TimeGranularity, WindowAggregator, create_window_aggregator};
use crate::storage::batch_store::Timestamp;

use crate::runtime::operators::window::aggregates::VirtualPoint;
use crate::runtime::operators::window::index::{DataBounds, DataRequest, SortedRangeIndex, SortedRangeView};
use crate::runtime::operators::window::Cursor;

#[derive(Debug, Clone)]
struct PointPlan {
    point: VirtualPoint,
    req_idx: usize,
}

#[derive(Debug)]
pub struct PlainPointsAggregation {
    plans: Vec<PointPlan>,
    data_requests: Vec<DataRequest>,
    window_expr: Arc<dyn WindowExpr>,
    tiles: Option<Tiles>,
    window_id: usize,
    bucket_granularity: TimeGranularity,
}

impl PlainPointsAggregation {
    pub fn new(
        points: Vec<VirtualPoint>,
        bucket_index: &BucketIndex,
        window_expr: Arc<dyn WindowExpr>,
        tiles: Option<Tiles>,
        _ts_column_index: usize,
        window_id: usize,
    ) -> Self {
        let window_frame = window_expr.get_window_frame();
        let bucket_granularity = bucket_index.bucket_granularity();
        let wl = get_window_length_ms(window_frame);
        let ws = get_window_size_rows(window_frame);

        #[derive(Clone)]
        struct Tmp {
            orig_idx: usize,
            window_range: BucketRange,
            bounds: DataBounds,
            point: VirtualPoint,
        }
        let mut tmp: Vec<Tmp> = Vec::with_capacity(points.len());
        for (orig_idx, p) in points.into_iter().enumerate() {
            let bucket_ts = bucket_granularity.start(p.ts);
            let update = BucketRange::new(bucket_ts, bucket_ts);
            let (window_range, bounds) = match window_frame.units {
                WindowFrameUnits::Range => (
                    bucket_index.get_relevant_range_for_range_windows(update, wl),
                    DataBounds::Time {
                        start_ts: p.ts.saturating_sub(wl),
                        end_ts: p.ts,
                    },
                ),
                WindowFrameUnits::Rows => (
                    bucket_index.get_relevant_range_for_rows_windows(update, ws),
                    DataBounds::All,
                ),
                _ => (update, DataBounds::All),
            };

            tmp.push(Tmp {
                orig_idx,
                window_range,
                bounds,
                point: p,
            });
        }

        // Sort by bucket_range, then by bounds.
        tmp.sort_by_key(|t| (t.window_range.start, t.window_range.end, t.point.ts));

        let mut data_requests: Vec<DataRequest> = Vec::new();
        let mut plans_by_orig: Vec<Option<PointPlan>> = vec![None; tmp.len()];

        let mut cur_req: Option<DataRequest> = None;
        let mut cur_idx: usize = 0;

        for t in tmp {
            match cur_req {
                None => {
                    let req = DataRequest {
                        bucket_range: t.window_range,
                        bounds: t.bounds,
                    };
                    data_requests.push(req);
                    cur_idx = data_requests.len() - 1;
                    cur_req = Some(req);
                    plans_by_orig[t.orig_idx] = Some(PointPlan {
                        point: t.point,
                        req_idx: cur_idx,
                    });
                }
                Some(mut req) => {
                    let bucket_adjacent_or_overlap =
                        t.window_range.start <= bucket_granularity.next_start(req.bucket_range.end);
                    let can_merge = match (req.bounds, t.bounds) {
                        (DataBounds::All, DataBounds::All) => bucket_adjacent_or_overlap,
                        (
                            DataBounds::Time {
                                start_ts: a_s,
                                end_ts: a_e,
                            },
                            DataBounds::Time {
                                start_ts: b_s,
                                end_ts: b_e,
                            },
                        ) => bucket_adjacent_or_overlap && b_s <= a_e,
                        _ => false,
                    };

                    if can_merge {
                        req.bucket_range.end = req.bucket_range.end.max(t.window_range.end);
                        req.bounds = match (req.bounds, t.bounds) {
                            (
                                DataBounds::Time {
                                    start_ts: a_s,
                                    end_ts: a_e,
                                },
                                DataBounds::Time {
                                    start_ts: b_s,
                                    end_ts: b_e,
                                },
                            ) => DataBounds::Time {
                                start_ts: a_s.min(b_s),
                                end_ts: a_e.max(b_e),
                            },
                            (b, _) => b,
                        };
                        cur_req = Some(req);
                        *data_requests.last_mut().expect("must exist") = req;
                        plans_by_orig[t.orig_idx] = Some(PointPlan {
                            point: t.point,
                            req_idx: cur_idx,
                        });
                    } else {
                        let req2 = DataRequest {
                            bucket_range: t.window_range,
                            bounds: t.bounds,
                        };
                        data_requests.push(req2);
                        cur_idx = data_requests.len() - 1;
                        cur_req = Some(req2);
                        plans_by_orig[t.orig_idx] = Some(PointPlan {
                            point: t.point,
                            req_idx: cur_idx,
                        });
                    }
                }
            }
        }

        let plans: Vec<PointPlan> = plans_by_orig
            .into_iter()
            .map(|p| p.expect("plan must be set"))
            .collect();

        Self {
            plans,
            data_requests,
            window_expr,
            tiles,
            window_id,
            bucket_granularity,
        }
    }
}

#[async_trait]
impl Aggregation for PlainPointsAggregation {
    fn window_expr(&self) -> &Arc<dyn WindowExpr> {
        &self.window_expr
    }

    fn aggregator_type(&self) -> crate::runtime::operators::window::AggregatorType {
        crate::runtime::operators::window::AggregatorType::PlainAccumulator
    }

    fn get_data_requests(&self, _exclude_current_row: Option<bool>) -> Vec<DataRequest> {
        self.data_requests.clone()
    }

    async fn produce_aggregates_from_ranges(
        &self,
        sorted_ranges: &[SortedRangeView],
        _thread_pool: Option<&ThreadPool>,
        exclude_current_row: Option<bool>,
    ) -> (Vec<ScalarValue>, Option<AccumulatorState>) {
        let exclude_current_row = exclude_current_row.unwrap_or(false);
        let include_virtual = !exclude_current_row;

        let window_frame = self.window_expr.get_window_frame();
        let window_length = get_window_length_ms(window_frame);
        let window_size = get_window_size_rows(window_frame);

        let indices: Vec<SortedRangeIndex<'_>> = sorted_ranges
            .iter()
            .map(SortedRangeIndex::new)
            .collect();

        let mut out: Vec<ScalarValue> = Vec::with_capacity(self.plans.len());
        for plan in &self.plans {
            let idx = &indices[plan.req_idx];
            let mut accumulator = match create_window_aggregator(&self.window_expr) {
                WindowAggregator::Accumulator(accumulator) => accumulator,
                WindowAggregator::Evaluator(_) => panic!("PlainAggregation should not use evaluator"),
            };

            match window_frame.units {
                WindowFrameUnits::Range => {
                    if !idx.is_empty() {
                        let start_ts = plan.point.ts.saturating_sub(window_length);
                        let start = idx.seek_ts_ge(start_ts);
                        let end = last_row_le_ts_in_range(idx, plan.point.ts);
                        if let (Some(start), Some(end)) = (start, end) {
                            if start <= end {
                                let args = idx.get_args_in_range(&start, &end);
                                if !args.is_empty() {
                                    accumulator.update_batch(&args).expect("update_batch failed");
                                }
                            }
                        }
                    }
                }
                WindowFrameUnits::Rows => {
                    if !idx.is_empty() {
                        let Some(end) = last_row_le_ts_in_range(idx, plan.point.ts) else {
                            // no stored rows
                            goto_virtual(&mut *accumulator, include_virtual, &plan.point);
                            out.push(accumulator.evaluate().expect("evaluate failed"));
                            continue;
                        };
                        let stored_rows = if include_virtual {
                            window_size.saturating_sub(1)
                        } else {
                            window_size
                        };
                        if stored_rows > 0 {
                            let start = idx.pos_n_rows(&end, stored_rows.saturating_sub(1), true);
                            if start <= end {
                                let args = idx.get_args_in_range(&start, &end);
                                if !args.is_empty() {
                                    accumulator.update_batch(&args).expect("update_batch failed");
                                }
                            }
                        }
                    }
                }
                _ => {}
            }

            goto_virtual(&mut *accumulator, include_virtual, &plan.point);
            out.push(accumulator.evaluate().expect("evaluate failed"));
        }

        (out, None)
    }
}

fn goto_virtual(acc: &mut dyn datafusion::logical_expr::Accumulator, include_virtual: bool, p: &VirtualPoint) {
    if include_virtual {
        if let Some(args) = p.args.as_ref() {
            acc.update_batch(args.as_ref()).expect("update_batch failed");
        }
    }
}

fn last_row_le_ts_in_range(idx: &SortedRangeIndex<'_>, ts: Timestamp) -> Option<RowPtr> {
    if idx.is_empty() {
        return None;
    }
    let first_ts = idx.get_timestamp(&idx.first_pos());
    if first_ts > ts {
        return None;
    }
    match idx.seek_rowpos_gt(Cursor::new(ts, u64::MAX)) {
        Some(after) => idx.prev_pos(after),
        None => Some(idx.last_pos()),
    }
}