use std::collections::HashMap;
use std::sync::Arc;

use datafusion::physical_plan::WindowExpr;
use indexmap::IndexSet;

use crate::storage::index::TimeGranularity;
use crate::storage::batch::{BatchId, Timestamp};
use crate::storage::index::bucket_index::BatchRef;
use crate::storage::index::{BucketIndex, Cursor, DataBounds, DataRequest, InMemBatchId};

#[derive(Debug, Clone)]
pub struct RangesLoadPlan {
    pub requests: Vec<DataRequest>,
    pub window_expr_for_args: Arc<dyn WindowExpr>,
}

#[derive(Clone, Debug)]
pub(crate) struct MetaSnap {
    pub(crate) bucket_ts: Timestamp,
    pub(crate) min_pos: Cursor,
    pub(crate) max_pos: Cursor,
    pub(crate) batch: BatchRef,
}

#[derive(Clone, Debug)]
pub(crate) struct ReqSnap {
    pub(crate) view_req: DataRequest,
    pub(crate) start: Cursor,
    pub(crate) end: Cursor,
    pub(crate) metas: Vec<MetaSnap>,
}

#[derive(Clone, Debug)]
pub struct PlannedRangesLoad {
    pub(crate) bucket_granularity: TimeGranularity,
    pub(crate) plan_snaps: Vec<Vec<ReqSnap>>,
    pub(crate) stored_batch_ids: Vec<BatchId>,
    pub(crate) stored_bytes_estimate_total: usize,
    pub(crate) stored_missing_bytes_estimate_ids: Vec<BatchId>,
    pub(crate) inmem_batch_ids: Vec<InMemBatchId>,
}

pub fn plan_load_from_index(bucket_index: &BucketIndex, plans: &[RangesLoadPlan]) -> PlannedRangesLoad {
    let bucket_granularity = bucket_index.bucket_granularity();

    let mut plan_snaps: Vec<Vec<ReqSnap>> = Vec::with_capacity(plans.len());
    let mut batch_ids_to_load: IndexSet<BatchId> = IndexSet::new();
    let mut batch_bytes_estimate: HashMap<BatchId, usize> = HashMap::new();
    let mut missing_bytes_estimate: IndexSet<BatchId> = IndexSet::new();
    let mut inmem_to_load: IndexSet<InMemBatchId> = IndexSet::new();

    for p in plans {
        let mut reqs: Vec<ReqSnap> = Vec::with_capacity(p.requests.len());
        for req in &p.requests {
            let (start, end) = match req.bounds {
                DataBounds::All => (Cursor::new(i64::MIN, 0), Cursor::new(i64::MAX, u64::MAX)),
                DataBounds::Time { start_ts, end_ts } => {
                    (Cursor::new(start_ts, 0), Cursor::new(end_ts, u64::MAX))
                }
                DataBounds::RowsTail { end_ts, .. } => {
                    (Cursor::new(i64::MIN, 0), Cursor::new(end_ts, u64::MAX))
                }
            };

            let effective_bucket_range = match req.bounds {
                DataBounds::RowsTail { end_ts, rows } => bucket_index
                    .plan_rows_tail(end_ts, rows, req.bucket_range)
                    .unwrap_or(req.bucket_range),
                _ => req.bucket_range,
            };
            let view_req = DataRequest {
                bucket_range: effective_bucket_range,
                bounds: req.bounds,
            };

            let mut metas: Vec<MetaSnap> = Vec::new();
            for bucket in bucket_index
                .query_buckets_in_range(effective_bucket_range.start, effective_bucket_range.end)
            {
                for meta in bucket
                    .hot_base_segments
                    .iter()
                    .chain(bucket.hot_deltas.iter())
                    .chain(bucket.persisted_segments.iter())
                {
                    match req.bounds {
                        DataBounds::Time { start_ts, end_ts } => {
                            if meta.max_pos.ts < start_ts || meta.min_pos.ts > end_ts {
                                continue;
                            }
                        }
                        DataBounds::RowsTail { end_ts, .. } => {
                            if meta.min_pos.ts > end_ts {
                                continue;
                            }
                        }
                        DataBounds::All => {}
                    }

                    match meta.run {
                        BatchRef::Stored(id) => {
                            batch_ids_to_load.insert(id);
                            // Keep the max estimate we have seen for this stored batch id.
                            batch_bytes_estimate
                                .entry(id)
                                .and_modify(|v| *v = (*v).max(meta.bytes_estimate))
                                .or_insert(meta.bytes_estimate);
                            if meta.bytes_estimate == 0 {
                                missing_bytes_estimate.insert(id);
                            }
                        }
                        BatchRef::InMem(id) => {
                            inmem_to_load.insert(id);
                        }
                    }

                    metas.push(MetaSnap {
                        bucket_ts: bucket.timestamp,
                        min_pos: meta.min_pos,
                        max_pos: meta.max_pos,
                        batch: meta.run,
                    });
                }
            }

            reqs.push(ReqSnap {
                view_req,
                start,
                end,
                metas,
            });
        }
        plan_snaps.push(reqs);
    }

    PlannedRangesLoad {
        bucket_granularity,
        plan_snaps,
        stored_batch_ids: batch_ids_to_load.iter().copied().collect(),
        stored_bytes_estimate_total: batch_bytes_estimate.values().copied().sum(),
        stored_missing_bytes_estimate_ids: missing_bytes_estimate.iter().copied().collect(),
        inmem_batch_ids: inmem_to_load.iter().copied().collect(),
    }
}
