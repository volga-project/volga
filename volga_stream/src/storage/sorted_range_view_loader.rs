use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::WindowExpr;
use indexmap::IndexSet;

use crate::common::Key;
use crate::runtime::operators::window::SEQ_NO_COLUMN_NAME;
use crate::storage::batch_pins::BatchPins;
use crate::storage::batch_store::{BatchId, BatchStore, Timestamp};
use crate::storage::index::{BucketIndex, Cursor, DataBounds, DataRequest, SortedRangeView, SortedSegment};
use crate::storage::index::bucket_index::BatchRef;
use crate::storage::index::InMemBatchId;
use crate::storage::InMemBatchCache;

#[derive(Debug, Clone)]
pub struct RangesLoadPlan {
    pub requests: Vec<DataRequest>,
    pub window_expr_for_args: Arc<dyn WindowExpr>,
}

#[derive(Clone, Debug)]
struct MetaSnap {
    bucket_ts: Timestamp,
    min_pos: Cursor,
    max_pos: Cursor,
    run: BatchRef,
}

#[derive(Clone, Debug)]
struct ReqSnap {
    view_req: DataRequest,
    start: Cursor,
    end: Cursor,
    metas: Vec<MetaSnap>,
}

#[derive(Clone, Debug)]
pub struct PlannedRangesLoad {
    bucket_granularity: crate::runtime::operators::window::TimeGranularity,
    plan_snaps: Vec<Vec<ReqSnap>>,
    stored_batch_ids: Vec<BatchId>,
    inmem_batch_ids: Vec<InMemBatchId>,
}

pub fn plan_load_from_index(bucket_index: &BucketIndex, plans: &[RangesLoadPlan]) -> PlannedRangesLoad {
    let bucket_granularity = bucket_index.bucket_granularity();

    let mut plan_snaps: Vec<Vec<ReqSnap>> = Vec::with_capacity(plans.len());
    let mut batch_ids_to_load: IndexSet<BatchId> = IndexSet::new();
    let mut inmem_to_load: IndexSet<InMemBatchId> = IndexSet::new();

    for p in plans {
        let mut reqs: Vec<ReqSnap> = Vec::with_capacity(p.requests.len());
        for req in &p.requests {
            let (start, end) = match req.bounds {
                DataBounds::All => (Cursor::new(i64::MIN, 0), Cursor::new(i64::MAX, u64::MAX)),
                DataBounds::Time { start_ts, end_ts } => (Cursor::new(start_ts, 0), Cursor::new(end_ts, u64::MAX)),
                DataBounds::RowsTail { end_ts, .. } => (Cursor::new(i64::MIN, 0), Cursor::new(end_ts, u64::MAX)),
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
            for bucket in bucket_index.query_buckets_in_range(effective_bucket_range.start, effective_bucket_range.end) {
                for meta in bucket.hot_base_segments.iter().chain(bucket.hot_deltas.iter()) {
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
                        }
                        BatchRef::InMem(id) => {
                            inmem_to_load.insert(id);
                        }
                    }

                    metas.push(MetaSnap {
                        bucket_ts: bucket.timestamp,
                        min_pos: meta.min_pos,
                        max_pos: meta.max_pos,
                        run: meta.run,
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
        inmem_batch_ids: inmem_to_load.iter().copied().collect(),
    }
}

pub async fn execute_planned_load(
    planned: PlannedRangesLoad,
    batch_store: Arc<dyn BatchStore>,
    pins: Arc<BatchPins>,
    in_mem: &InMemBatchCache,
    key: &Key,
    ts_column_index: usize,
    plans: &[RangesLoadPlan],
) -> Vec<Vec<SortedRangeView>> {
    if plans.is_empty() {
        return Vec::new();
    }
    if plans.iter().all(|p| p.requests.is_empty()) {
        return vec![Vec::new(); plans.len()];
    }

    // Pin all planned stored runs before loading. This prevents a race where pruning deletes batches
    // that are still needed for this read.
    let _lease = pins.clone().pin(planned.stored_batch_ids.clone());

    // Clone all planned in-mem runs now to keep them alive even if pruned concurrently.
    let mut in_mem_runs: HashMap<InMemBatchId, Arc<RecordBatch>> = HashMap::new();
    for id in planned.inmem_batch_ids.iter().copied() {
        if let Some(b) = in_mem.get(id) {
            in_mem_runs.insert(id, b);
        }
    }

    let loaded = batch_store
        .load_batches(planned.stored_batch_ids.clone(), key)
        .await;

    let mut out: Vec<Vec<SortedRangeView>> = Vec::with_capacity(plans.len());
    for (p, reqs) in plans.iter().zip(planned.plan_snaps.iter()) {
        let mut views: Vec<SortedRangeView> = Vec::with_capacity(reqs.len());
        for r in reqs {
            let mut segments_with_sort: Vec<(Timestamp, Cursor, SortedSegment)> = Vec::new();
            for meta in &r.metas {
                match r.view_req.bounds {
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

                let batch = match meta.run {
                    BatchRef::Stored(id) => loaded.get(&id).cloned(),
                    BatchRef::InMem(id) => in_mem_runs.get(&id).map(|b| (**b).clone()),
                };
                let Some(batch) = batch else {
                    continue;
                };
                if batch.num_rows() == 0 {
                    continue;
                }

                let seq_column_index = batch
                    .schema()
                    .fields()
                    .iter()
                    .position(|f| f.name() == SEQ_NO_COLUMN_NAME)
                    .expect("Expected __seq_no column to exist");

                let args = p
                    .window_expr_for_args
                    .evaluate_args(&batch)
                    .expect("Should be able to evaluate window args");

                let seg = SortedSegment::new(
                    meta.bucket_ts,
                    batch,
                    ts_column_index,
                    seq_column_index,
                    Arc::new(args),
                );
                segments_with_sort.push((meta.bucket_ts, meta.min_pos, seg));
            }

            segments_with_sort.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
            let segments: Vec<SortedSegment> = segments_with_sort.into_iter().map(|(_, _, s)| s).collect();

            views.push(SortedRangeView::new(
                r.view_req,
                planned.bucket_granularity,
                r.start,
                r.end,
                segments,
            ));
        }
        out.push(views);
    }

    out
}

