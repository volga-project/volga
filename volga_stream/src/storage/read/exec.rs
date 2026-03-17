use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Semaphore;

use crate::common::Key;
use crate::runtime::TaskId;
use crate::storage::constants::SEQ_NO_COLUMN_NAME;
use crate::storage::BatchPins;
use crate::storage::backend::DynStorageBackend;
use crate::storage::batch::{BatchId, Timestamp};
use crate::storage::index::bucket_index::BatchRef;
use crate::storage::index::{
    Cursor, DataBounds, SortedRangeView, SortedSegment,
};
use crate::storage::index::InMemBatchId;
use crate::storage::memory_pool::ScratchLimiter;
use crate::storage::read::plan::{PlannedRangesLoad, RangesLoadPlan};
use crate::storage::WriteBuffer;

#[derive(Clone)]
pub struct LoaderContext {
    pub backend: DynStorageBackend,
    pub pins: Arc<BatchPins>,
    pub scratch: ScratchLimiter,
    pub write_buffer: Arc<WriteBuffer>,
    pub load_io_parallelism: usize,
}

pub async fn execute_planned_load(
    planned: PlannedRangesLoad,
    ctx: &LoaderContext,
    task_id: TaskId,
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
    let pin_lease = if planned.stored_batch_ids.is_empty() {
        None
    } else {
        Some(
            ctx.pins
                .clone()
                .pin(task_id.clone(), planned.stored_batch_ids.clone()),
        )
    };

    // If metadata estimates are missing (e.g. older checkpoints), try to ask the store for
    // best-effort byte estimates before admitting work memory.
    let mut total_est = planned.stored_bytes_estimate_total;
    if !planned.stored_missing_bytes_estimate_ids.is_empty() {
        let sem = Arc::new(Semaphore::new(ctx.load_io_parallelism.max(1)));
        let mut futs: FuturesUnordered<_> = FuturesUnordered::new();
        for id in planned.stored_missing_bytes_estimate_ids.iter().copied() {
            let key = key.clone();
            let sem = sem.clone();
            let task_id = task_id.clone();
            let backend = ctx.backend.clone();
            futs.push(async move {
                let _p = sem.acquire_owned().await.expect("semaphore");
                let est = backend
                    .batch_bytes_estimate(task_id.clone(), id, &key)
                    .await
                    .unwrap_or(0);
                est
            });
        }
        while let Some(est) = futs.next().await {
            total_est = total_est.saturating_add(est);
        }
    }
    // Ensure the work lease is never "empty" when we will hydrate stored batches.
    // Some stores/checkpoints may not provide a good estimate (0); we still want admission control
    // to observe a non-zero working set while views are alive.
    if total_est == 0 && !planned.stored_batch_ids.is_empty() {
        total_est = 1;
    }

    // Admit scratch concurrency for the read/hydration working set (stored batches loaded into memory).
    // This permit must stay alive as long as returned views may reference loaded batches.
    let _ = total_est;
    let scratch_permit = ctx.scratch.acquire().await;

    // Clone all planned in-mem runs now to keep them alive even if pruned concurrently.
    let mut in_mem_runs: HashMap<InMemBatchId, Arc<RecordBatch>> = HashMap::new();
    for id in planned.inmem_batch_ids.iter().copied() {
        if let Some(b) = ctx.write_buffer.get(id) {
            in_mem_runs.insert(id, b);
        }
    }

    // Load stored runs with bounded fanout; store API is singular.
    let sem = Arc::new(Semaphore::new(ctx.load_io_parallelism.max(1)));
    let mut futs: FuturesUnordered<_> = FuturesUnordered::new();
    for id in planned.stored_batch_ids.iter().copied() {
        let key = key.clone();
        let sem = sem.clone();
        let task_id = task_id.clone();
        let backend = ctx.backend.clone();
        futs.push(async move {
            let _p = sem.acquire_owned().await.expect("semaphore");
            let b = backend.batch_get(task_id.clone(), id, &key).await;
            (id, b)
        });
    }
    let mut loaded: HashMap<BatchId, RecordBatch> = HashMap::new();
    while let Some((id, b)) = futs.next().await {
        if let Some(b) = b {
            loaded.insert(id, b);
        }
    }

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

                let batch = match meta.batch {
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
            let segments: Vec<SortedSegment> =
                segments_with_sort.into_iter().map(|(_, _, s)| s).collect();

            views.push(SortedRangeView::new(
                r.view_req,
                planned.bucket_granularity,
                r.start,
                r.end,
                segments,
                Some(scratch_permit.clone()),
                pin_lease.clone(),
            ));
        }
        out.push(views);
    }

    out
}

#[cfg(test)]
mod exec_tests;
