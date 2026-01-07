use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::WindowExpr;
use futures::stream::{FuturesUnordered, StreamExt};
use indexmap::IndexSet;
use tokio::sync::Semaphore;

use crate::common::Key;
use crate::runtime::operators::window::SEQ_NO_COLUMN_NAME;
use crate::storage::batch_pins::BatchPins;
use crate::storage::batch_store::{BatchId, BatchStore, Timestamp};
use crate::storage::index::{BucketIndex, Cursor, DataBounds, DataRequest, SortedRangeView, SortedSegment};
use crate::storage::index::bucket_index::BatchRef;
use crate::storage::index::InMemBatchId;
use crate::storage::InMemBatchCache;
use crate::storage::WorkBudget;
use crate::runtime::TaskId;

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
    batch: BatchRef,
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
    stored_bytes_estimate_total: usize,
    stored_missing_bytes_estimate_ids: Vec<BatchId>,
    inmem_batch_ids: Vec<InMemBatchId>,
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

pub async fn execute_planned_load(
    planned: PlannedRangesLoad,
    batch_store: Arc<dyn BatchStore>,
    pins: Arc<BatchPins>,
    work_budget: Arc<WorkBudget>,
    in_mem: &InMemBatchCache,
    task_id: TaskId,
    key: &Key,
    ts_column_index: usize,
    load_io_parallelism: usize,
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
    let _lease = pins.clone().pin(task_id.clone(), planned.stored_batch_ids.clone());

    // If metadata estimates are missing (e.g. older checkpoints), try to ask the store for
    // best-effort byte estimates before admitting work memory.
    let mut total_est = planned.stored_bytes_estimate_total;
    if !planned.stored_missing_bytes_estimate_ids.is_empty() {
        let sem = Arc::new(Semaphore::new(load_io_parallelism.max(1)));
        let mut futs: FuturesUnordered<_> = FuturesUnordered::new();
        for id in planned.stored_missing_bytes_estimate_ids.iter().copied() {
            let store = batch_store.clone();
            let key = key.clone();
            let sem = sem.clone();
            let task_id = task_id.clone();
            futs.push(async move {
                let _p = sem.acquire_owned().await.expect("semaphore");
                let est = store
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

    // Admit work memory for the read/hydration working set (stored batches loaded into memory).
    // This lease must stay alive as long as returned views may reference loaded batches.
    let work_lease = work_budget
        .acquire(total_est)
        .await;

    // Clone all planned in-mem runs now to keep them alive even if pruned concurrently.
    let mut in_mem_runs: HashMap<InMemBatchId, Arc<RecordBatch>> = HashMap::new();
    for id in planned.inmem_batch_ids.iter().copied() {
        if let Some(b) = in_mem.get(id) {
            in_mem_runs.insert(id, b);
        }
    }

    // Load stored runs with bounded fanout; store API is singular.
    let sem = Arc::new(Semaphore::new(load_io_parallelism.max(1)));
    let mut futs: FuturesUnordered<_> = FuturesUnordered::new();
    for id in planned.stored_batch_ids.iter().copied() {
        let store = batch_store.clone();
        let key = key.clone();
        let sem = sem.clone();
        let task_id = task_id.clone();
        futs.push(async move {
            let _p = sem.acquire_owned().await.expect("semaphore");
            let b = store.load_batch(task_id.clone(), id, &key).await;
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
            let segments: Vec<SortedSegment> = segments_with_sort.into_iter().map(|(_, _, s)| s).collect();

            views.push(SortedRangeView::new(
                r.view_req,
                planned.bucket_granularity,
                r.start,
                r.end,
                segments,
                Some(work_lease.clone()),
            ));
        }
        out.push(views);
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::fmt;

    use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;

    use crate::runtime::operators::window::TimeGranularity;
    use crate::runtime::operators::window::aggregates::{BucketRange, test_utils};
    use crate::storage::batch_store::{BatchStore, BatchId, InMemBatchStore};
    use crate::storage::stats::StorageStats;
    use crate::storage::WorkBudget;

    #[derive(Clone)]
    struct CountingStore {
        inner: Arc<InMemBatchStore>,
        estimate_calls: Arc<AtomicUsize>,
    }

    impl fmt::Debug for CountingStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("CountingStore")
                .field("estimate_calls", &self.estimate_calls.load(Ordering::Relaxed))
                .finish()
        }
    }

    impl CountingStore {
        fn new(inner: InMemBatchStore) -> Self {
            Self {
                inner: Arc::new(inner),
                estimate_calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl BatchStore for CountingStore {
        fn bucket_granularity(&self) -> TimeGranularity {
            self.inner.bucket_granularity()
        }

        fn max_batch_size(&self) -> usize {
            self.inner.max_batch_size()
        }

        fn partition_records(
            &self,
            batch: &RecordBatch,
            partition_key: &Key,
            ts_column_index: usize,
        ) -> Vec<(i64, RecordBatch)> {
            self.inner.partition_records(batch, partition_key, ts_column_index)
        }

        fn load_batch<'a>(
            &'a self,
            task_id: TaskId,
            batch_id: BatchId,
            partition_key: &'a Key,
        ) -> crate::storage::batch_store::BoxFut<'a, Option<RecordBatch>> {
            let inner = self.inner.clone();
            Box::pin(async move { inner.load_batch(task_id, batch_id, partition_key).await })
        }

        fn remove_batch<'a>(
            &'a self,
            task_id: TaskId,
            batch_id: BatchId,
            partition_key: &'a Key,
        ) -> crate::storage::batch_store::BoxFut<'a, ()> {
            let inner = self.inner.clone();
            Box::pin(async move { inner.remove_batch(task_id, batch_id, partition_key).await })
        }

        fn batch_bytes_estimate<'a>(
            &'a self,
            task_id: TaskId,
            batch_id: BatchId,
            partition_key: &'a Key,
        ) -> crate::storage::batch_store::BoxFut<'a, Option<usize>> {
            let calls = self.estimate_calls.clone();
            let fut = self.inner.batch_bytes_estimate(task_id, batch_id, partition_key);
            Box::pin(async move {
                calls.fetch_add(1, Ordering::Relaxed);
                fut.await
            })
        }

        fn put_batch_with_id<'a>(
            &'a self,
            task_id: TaskId,
            batch_id: BatchId,
            batch: RecordBatch,
            partition_key: &'a Key,
        ) -> crate::storage::batch_store::BoxFut<'a, ()> {
            let inner = self.inner.clone();
            Box::pin(async move { inner.put_batch_with_id(task_id, batch_id, batch, partition_key).await })
        }

        fn await_persisted<'a>(
            &'a self,
        ) -> crate::storage::batch_store::BoxFut<'a, anyhow::Result<()>> {
            let inner = self.inner.clone();
            Box::pin(async move { inner.await_persisted().await })
        }

        fn to_checkpoint(&self, task_id: TaskId) -> crate::storage::batch_store::BatchStoreCheckpoint {
            self.inner.to_checkpoint(task_id)
        }

        fn apply_checkpoint(&self, task_id: TaskId, cp: crate::storage::batch_store::BatchStoreCheckpoint) {
            self.inner.apply_checkpoint(task_id, cp)
        }
    }

    fn make_key(partition: &str) -> Key {
        let schema = Arc::new(Schema::new(vec![Field::new("partition", DataType::Utf8, false)]));
        let arr = StringArray::from(vec![partition]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("key batch");
        Key::new(batch).expect("key")
    }

    fn make_batch(ts: i64, seq: u64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("value", DataType::Float64, false),
            Field::new("partition_key", DataType::Utf8, false),
            Field::new("__seq_no", DataType::UInt64, false),
        ]));
        let ts_arr = TimestampMillisecondArray::from(vec![ts]);
        let val_arr = Float64Array::from(vec![1.0]);
        let pk_arr = StringArray::from(vec!["A"]);
        let seq_arr = UInt64Array::from(vec![seq]);
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(ts_arr),
                Arc::new(val_arr),
                Arc::new(pk_arr),
                Arc::new(seq_arr),
            ],
        )
        .expect("batch")
    }

    #[tokio::test]
    async fn work_budget_uses_store_estimate_when_meta_is_missing() {
        let sql = "SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
        FROM test_table
        WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW)";
        let window_expr = test_utils::window_expr_from_sql(sql).await;

        let key = make_key("A");
        let batch_id = BatchId::new(key.hash(), 0, 1);
        let stored_batch = make_batch(0, 0);

        let store = Arc::new(InMemBatchStore::new(8, TimeGranularity::Seconds(1), 16)) as Arc<dyn BatchStore>;
        store
            .put_batch_with_id(Arc::<str>::from("t"), batch_id, stored_batch.clone(), &key)
            .await;

        let mut idx = BucketIndex::new(TimeGranularity::Seconds(1));
        // Simulate an older checkpoint/layout with missing bytes_estimate (0).
        idx.insert_batch_ref(
            0,
            BatchRef::Stored(batch_id),
            Cursor::new(0, 0),
            Cursor::new(0, 0),
            stored_batch.num_rows(),
            0,
        );

        let plans = vec![RangesLoadPlan {
            requests: vec![DataRequest {
                bucket_range: BucketRange::new(0, 0),
                bounds: DataBounds::All,
            }],
            window_expr_for_args: window_expr.clone(),
        }];

        let planned = plan_load_from_index(&idx, &plans);
        assert_eq!(planned.stored_bytes_estimate_total, 0);

        let pins = Arc::new(BatchPins::new());
        let stats = StorageStats::global();
        let in_mem = InMemBatchCache::new_with_stats(stats);
        let work_budget = WorkBudget::new(1024 * 1024);

        assert_eq!(work_budget.used_bytes(), 0);
        let out = execute_planned_load(
            planned,
            store.clone(),
            pins,
            work_budget.clone(),
            &in_mem,
            Arc::<str>::from("t"),
            &key,
            0,
            2,
            &plans,
        )
        .await;
        assert!(!out.is_empty());
        assert!(work_budget.used_bytes() > 0, "lease should be held by views");

        drop(out);
        // After views are dropped, lease should be released.
        // (Give the runtime a tick in case drops cascade.)
        tokio::task::yield_now().await;
        assert_eq!(work_budget.used_bytes(), 0);
    }

    #[tokio::test]
    async fn loader_queries_store_only_for_missing_estimates() {
        let sql = "SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
        FROM test_table
        WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW)";
        let window_expr = test_utils::window_expr_from_sql(sql).await;

        let key = make_key("A");
        let id_known = BatchId::new(key.hash(), 0, 10);
        let id_missing = BatchId::new(key.hash(), 0, 11);

        let store = Arc::new(CountingStore::new(InMemBatchStore::new(
            8,
            TimeGranularity::Seconds(1),
            16,
        )));
        let calls = store.estimate_calls.clone();
        let store_dyn: Arc<dyn BatchStore> = store.clone();

        store_dyn
            .put_batch_with_id(Arc::<str>::from("t"), id_known, make_batch(0, 0), &key)
            .await;
        store_dyn
            .put_batch_with_id(Arc::<str>::from("t"), id_missing, make_batch(1, 1), &key)
            .await;

        let mut idx = BucketIndex::new(TimeGranularity::Seconds(1));
        idx.insert_batch_ref(
            0,
            BatchRef::Stored(id_known),
            Cursor::new(0, 0),
            Cursor::new(0, 0),
            1,
            123, // known estimate
        );
        idx.insert_batch_ref(
            0,
            BatchRef::Stored(id_missing),
            Cursor::new(1, 1),
            Cursor::new(1, 1),
            1,
            0, // missing estimate
        );

        let plans = vec![RangesLoadPlan {
            requests: vec![DataRequest {
                bucket_range: BucketRange::new(0, 0),
                bounds: DataBounds::All,
            }],
            window_expr_for_args: window_expr.clone(),
        }];

        let planned = plan_load_from_index(&idx, &plans);
        assert_eq!(planned.stored_batch_ids.len(), 2);
        assert_eq!(planned.stored_missing_bytes_estimate_ids.len(), 1);

        // Before we run: should not have called estimates.
        let calls_before = calls.load(Ordering::Relaxed);
        assert_eq!(calls_before, 0);

        let pins = Arc::new(BatchPins::new());
        let stats = StorageStats::global();
        let in_mem = InMemBatchCache::new_with_stats(stats);
        let work_budget = WorkBudget::new(1024 * 1024);

        let out = execute_planned_load(
            planned,
            store_dyn.clone(),
            pins,
            work_budget.clone(),
            &in_mem,
            Arc::<str>::from("t"),
            &key,
            0,
            2,
            &plans,
        )
        .await;
        drop(out);

        let calls_after = calls.load(Ordering::Relaxed);
        // Exactly one batch had missing estimate.
        assert_eq!(calls_after, 1);
    }
}





