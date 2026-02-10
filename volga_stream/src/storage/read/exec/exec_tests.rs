use super::*;


use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use crate::storage::index::TimeGranularity;
use crate::runtime::operators::window::aggregates::{BucketRange, test_utils};
use crate::storage::batch::BatchId;
use crate::storage::backend::{DynStorageBackend, StorageBackend};
use crate::storage::backend::inmem::InMemBackend;
use crate::storage::index::{BucketIndex, DataRequest};
use crate::storage::read::plan::plan_load_from_index;
use crate::storage::write::write_buffer::WriteBuffer;
use crate::storage::delete::batch_pins::BatchPins;

#[derive(Clone)]
struct CountingBackend {
    inner: DynStorageBackend,
    estimate_calls: Arc<AtomicUsize>,
}

impl fmt::Debug for CountingBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CountingBackend")
            .field("estimate_calls", &self.estimate_calls.load(Ordering::Relaxed))
            .finish()
    }
}

impl CountingBackend {
    fn new(inner: DynStorageBackend) -> Self {
        Self {
            inner,
            estimate_calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl StorageBackend for CountingBackend {
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
    ) -> Vec<(Timestamp, RecordBatch)> {
        self.inner.partition_records(batch, partition_key, ts_column_index)
    }

    fn batch_get<'a>(
        &'a self,
        task_id: TaskId,
        batch_id: BatchId,
        partition_key: &'a Key,
    ) -> crate::storage::batch::BoxFut<'a, Option<RecordBatch>> {
        self.inner.batch_get(task_id, batch_id, partition_key)
    }

    fn batch_bytes_estimate<'a>(
        &'a self,
        task_id: TaskId,
        batch_id: BatchId,
        partition_key: &'a Key,
    ) -> crate::storage::batch::BoxFut<'a, Option<usize>> {
        let calls = self.estimate_calls.clone();
        let inner = self.inner.clone();
        Box::pin(async move {
            calls.fetch_add(1, Ordering::Relaxed);
            inner
                .batch_get(task_id, batch_id, partition_key)
                .await
                .map(|b| b.get_array_memory_size())
        })
    }

    fn batch_put<'a>(
        &'a self,
        task_id: TaskId,
        batch_id: BatchId,
        batch: RecordBatch,
        partition_key: &'a Key,
    ) -> crate::storage::batch::BoxFut<'a, anyhow::Result<()>> {
        self.inner.batch_put(task_id, batch_id, batch, partition_key)
    }

    fn batch_delete<'a>(
        &'a self,
        task_id: TaskId,
        batch_id: BatchId,
        partition_key: &'a Key,
    ) -> crate::storage::batch::BoxFut<'a, anyhow::Result<()>> {
        self.inner.batch_delete(task_id, batch_id, partition_key)
    }

    fn state_get<'a>(
        &'a self,
        task_id: TaskId,
        key: &'a [u8],
    ) -> crate::storage::batch::BoxFut<'a, anyhow::Result<Option<Vec<u8>>>> {
        self.inner.state_get(task_id, key)
    }

    fn state_put<'a>(
        &'a self,
        task_id: TaskId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> crate::storage::batch::BoxFut<'a, anyhow::Result<()>> {
        self.inner.state_put(task_id, key, value)
    }

    fn state_delete<'a>(
        &'a self,
        task_id: TaskId,
        key: &'a [u8],
    ) -> crate::storage::batch::BoxFut<'a, anyhow::Result<()>> {
        self.inner.state_delete(task_id, key)
    }

    fn await_persisted<'a>(&'a self) -> crate::storage::batch::BoxFut<'a, anyhow::Result<()>> {
        self.inner.await_persisted()
    }

    fn checkpoint<'a>(
        &'a self,
        task_id: TaskId,
    ) -> crate::storage::batch::BoxFut<'a, anyhow::Result<crate::storage::StorageCheckpointToken>> {
        self.inner.checkpoint(task_id)
    }

    fn apply_checkpoint<'a>(
        &'a self,
        task_id: TaskId,
        token: crate::storage::StorageCheckpointToken,
    ) -> crate::storage::batch::BoxFut<'a, anyhow::Result<()>> {
        self.inner.apply_checkpoint(task_id, token)
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
async fn scratch_permit_uses_store_estimate_when_meta_is_missing() {
    let sql = "SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
    FROM test_table
    WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW)";
    let window_expr = test_utils::window_expr_from_sql(sql).await;

    let key = make_key("A");
    let batch_id = BatchId::new(key.hash(), 0, 1);
    let stored_batch = make_batch(0, 0);

    let backend: DynStorageBackend = Arc::new(InMemBackend::new(TimeGranularity::Seconds(1), 16));
    backend
        .batch_put(Arc::<str>::from("t"), batch_id, stored_batch.clone(), &key)
        .await
        .unwrap();

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
    let stats = crate::storage::StorageStats::new();
    let cfg = crate::storage::budget::StorageBackendConfig::default();
    let mem_pool = crate::storage::WorkerMemoryPool::new(cfg.memory);
    let in_mem = WriteBuffer::new_with_stats(stats.clone(), mem_pool.clone());
    let scratch = crate::storage::ScratchLimiter::new(cfg.concurrency.max_inflight_keys);
    let ctx = LoaderContext {
        backend: backend.clone(),
        pins,
        scratch,
        write_buffer: Arc::new(in_mem),
        load_io_parallelism: 2,
    };
    let out = execute_planned_load(planned, &ctx, Arc::<str>::from("t"), &key, 0, &plans).await;
    assert!(!out.is_empty());
    drop(out);
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

    let base_backend: DynStorageBackend = Arc::new(InMemBackend::new(TimeGranularity::Seconds(1), 16));
    let counting = Arc::new(CountingBackend::new(base_backend.clone()));
    let calls = counting.estimate_calls.clone();
    let backend: DynStorageBackend = counting;

    backend
        .batch_put(Arc::<str>::from("t"), id_known, make_batch(0, 0), &key)
        .await
        .unwrap();
    backend
        .batch_put(Arc::<str>::from("t"), id_missing, make_batch(1, 1), &key)
        .await
        .unwrap();

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
    let stats = crate::storage::StorageStats::new();
    let cfg = crate::storage::budget::StorageBackendConfig::default();
    let mem_pool = crate::storage::WorkerMemoryPool::new(cfg.memory);
    let in_mem = WriteBuffer::new_with_stats(stats.clone(), mem_pool.clone());
    let ctx = LoaderContext {
        backend: backend.clone(),
        pins,
        scratch: crate::storage::ScratchLimiter::new(cfg.concurrency.max_inflight_keys),
        write_buffer: Arc::new(in_mem),
        load_io_parallelism: 2,
    };
    let out = execute_planned_load(planned, &ctx, Arc::<str>::from("t"), &key, 0, &plans).await;
    drop(out);

    let calls_after = calls.load(Ordering::Relaxed);
    // Exactly one batch had missing estimate.
    assert_eq!(calls_after, 1);
}

#[tokio::test]
async fn pins_held_until_views_drop() {
    let sql = "SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
    FROM test_table
    WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW)";
    let window_expr = test_utils::window_expr_from_sql(sql).await;

    let key = make_key("A");
    let batch_id = BatchId::new(key.hash(), 0, 1);
    let stored_batch = make_batch(0, 0);

    let backend: DynStorageBackend = Arc::new(InMemBackend::new(TimeGranularity::Seconds(1), 16));
    backend
        .batch_put(Arc::<str>::from("t"), batch_id, stored_batch.clone(), &key)
        .await
        .unwrap();

    let mut idx = BucketIndex::new(TimeGranularity::Seconds(1));
    idx.insert_batch_ref(
        0,
        BatchRef::Stored(batch_id),
        Cursor::new(0, 0),
        Cursor::new(0, 0),
        stored_batch.num_rows(),
        stored_batch.get_array_memory_size(),
    );

    let plans = vec![RangesLoadPlan {
        requests: vec![DataRequest {
            bucket_range: BucketRange::new(0, 0),
            bounds: DataBounds::All,
        }],
        window_expr_for_args: window_expr.clone(),
    }];

    let planned = plan_load_from_index(&idx, &plans);

    let pins = Arc::new(BatchPins::new());
    let stats = crate::storage::StorageStats::new();
    let cfg = crate::storage::budget::StorageBackendConfig::default();
    let mem_pool = crate::storage::WorkerMemoryPool::new(cfg.memory);
    let in_mem = WriteBuffer::new_with_stats(stats.clone(), mem_pool.clone());
    let ctx = LoaderContext {
        backend: backend.clone(),
        pins: pins.clone(),
        scratch: crate::storage::ScratchLimiter::new(cfg.concurrency.max_inflight_keys),
        write_buffer: Arc::new(in_mem),
        load_io_parallelism: 2,
    };

    let task_id: TaskId = Arc::<str>::from("t");
    let out = execute_planned_load(planned, &ctx, task_id.clone(), &key, 0, &plans).await;
    assert!(pins.is_pinned(&task_id, &batch_id));
    drop(out);
    assert!(!pins.is_pinned(&task_id, &batch_id));
}

#[tokio::test]
async fn in_mem_snapshot_survives_eviction() {
    let sql = "SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
    FROM test_table
    WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp RANGE BETWEEN INTERVAL '1000' MILLISECOND PRECEDING AND CURRENT ROW)";
    let window_expr = test_utils::window_expr_from_sql(sql).await;

    let key = make_key("A");
    let cfg = crate::storage::budget::StorageBackendConfig::default();
    let mem_pool = crate::storage::WorkerMemoryPool::new(cfg.memory);
    let stats = crate::storage::StorageStats::new();
    let in_mem = Arc::new(WriteBuffer::new_with_stats(stats.clone(), mem_pool.clone()));
    let in_mem_id = in_mem.put(Arc::<str>::from("t"), make_batch(0, 0));

    let mut idx = BucketIndex::new(TimeGranularity::Seconds(1));
    idx.insert_batch_ref(
        0,
        BatchRef::InMem(in_mem_id),
        Cursor::new(0, 0),
        Cursor::new(0, 0),
        1,
        123,
    );

    let plans = vec![RangesLoadPlan {
        requests: vec![DataRequest {
            bucket_range: BucketRange::new(0, 0),
            bounds: DataBounds::All,
        }],
        window_expr_for_args: window_expr.clone(),
    }];
    let planned = plan_load_from_index(&idx, &plans);

    let pins = Arc::new(BatchPins::new());
    let ctx = LoaderContext {
        backend: Arc::new(InMemBackend::new(TimeGranularity::Seconds(1), 16)),
        pins,
        scratch: crate::storage::ScratchLimiter::new(cfg.concurrency.max_inflight_keys),
        write_buffer: in_mem.clone(),
        load_io_parallelism: 2,
    };

    let out = execute_planned_load(planned, &ctx, Arc::<str>::from("t"), &key, 0, &plans).await;
    ctx.write_buffer.remove(&[in_mem_id]);
    assert!(!out.is_empty());
    assert!(!out[0].is_empty());
    assert!(!out[0][0].segments().is_empty());
    assert!(out[0][0].segments()[0].size() > 0);
}
