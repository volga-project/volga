pub mod state_cache;
mod state_handle;
pub mod state_serde;
pub mod write_buffer;

pub use state_handle::StateHandle;

use std::sync::Arc;
use arrow::record_batch::RecordBatch;

use crate::common::Key;
use crate::runtime::TaskId;
use crate::storage::constants::SEQ_NO_COLUMN_NAME;
use crate::storage::index::TimeGranularity;
use crate::storage::StorageStatsSnapshot;
use crate::storage::batch::Timestamp;
use crate::storage::index::{Cursor, InMemBatchId};
use crate::storage::WorkerStorageRuntime;

/// Operator-facing write API.
#[derive(Clone)]
pub struct StorageWriter {
    storage: Arc<WorkerStorageRuntime>,
}

#[derive(Debug, Clone)]
pub struct AppendBatch {
    pub bucket_ts: Timestamp,
    pub in_mem_id: InMemBatchId,
    pub batch: Arc<RecordBatch>,
    pub min_pos: Cursor,
    pub max_pos: Cursor,
    pub row_count: usize,
    pub bytes_estimate: usize,
}

impl StorageWriter {
    pub fn new(storage: &Arc<WorkerStorageRuntime>) -> Self {
        Self {
            storage: storage.clone(),
        }
    }

    pub fn bucket_granularity(&self) -> TimeGranularity {
        self.storage.backend.bucket_granularity()
    }

    pub async fn append(
        &self,
        task_id: TaskId,
        key: &Key,
        ts_column_index: usize,
        batch: RecordBatch,
    ) -> Vec<AppendBatch> {
        if batch.num_rows() == 0 {
            return Vec::new();
        }
        self.storage.backlog.acquire(batch.get_array_memory_size()).await;
        let parts = self
            .storage
            .backend
            .partition_records(&batch, key, ts_column_index);
        let mut out = Vec::with_capacity(parts.len());
        for (bucket_ts, part) in parts {
            if part.num_rows() == 0 {
                continue;
            }
            let seq_column_index = part
                .schema()
                .fields()
                .iter()
                .position(|f| f.name() == SEQ_NO_COLUMN_NAME)
                .expect("Expected __seq_no column to exist in hot runs");
            let (min_pos, max_pos) =
                batch_rowpos_range(&part, ts_column_index, seq_column_index);
            let row_count = part.num_rows();
            let bytes_estimate = part.get_array_memory_size();
            let id = self.storage.write_buffer.put(task_id.clone(), part);
            let batch = self
                .storage
                .write_buffer
                .get(id)
                .expect("write buffer entry");
            out.push(AppendBatch {
                bucket_ts,
                in_mem_id: id,
                batch,
                min_pos,
                max_pos,
                row_count,
                bytes_estimate,
            });
        }
        out
    }

    pub fn is_over_limit(&self, task_id: &TaskId) -> bool {
        self.storage.write_buffer.is_over_limit(task_id)
    }

    pub fn drop_inmem(&self, ids: &[InMemBatchId]) {
        self.storage.write_buffer.remove(ids);
    }

    pub fn write_buffer_clear(&self) {
        self.storage.write_buffer.clear();
    }

    pub fn write_buffer_soft_max_bytes(&self) -> usize {
        self.storage.write_buffer.soft_max_bytes()
    }

    pub fn write_buffer_bytes(&self) -> usize {
        self.storage.write_buffer.bytes()
    }

    pub fn write_buffer_stats_snapshot(&self) -> StorageStatsSnapshot {
        self.storage.write_buffer.stats().snapshot()
    }


    pub fn record_key_access(&self, task_id: TaskId, key: &Key) {
        self.storage.maintenance.record_key_access(task_id, key);
    }

    pub fn retire_stored(&self, task_id: TaskId, key: &Key, ids: &[crate::storage::batch::BatchId]) {
        self.storage.batch_retirement.retire(task_id, key, ids);
    }

    pub async fn await_store_persisted(&self) -> anyhow::Result<()> {
        self.storage.backend.await_persisted().await
    }

    pub fn backlog_release(&self, bytes: usize) {
        self.storage.backlog.release(bytes);
    }
}

fn batch_rowpos_range(
    batch: &RecordBatch,
    ts_column_index: usize,
    seq_column_index: usize,
) -> (Cursor, Cursor) {
    use arrow::array::{TimestampMillisecondArray, UInt64Array};

    let ts = batch
        .column(ts_column_index)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("Timestamp column should be TimestampMillisecondArray");
    let seq = batch
        .column(seq_column_index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("__seq_no column should be UInt64Array");

    let mut min_pos = Cursor::new(i64::MAX, u64::MAX);
    let mut max_pos = Cursor::new(i64::MIN, 0);
    for i in 0..batch.num_rows() {
        let p = Cursor::new(ts.value(i), seq.value(i));
        if p < min_pos {
            min_pos = p;
        }
        if p > max_pos {
            max_pos = p;
        }
    }
    (min_pos, max_pos)
}
