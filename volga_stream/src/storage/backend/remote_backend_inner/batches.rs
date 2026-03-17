use arrow::record_batch::RecordBatch;

use crate::common::Key;
use crate::runtime::TaskId;
use crate::storage::index::TimeGranularity;
use crate::storage::batch::{BatchId, BoxFut, Timestamp, partition_records, record_batch_from_ipc_bytes, record_batch_to_ipc_bytes};

use super::RemoteBackendInner;

impl RemoteBackendInner {
    pub fn bucket_granularity(&self) -> TimeGranularity {
        self.cfg.bucket_granularity
    }

    pub fn max_batch_size(&self) -> usize {
        self.cfg.max_batch_size
    }

    pub fn partition_records(
        &self,
        batch: &RecordBatch,
        partition_key: &Key,
        ts_column_index: usize,
    ) -> Vec<(Timestamp, RecordBatch)> {
        partition_records(
            batch,
            partition_key,
            ts_column_index,
            self.cfg.bucket_granularity,
            self.cfg.max_batch_size,
        )
    }

    fn key_bytes(task_id: &TaskId, batch_id: BatchId) -> Vec<u8> {
        format!("{}/{}", task_id.as_ref(), batch_id.to_string()).into_bytes()
    }

    pub fn load_batch<'a>(&'a self, task_id: TaskId, batch_id: BatchId) -> BoxFut<'a, Option<RecordBatch>> {
        Box::pin(async move {
            let key = Self::key_bytes(&task_id, batch_id);
            let db = self.db.read().clone();
            match db.get(&key).await {
                Ok(Some(bytes)) => Some(record_batch_from_ipc_bytes(bytes.as_ref())),
                _ => None,
            }
        })
    }

    pub fn put_batch_with_id<'a>(
        &'a self,
        task_id: TaskId,
        batch_id: BatchId,
        batch: RecordBatch,
    ) -> BoxFut<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let key = Self::key_bytes(&task_id, batch_id);
            let bytes = record_batch_to_ipc_bytes(&batch);
            let db = self.db.read().clone();
            db.put_with_options(&key, bytes, &slatedb::config::PutOptions::default(), &Self::write_opts_low_latency())
                .await
                .map(|_| ())
                .map_err(|e| anyhow::anyhow!(e.to_string()))
        })
    }

    pub fn remove_batch<'a>(&'a self, task_id: TaskId, batch_id: BatchId) -> BoxFut<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let key = Self::key_bytes(&task_id, batch_id);
            let db = self.db.read().clone();
            db.delete_with_options(&key, &Self::write_opts_low_latency())
                .await
                .map(|_| ())
                .map_err(|e| anyhow::anyhow!(e.to_string()))
        })
    }
}

