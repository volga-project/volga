use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use slatedb::object_store::ObjectStore;

use crate::common::Key;
use crate::runtime::TaskId;
use crate::storage::index::TimeGranularity;
use crate::storage::batch::{BatchId, Timestamp};
use super::{BoxFut, StorageBackend, StorageCheckpointToken};
use super::remote_backend_inner::{RemoteBackendInner, RemoteBackendInnerConfig};

#[derive(Debug)]
pub struct RemoteBackend {
    inner: Arc<RemoteBackendInner>,
}

impl RemoteBackend {
    pub async fn open(
        cfg: RemoteBackendInnerConfig,
        object_store: Arc<dyn ObjectStore>,
    ) -> anyhow::Result<Self> {
        let inner = Arc::new(RemoteBackendInner::open(cfg, object_store).await?);
        Ok(Self { inner })
    }

    fn state_key_bytes(task_id: &TaskId, key: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(6 + task_id.as_ref().len() + 1 + key.len());
        out.extend_from_slice(b"state/");
        out.extend_from_slice(task_id.as_ref().as_bytes());
        out.push(b'/');
        out.extend_from_slice(key);
        out
    }
}

impl StorageBackend for RemoteBackend {
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
        _partition_key: &'a Key,
    ) -> BoxFut<'a, Option<RecordBatch>> {
        self.inner.load_batch(task_id, batch_id)
    }

    fn batch_put<'a>(
        &'a self,
        task_id: TaskId,
        batch_id: BatchId,
        batch: RecordBatch,
        _partition_key: &'a Key,
    ) -> BoxFut<'a, anyhow::Result<()>> {
        Box::pin(async move {
            self.inner.put_batch_with_id(task_id, batch_id, batch).await
        })
    }

    fn batch_delete<'a>(
        &'a self,
        task_id: TaskId,
        batch_id: BatchId,
        _partition_key: &'a Key,
    ) -> BoxFut<'a, anyhow::Result<()>> {
        self.inner.remove_batch(task_id, batch_id)
    }

    fn state_get<'a>(
        &'a self,
        task_id: TaskId,
        key: &'a [u8],
    ) -> BoxFut<'a, anyhow::Result<Option<Vec<u8>>>> {
        self.inner.get_raw_bytes(Self::state_key_bytes(&task_id, key))
    }

    fn state_put<'a>(
        &'a self,
        task_id: TaskId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> BoxFut<'a, anyhow::Result<()>> {
        Box::pin(async move {
            self.inner
                .put_raw_bytes(Self::state_key_bytes(&task_id, &key), value)
                .await
        })
    }

    fn state_delete<'a>(
        &'a self,
        task_id: TaskId,
        key: &'a [u8],
    ) -> BoxFut<'a, anyhow::Result<()>> {
        self.inner.delete_raw_bytes(Self::state_key_bytes(&task_id, key))
    }

    fn await_persisted<'a>(&'a self) -> BoxFut<'a, anyhow::Result<()>> {
        self.inner.await_persisted()
    }

    fn checkpoint<'a>(&'a self, task_id: TaskId) -> BoxFut<'a, anyhow::Result<StorageCheckpointToken>> {
        Box::pin(async move {
            let token = self.inner.to_checkpoint(task_id).await?;
            Ok(StorageCheckpointToken::Remote(token))
        })
    }

    fn apply_checkpoint<'a>(
        &'a self,
        _task_id: TaskId,
        token: StorageCheckpointToken,
    ) -> BoxFut<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let remote = match token {
                StorageCheckpointToken::Remote(t) => t,
                StorageCheckpointToken::InMem { .. } => {
                    anyhow::bail!("cannot apply in-mem checkpoint token to RemoteBackend")
                }
            };
            self.inner.apply_checkpoint(remote).await
        })
    }
}

