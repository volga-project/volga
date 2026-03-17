use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::common::Key;
use crate::runtime::TaskId;
use crate::storage::index::TimeGranularity;
use crate::storage::batch::{BatchId, InMemBatchStoreCheckpoint, RemoteCheckpointToken, Timestamp, BoxFut};

pub mod inmem;
pub mod remote_backend;
pub mod remote_backend_inner;

/// Unified storage checkpoint token (no backward compatibility guarantees).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageCheckpointToken {
    InMem {
        batches: InMemBatchStoreCheckpoint,
        state: Vec<(Vec<u8>, Vec<u8>)>,
    },
    Remote(RemoteCheckpointToken),
}

/// Detached persistence interface.
///
/// This combines the physical batch and state KV APIs so checkpoint/restore can be atomic.
pub trait StorageBackend: Send + Sync + std::fmt::Debug {
    fn bucket_granularity(&self) -> TimeGranularity;
    fn max_batch_size(&self) -> usize;

    fn partition_records(
        &self,
        batch: &RecordBatch,
        partition_key: &Key,
        ts_column_index: usize,
    ) -> Vec<(Timestamp, RecordBatch)>;

    fn batch_get<'a>(
        &'a self,
        task_id: TaskId,
        batch_id: BatchId,
        partition_key: &'a Key,
    ) -> BoxFut<'a, Option<RecordBatch>>;

    /// Best-effort stored batch size estimate in bytes (metadata-only if possible).
    ///
    /// Used for work-memory admission before hydration. Returning `None` is allowed.
    fn batch_bytes_estimate<'a>(
        &'a self,
        _task_id: TaskId,
        _batch_id: BatchId,
        _partition_key: &'a Key,
    ) -> BoxFut<'a, Option<usize>> {
        Box::pin(async move { None })
    }

    fn batch_put<'a>(
        &'a self,
        task_id: TaskId,
        batch_id: BatchId,
        batch: RecordBatch,
        partition_key: &'a Key,
    ) -> BoxFut<'a, anyhow::Result<()>>;
    fn batch_delete<'a>(
        &'a self,
        task_id: TaskId,
        batch_id: BatchId,
        partition_key: &'a Key,
    ) -> BoxFut<'a, anyhow::Result<()>>;

    fn state_get<'a>(&'a self, task_id: TaskId, key: &'a [u8]) -> BoxFut<'a, anyhow::Result<Option<Vec<u8>>>>;
    fn state_put<'a>(&'a self, task_id: TaskId, key: Vec<u8>, value: Vec<u8>) -> BoxFut<'a, anyhow::Result<()>>;
    fn state_delete<'a>(&'a self, task_id: TaskId, key: &'a [u8]) -> BoxFut<'a, anyhow::Result<()>>;

    fn await_persisted<'a>(&'a self) -> BoxFut<'a, anyhow::Result<()>>;

    fn checkpoint<'a>(&'a self, task_id: TaskId) -> BoxFut<'a, anyhow::Result<StorageCheckpointToken>>;
    fn apply_checkpoint<'a>(
        &'a self,
        task_id: TaskId,
        token: StorageCheckpointToken,
    ) -> BoxFut<'a, anyhow::Result<()>>;
}

pub type DynStorageBackend = Arc<dyn StorageBackend>;

