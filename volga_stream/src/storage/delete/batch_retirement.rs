use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::common::Key;
use crate::storage::backend::DynStorageBackend;
use crate::storage::delete::batch_pins::BatchPins;
use crate::storage::batch::BatchId;
use crate::runtime::TaskId;

/// Tracks batches that were removed from metadata and should be deleted once unpinned.
#[derive(Debug, Default)]
pub struct BatchRetirementQueue {
    // (task_id, batch_id) -> partition_key (needed for store deletion)
    retired: DashMap<(TaskId, BatchId), Key>,
}

impl BatchRetirementQueue {
    pub fn new() -> Self {
        Self {
            retired: DashMap::new(),
        }
    }

    /// Schedule batches for eventual deletion.
    pub fn retire(&self, task_id: TaskId, partition_key: &Key, batch_ids: &[BatchId]) {
        for id in batch_ids {
            self.retired
                .entry((task_id.clone(), *id))
                .or_insert_with(|| partition_key.clone());
        }
    }

    /// Try deleting up to `max_batches` retired batches that are currently unpinned.
    pub async fn delete_once(
        &self,
        backend: DynStorageBackend,
        pins: &BatchPins,
        task_id: &TaskId,
        max_batches: usize,
    ) {
        if max_batches == 0 {
            return;
        }

        // Collect candidates without holding iter guards across await.
        let mut candidates: Vec<(BatchId, Key)> = Vec::new();
        for entry in self.retired.iter() {
            if candidates.len() >= max_batches {
                break;
            }
            let (t, id) = entry.key().clone();
            if &t != task_id {
                continue;
            }
            if pins.is_pinned(task_id, &id) {
                continue;
            }
            candidates.push((id, entry.value().clone()));
        }
        if candidates.is_empty() {
            return;
        }

        // Best-effort bounded fanout: store API is singular.
        let sem = Arc::new(Semaphore::new(8));
        let mut futs: FuturesUnordered<_> = FuturesUnordered::new();
        for (id, key) in &candidates {
            let backend = backend.clone();
            let key = key.clone();
            let id = *id;
            let sem = sem.clone();
            let task_id = task_id.clone();
            futs.push(async move {
                let _p = sem.acquire_owned().await.expect("semaphore");
                let _ = backend.batch_delete(task_id, id, &key).await;
            });
        }
        while futs.next().await.is_some() {}

        for (id, _) in candidates {
            self.retired.remove(&(task_id.clone(), id));
        }
    }

    /// Spawn a background task that periodically deletes unpinned retired batches.
    pub fn spawn_deleter(
        self: Arc<Self>,
        backend: DynStorageBackend,
        pins: Arc<BatchPins>,
        task_id: TaskId,
        interval: Duration,
        batch_budget_per_tick: usize,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            loop {
                tick.tick().await;
                self.delete_once(backend.clone(), &pins, &task_id, batch_budget_per_tick)
                    .await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::record_batch::RecordBatch;

    use crate::storage::index::TimeGranularity;
    use crate::storage::batch::Timestamp;
    use crate::storage::backend::StorageBackend;

    #[derive(Clone, Debug)]
    struct DeleteCountingBackend {
        deletes: Arc<AtomicUsize>,
    }

    impl StorageBackend for DeleteCountingBackend {
        fn bucket_granularity(&self) -> TimeGranularity {
            TimeGranularity::Seconds(1)
        }

        fn max_batch_size(&self) -> usize {
            1024
        }

        fn partition_records(
            &self,
            _batch: &RecordBatch,
            _partition_key: &Key,
            _ts_column_index: usize,
        ) -> Vec<(Timestamp, RecordBatch)> {
            Vec::new()
        }

        fn batch_get<'a>(
            &'a self,
            _task_id: TaskId,
            _batch_id: BatchId,
            _partition_key: &'a Key,
        ) -> crate::storage::batch::BoxFut<'a, Option<RecordBatch>> {
            Box::pin(async move { None })
        }

        fn batch_put<'a>(
            &'a self,
            _task_id: TaskId,
            _batch_id: BatchId,
            _batch: RecordBatch,
            _partition_key: &'a Key,
        ) -> crate::storage::batch::BoxFut<'a, anyhow::Result<()>> {
            Box::pin(async move { Ok(()) })
        }

        fn batch_delete<'a>(
            &'a self,
            _task_id: TaskId,
            _batch_id: BatchId,
            _partition_key: &'a Key,
        ) -> crate::storage::batch::BoxFut<'a, anyhow::Result<()>> {
            let deletes = self.deletes.clone();
            Box::pin(async move {
                deletes.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
        }

        fn state_get<'a>(
            &'a self,
            _task_id: TaskId,
            _key: &'a [u8],
        ) -> crate::storage::batch::BoxFut<'a, anyhow::Result<Option<Vec<u8>>>> {
            Box::pin(async move { Ok(None) })
        }

        fn state_put<'a>(
            &'a self,
            _task_id: TaskId,
            _key: Vec<u8>,
            _value: Vec<u8>,
        ) -> crate::storage::batch::BoxFut<'a, anyhow::Result<()>> {
            Box::pin(async move { Ok(()) })
        }

        fn state_delete<'a>(
            &'a self,
            _task_id: TaskId,
            _key: &'a [u8],
        ) -> crate::storage::batch::BoxFut<'a, anyhow::Result<()>> {
            Box::pin(async move { Ok(()) })
        }

        fn await_persisted<'a>(&'a self) -> crate::storage::batch::BoxFut<'a, anyhow::Result<()>> {
            Box::pin(async move { Ok(()) })
        }

        fn checkpoint<'a>(
            &'a self,
            _task_id: TaskId,
        ) -> crate::storage::batch::BoxFut<'a, anyhow::Result<crate::storage::StorageCheckpointToken>> {
            Box::pin(async move {
                Err(anyhow::anyhow!("checkpoint not supported in test backend"))
            })
        }

        fn apply_checkpoint<'a>(
            &'a self,
            _task_id: TaskId,
            _token: crate::storage::StorageCheckpointToken,
        ) -> crate::storage::batch::BoxFut<'a, anyhow::Result<()>> {
            Box::pin(async move { Ok(()) })
        }
    }

    fn make_key(s: &str) -> Key {
        use arrow::array::StringArray;
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
        let arr = StringArray::from(vec![s]);
        let rb = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        Key::new(rb).unwrap()
    }

    #[tokio::test]
    async fn delete_skips_pinned_batches() {
        let deletes = Arc::new(AtomicUsize::new(0));
        let backend: DynStorageBackend = Arc::new(DeleteCountingBackend { deletes: deletes.clone() });
        let pins = Arc::new(BatchPins::new());
        let retired = BatchRetirementQueue::new();

        let task_id: TaskId = Arc::<str>::from("t");
        let key = make_key("A");
        let batch_id = BatchId::new(1, 2, 3);

        pins.pin_one(task_id.clone(), batch_id);
        retired.retire(task_id.clone(), &key, &[batch_id]);

        retired
            .delete_once(backend.clone(), &pins, &task_id, 10)
            .await;
        assert_eq!(deletes.load(Ordering::Relaxed), 0);

        pins.unpin_one(task_id.clone(), batch_id);
        retired
            .delete_once(backend.clone(), &pins, &task_id, 10)
            .await;
        assert_eq!(deletes.load(Ordering::Relaxed), 1);
    }
}
