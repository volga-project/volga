use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use dashmap::DashMap;

use crate::common::Key;
use crate::runtime::TaskId;
use crate::storage::index::TimeGranularity;
use crate::storage::batch::{
    BatchId,
    InMemBatchStoreCheckpoint,
    Timestamp,
    partition_records,
    record_batch_from_ipc_bytes,
    record_batch_to_ipc_bytes,
};
use super::{BoxFut, StorageBackend, StorageCheckpointToken};

#[derive(Debug)]
pub struct InMemBackend {
    bucket_granularity: TimeGranularity,
    max_batch_size: usize,
    // (task_id, batch_id) -> batch
    batches: Arc<DashMap<(TaskId, BatchId), RecordBatch>>,
    // (task_id, key) -> value
    state: Arc<DashMap<(TaskId, Vec<u8>), Vec<u8>>>,
}

impl InMemBackend {
    pub fn new(bucket_granularity: TimeGranularity, max_batch_size: usize) -> Self {
        Self {
            bucket_granularity,
            max_batch_size,
            batches: Arc::new(DashMap::new()),
            state: Arc::new(DashMap::new()),
        }
    }
}

impl StorageBackend for InMemBackend {
    fn bucket_granularity(&self) -> TimeGranularity {
        self.bucket_granularity
    }

    fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    fn partition_records(
        &self,
        batch: &RecordBatch,
        partition_key: &Key,
        ts_column_index: usize,
    ) -> Vec<(Timestamp, RecordBatch)> {
        partition_records(
            batch,
            partition_key,
            ts_column_index,
            self.bucket_granularity,
            self.max_batch_size,
        )
    }

    fn batch_get<'a>(
        &'a self,
        task_id: TaskId,
        batch_id: BatchId,
        _partition_key: &'a Key,
    ) -> BoxFut<'a, Option<RecordBatch>> {
        Box::pin(async move { self.batches.get(&(task_id, batch_id)).map(|b| b.value().clone()) })
    }

    fn batch_bytes_estimate<'a>(
        &'a self,
        task_id: TaskId,
        batch_id: BatchId,
        _partition_key: &'a Key,
    ) -> BoxFut<'a, Option<usize>> {
        Box::pin(async move {
            self.batches
                .get(&(task_id, batch_id))
                .map(|b| b.value().get_array_memory_size())
        })
    }

    fn batch_put<'a>(
        &'a self,
        task_id: TaskId,
        batch_id: BatchId,
        batch: RecordBatch,
        _partition_key: &'a Key,
    ) -> BoxFut<'a, anyhow::Result<()>> {
        Box::pin(async move {
            self.batches.insert((task_id, batch_id), batch);
            Ok(())
        })
    }

    fn batch_delete<'a>(
        &'a self,
        task_id: TaskId,
        batch_id: BatchId,
        _partition_key: &'a Key,
    ) -> BoxFut<'a, anyhow::Result<()>> {
        Box::pin(async move {
            self.batches.remove(&(task_id, batch_id));
            Ok(())
        })
    }

    fn state_get<'a>(&'a self, task_id: TaskId, key: &'a [u8]) -> BoxFut<'a, anyhow::Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            Ok(self
                .state
                .get(&(task_id, key.to_vec()))
                .map(|v| v.value().clone()))
        })
    }

    fn state_put<'a>(&'a self, task_id: TaskId, key: Vec<u8>, value: Vec<u8>) -> BoxFut<'a, anyhow::Result<()>> {
        Box::pin(async move {
            self.state.insert((task_id, key), value);
            Ok(())
        })
    }

    fn state_delete<'a>(&'a self, task_id: TaskId, key: &'a [u8]) -> BoxFut<'a, anyhow::Result<()>> {
        Box::pin(async move {
            self.state.remove(&(task_id, key.to_vec()));
            Ok(())
        })
    }

    fn await_persisted<'a>(&'a self) -> BoxFut<'a, anyhow::Result<()>> {
        Box::pin(async move { Ok(()) })
    }

    fn checkpoint<'a>(&'a self, task_id: TaskId) -> BoxFut<'a, anyhow::Result<StorageCheckpointToken>> {
        Box::pin(async move {
            let batches = self
                .batches
                .iter()
                .filter_map(|entry| {
                    let (t, id) = entry.key().clone();
                    (t.as_ref() == task_id.as_ref())
                        .then_some((id, record_batch_to_ipc_bytes(entry.value())))
                })
                .collect::<Vec<_>>();
            let batch_cp = InMemBatchStoreCheckpoint {
                bucket_granularity: self.bucket_granularity,
                max_batch_size: self.max_batch_size,
                batches,
            };

            let mut entries = Vec::new();
            for e in self.state.iter() {
                let (t, k) = e.key();
                if t.as_ref() != task_id.as_ref() {
                    continue;
                }
                entries.push((k.clone(), e.value().clone()));
            }

            Ok(StorageCheckpointToken::InMem {
                batches: batch_cp,
                state: entries,
            })
        })
    }

    fn apply_checkpoint<'a>(
        &'a self,
        task_id: TaskId,
        token: StorageCheckpointToken,
    ) -> BoxFut<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let (batch_cp, state_entries) = match token {
                StorageCheckpointToken::InMem { batches, state } => (batches, state),
                StorageCheckpointToken::Remote(_) => {
                    anyhow::bail!("cannot apply remote checkpoint token to InMemBackend")
                }
            };

            // Clear and repopulate the in-memory batch map for this task namespace only.
            self.batches.retain(|(t, _), _| t.as_ref() != task_id.as_ref());
            for (batch_id, bytes) in batch_cp.batches {
                let batch = record_batch_from_ipc_bytes(&bytes);
                self.batches.insert((task_id.clone(), batch_id), batch);
            }

            // Clear only task-scoped entries.
            let mut to_remove = Vec::new();
            for e in self.state.iter() {
                let (t, k) = e.key();
                if t.as_ref() == task_id.as_ref() {
                    to_remove.push((t.clone(), k.clone()));
                }
            }
            for k in to_remove {
                self.state.remove(&k);
            }

            for (k, v) in state_entries {
                self.state.insert((task_id.clone(), k), v);
            }

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{StringArray, TimestampMillisecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    fn make_key(s: &str) -> Key {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
        let arr = StringArray::from(vec![s]);
        let rb = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        Key::new(rb).unwrap()
    }

    fn make_batch(ts: &[i64], seq: &[u64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("__seq_no", DataType::UInt64, false),
        ]));
        let ts_arr = TimestampMillisecondArray::from(ts.to_vec());
        let seq_arr = UInt64Array::from(seq.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(ts_arr), Arc::new(seq_arr)]).unwrap()
    }

    #[tokio::test]
    async fn inmem_backend_checkpoint_roundtrip_restores_batches_and_state() {
        let backend = InMemBackend::new(TimeGranularity::Seconds(1), 1024);
        let task_id: TaskId = Arc::<str>::from("t");
        let key = make_key("A");
        let batch_id = BatchId::new(key.hash(), 0, 1);

        backend
            .batch_put(
                task_id.clone(),
                batch_id,
                make_batch(&[0, 1, 2], &[0, 1, 2]),
                &key,
            )
            .await
            .unwrap();
        backend
            .state_put(task_id.clone(), b"k1".to_vec(), b"v1".to_vec())
            .await
            .unwrap();
        backend
            .state_put(task_id.clone(), b"k2".to_vec(), b"v2".to_vec())
            .await
            .unwrap();

        let cp = backend.checkpoint(task_id.clone()).await.unwrap();

        backend
            .batch_delete(task_id.clone(), batch_id, &key)
            .await
            .unwrap();
        backend
            .state_put(task_id.clone(), b"k2".to_vec(), b"CHANGED".to_vec())
            .await
            .unwrap();
        backend
            .state_put(task_id.clone(), b"k3".to_vec(), b"EXTRA".to_vec())
            .await
            .unwrap();

        backend.apply_checkpoint(task_id.clone(), cp).await.unwrap();

        assert!(backend.batch_get(task_id.clone(), batch_id, &key).await.is_some());
        assert_eq!(
            backend.state_get(task_id.clone(), b"k1").await.unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            backend.state_get(task_id.clone(), b"k2").await.unwrap(),
            Some(b"v2".to_vec())
        );
        assert_eq!(backend.state_get(task_id, b"k3").await.unwrap(), None);
    }
}

