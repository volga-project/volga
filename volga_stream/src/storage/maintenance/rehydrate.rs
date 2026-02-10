use std::sync::Arc;

use tokio::sync::RwLock;

use arrow::record_batch::RecordBatch;

use crate::common::Key;
use crate::runtime::TaskId;
use crate::storage::batch::{BatchId, Timestamp};
use crate::storage::index::bucket_index::{BatchMeta, BatchRef};
use crate::storage::index::InMemBatchId;

use crate::storage::maintenance::types::BucketIndexState;
use crate::storage::maintenance::MaintenanceSupervisor;

pub async fn rehydrate_bucket<T: BucketIndexState>(
    supervisor: &MaintenanceSupervisor,
    key: &Key,
    arc: &Arc<RwLock<T>>,
    task_id: TaskId,
    bucket_ts: Timestamp,
) {
    let lock = supervisor.get_bucket_lock(&task_id, key.hash(), bucket_ts);
    let _guard = lock.lock().await;
    let _ = rehydrate_bucket_inner(supervisor, key, arc, task_id, bucket_ts).await;
}

async fn rehydrate_bucket_inner<T: BucketIndexState>(
    supervisor: &MaintenanceSupervisor,
    key: &Key,
    arc: &Arc<RwLock<T>>,
    task_id: TaskId,
    bucket_ts: Timestamp,
) -> anyhow::Result<bool> {
    let (expected_version, persisted) = {
        let state = arc.read().await;
        let Some(bucket) = state.bucket_index().bucket(bucket_ts) else {
            return Ok(false);
        };
        if !bucket.hot_base_segments.is_empty() {
            return Ok(false);
        }
        if bucket.persisted_segments.is_empty() {
            return Ok(false);
        }
        (bucket.version, bucket.persisted_segments.clone())
    };

    let stored_ids: Vec<BatchId> = persisted
        .iter()
        .filter_map(|m| m.run.stored_batch_id())
        .collect();
    if stored_ids.is_empty() {
        return Ok(false);
    }

    let _lease = supervisor
        .batch_pins
        .clone()
        .pin(task_id.clone(), stored_ids.clone());
    let mut loaded: std::collections::HashMap<BatchId, RecordBatch> =
        std::collections::HashMap::new();
    for id in stored_ids {
        if let Some(b) = supervisor.backend.batch_get(task_id.clone(), id, key).await {
            loaded.insert(id, b);
        }
    }

    let mut new_inmem_ids: Vec<InMemBatchId> = Vec::new();
    let mut new_metas: Vec<BatchMeta> = Vec::new();

    for meta in &persisted {
        let Some(stored_id) = meta.run.stored_batch_id() else {
            continue;
        };
        let Some(batch) = loaded.get(&stored_id) else {
            continue;
        };
        let inmem_id = supervisor.write_buffer.put(task_id.clone(), batch.clone());
        new_inmem_ids.push(inmem_id);
        new_metas.push(BatchMeta {
            run: BatchRef::InMem(inmem_id),
            bucket_timestamp: bucket_ts,
            min_pos: meta.min_pos,
            max_pos: meta.max_pos,
            row_count: meta.row_count,
            bytes_estimate: batch.get_array_memory_size(),
        });
    }

    if new_metas.is_empty() {
        if !new_inmem_ids.is_empty() {
            supervisor.write_buffer.remove(&new_inmem_ids);
        }
        return Ok(false);
    }

    let published = {
        let mut state = arc.write().await;
        match state.bucket_index_mut().bucket_mut(bucket_ts) {
            None => false,
            Some(bucket)
                if bucket.version == expected_version && bucket.hot_base_segments.is_empty() =>
            {
                bucket.hot_base_segments = new_metas;
                true
            }
            Some(_) => false,
        }
    };

    if !published {
        supervisor.write_buffer.remove(&new_inmem_ids);
        return Ok(false);
    }

    Ok(true)
}
