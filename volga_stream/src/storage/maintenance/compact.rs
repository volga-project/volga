use std::sync::Arc;
use std::time::Instant;

use tokio::sync::RwLock;

use arrow::array::{TimestampMillisecondArray, UInt64Array};
use arrow::record_batch::RecordBatch;

use crate::common::Key;
use crate::runtime::TaskId;
use crate::storage::batch::{BatchId, Timestamp};
use crate::storage::constants::SEQ_NO_COLUMN_NAME;
use crate::storage::index::bucket_index::{BatchMeta, BatchRef};
use crate::storage::index::Cursor;
use crate::storage::index::InMemBatchId;
use crate::storage::segments::compact_to_disjoint_segments;

use crate::storage::maintenance::types::BucketIndexState;
use crate::storage::maintenance::MaintenanceSupervisor;

pub async fn compact_bucket<T: BucketIndexState>(
    supervisor: &MaintenanceSupervisor,
    key: &Key,
    arc: &Arc<RwLock<T>>,
    task_id: TaskId,
    bucket_ts: Timestamp,
    ts_column_index: usize,
) {
    let lock = supervisor.get_bucket_lock(&task_id, key.hash(), bucket_ts);
    let _guard = lock.lock().await;
    let _ = compact_bucket_inner(supervisor, key, arc, task_id, bucket_ts, ts_column_index).await;
}

async fn compact_bucket_inner<T: BucketIndexState>(
    supervisor: &MaintenanceSupervisor,
    key: &Key,
    arc: &Arc<RwLock<T>>,
    task_id: TaskId,
    bucket_ts: Timestamp,
    ts_column_index: usize,
) -> anyhow::Result<bool> {
    let t0 = Instant::now();
    let (expected_version, run_refs, old_inmem, old_stored) = {
        let state = arc.read().await;
        let Some(bucket) = state.bucket_index().bucket(bucket_ts) else {
            return Ok(false);
        };

        let mut run_refs: Vec<BatchRef> = Vec::new();
        let mut old_inmem: Vec<InMemBatchId> = Vec::new();
        let mut old_stored: Vec<BatchId> = Vec::new();

        for m in bucket
            .hot_base_segments
            .iter()
            .chain(bucket.hot_deltas.iter())
        {
            run_refs.push(m.run);
            if let Some(id) = m.run.inmem_id() {
                old_inmem.push(id);
            } else if let Some(id) = m.run.stored_batch_id() {
                old_stored.push(id);
            }
        }

        (bucket.version, run_refs, old_inmem, old_stored)
    };

    if run_refs.is_empty() || run_refs.len() <= 1 {
        supervisor
            .stats
            .on_compact_finish(false, t0.elapsed().as_nanos() as u64);
        return Ok(false);
    }

    let mut input_batches: Vec<RecordBatch> = Vec::new();
    let mut stored_ids: Vec<BatchId> = Vec::new();
    for r in &run_refs {
        if let Some(id) = r.stored_batch_id() {
            stored_ids.push(id);
        }
    }
    if !stored_ids.is_empty() {
        for id in stored_ids {
            if let Some(b) = supervisor.backend.batch_get(task_id.clone(), id, key).await {
                input_batches.push(b);
            }
        }
    }
    for r in &run_refs {
        if let Some(id) = r.inmem_id() {
            if let Some(b) = supervisor.write_buffer.get(id) {
                input_batches.push((*b).clone());
            }
        }
    }
    if input_batches.is_empty() {
        supervisor
            .stats
            .on_compact_finish(false, t0.elapsed().as_nanos() as u64);
        return Ok(false);
    }

    let schema = input_batches[0].schema();
    let seq_column_index = schema
        .fields()
        .iter()
        .position(|f| f.name() == SEQ_NO_COLUMN_NAME)
        .expect("Expected __seq_no column to exist");

    let segments = compact_to_disjoint_segments(
        &input_batches,
        ts_column_index,
        seq_column_index,
        supervisor.backend.max_batch_size(),
    );
    if segments.is_empty() {
        supervisor
            .stats
            .on_compact_finish(false, t0.elapsed().as_nanos() as u64);
        return Ok(false);
    }

    let mut new_metas: Vec<BatchMeta> = Vec::new();
    let mut new_inmem_ids: Vec<InMemBatchId> = Vec::new();

    for seg in segments {
        if seg.num_rows() == 0 {
            continue;
        }
        let inmem_id = supervisor.write_buffer.put(task_id.clone(), seg.clone());
        new_inmem_ids.push(inmem_id);
        let ts_arr = seg
            .column(ts_column_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("timestamp column");
        let seq_arr = seg
            .column(seq_column_index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("__seq_no column");
        let n = seg.num_rows();
        let min_pos = Cursor::new(ts_arr.value(0), seq_arr.value(0));
        let max_pos = Cursor::new(ts_arr.value(n - 1), seq_arr.value(n - 1));
        new_metas.push(BatchMeta {
            run: BatchRef::InMem(inmem_id),
            bucket_timestamp: bucket_ts,
            min_pos,
            max_pos,
            row_count: n,
            bytes_estimate: seg.get_array_memory_size(),
        });
    }

    if new_metas.is_empty() {
        supervisor.write_buffer.remove(&new_inmem_ids);
        supervisor
            .stats
            .on_compact_finish(false, t0.elapsed().as_nanos() as u64);
        return Ok(false);
    }

    let published = {
        let mut state = arc.write().await;
        match state.bucket_index_mut().bucket_mut(bucket_ts) {
            None => false,
            Some(bucket) if bucket.version == expected_version => {
                if !old_stored.is_empty() {
                    use std::collections::HashSet;
                    let mut set: HashSet<BatchId> =
                        bucket.pending_delete_stored.iter().copied().collect();
                    set.extend(old_stored.iter().copied());
                    bucket.pending_delete_stored = set.into_iter().collect();
                }
                bucket.hot_base_segments = new_metas;
                bucket.hot_deltas.clear();
                true
            }
            Some(_) => false,
        }
    };

    if !published {
        supervisor.write_buffer.remove(&new_inmem_ids);
        supervisor
            .stats
            .on_compact_finish(false, t0.elapsed().as_nanos() as u64);
        return Ok(false);
    }

    if !old_inmem.is_empty() {
        supervisor.write_buffer.remove(&old_inmem);
    }

    supervisor
        .stats
        .on_compact_finish(true, t0.elapsed().as_nanos() as u64);
    Ok(true)
}

pub async fn periodic_compact_in_mem<T: BucketIndexState>(
    supervisor: &MaintenanceSupervisor,
    window_states: &dashmap::DashMap<Key, Arc<RwLock<T>>>,
    task_id: TaskId,
    ts_column_index: usize,
    hot_bucket_count: usize,
) -> anyhow::Result<()> {
    for entry in window_states.iter() {
        let key = entry.key().clone();
        let arc = entry.value().clone();

        let to_compact: Vec<Timestamp> = {
            let guard = arc
                .try_read()
                .expect("Failed to read windows state for periodic compaction planning");
            let idx = guard.bucket_index();
            let bucket_ts_list = idx.bucket_timestamps(); // ascending
            if bucket_ts_list.is_empty() {
                Vec::new()
            } else {
                let cutoff = bucket_ts_list.len().saturating_sub(hot_bucket_count);
                bucket_ts_list
                    .into_iter()
                    .take(cutoff)
                    .filter(|bucket_ts| {
                        idx.bucket(*bucket_ts)
                            .map(|b| b.should_compact())
                            .unwrap_or(false)
                    })
                    .collect()
            }
        };

        for bucket_ts in to_compact {
            compact_bucket(
                supervisor,
                &key,
                &arc,
                task_id.clone(),
                bucket_ts,
                ts_column_index,
            )
            .await;
        }
    }
    Ok(())
}
