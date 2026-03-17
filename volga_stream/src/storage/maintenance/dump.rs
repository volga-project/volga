use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::RwLock;
use tokio::sync::Semaphore;

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

use super::types::{BucketIndexState, DumpMode, DumpPlan};
use crate::storage::maintenance::MaintenanceSupervisor;

pub async fn dump_plans_concurrent<T: BucketIndexState>(
    supervisor: &MaintenanceSupervisor,
    plans: Vec<DumpPlan<T>>,
    ts_column_index: usize,
    mode: DumpMode,
    parallelism: usize,
    update_last_dumped: bool,
) -> anyhow::Result<usize> {
    if plans.is_empty() {
        return Ok(0);
    }

    let parallelism = parallelism.max(1);
    let sem = Arc::new(Semaphore::new(parallelism));
    let mut futs: FuturesUnordered<_> = FuturesUnordered::new();

    for plan in plans {
        let permit = sem.clone();
        let this = supervisor.clone();
        futs.push(async move {
            let _p = permit.acquire_owned().await.expect("semaphore");

            let lock = this.get_bucket_lock(&plan.task_id, plan.key_hash, plan.bucket_ts);
            let _guard = lock.lock().await;

            let should_dump = {
                let state = plan.arc.read().await;
                match state.bucket_index().bucket(plan.bucket_ts) {
                    None => false,
                    Some(bucket) => bucket.version == plan.expected_version,
                }
            };
            if !should_dump {
                return Ok::<bool, anyhow::Error>(false);
            }

            let published = dump_bucket(
                &this,
                &plan.key,
                &plan.arc,
                plan.task_id.clone(),
                plan.bucket_ts,
                ts_column_index,
                mode,
            )
            .await?;
            if published && update_last_dumped {
                this.last_dumped_version.insert(
                    (plan.task_id.clone(), plan.key_hash, plan.bucket_ts),
                    plan.expected_version,
                );
            }
            Ok(published)
        });
    }

    let mut published_count = 0usize;
    while let Some(res) = futs.next().await {
        if res? {
            published_count += 1;
        }
    }
    Ok(published_count)
}

pub async fn dump_bucket<T: BucketIndexState>(
    supervisor: &MaintenanceSupervisor,
    key: &Key,
    arc: &Arc<RwLock<T>>,
    task_id: TaskId,
    bucket_ts: Timestamp,
    ts_column_index: usize,
    mode: DumpMode,
) -> anyhow::Result<bool> {
    let t0 = Instant::now();
    supervisor.stats.on_dump_start();
    let key_hash = key.hash();

    let (expected_version, run_refs, old_inmem, prev_persisted, pending_stored) = {
        let state = arc.read().await;
        let Some(bucket) = state.bucket_index().bucket(bucket_ts) else {
            return Ok(false);
        };
        let use_hot_base = !bucket.hot_base_segments.is_empty();

        let mut run_refs: Vec<BatchRef> = Vec::new();
        let mut old_inmem: Vec<InMemBatchId> = Vec::new();
        let prev_persisted: Vec<BatchId> = bucket
            .persisted_segments
            .iter()
            .filter_map(|m| m.run.stored_batch_id())
            .collect();

        let base = if use_hot_base {
            &bucket.hot_base_segments
        } else {
            &bucket.persisted_segments
        };

        for m in base.iter().chain(bucket.hot_deltas.iter()) {
            run_refs.push(m.run);
            if let Some(id) = m.run.inmem_id() {
                old_inmem.push(id);
            }
        }
        (
            bucket.version,
            run_refs,
            old_inmem,
            prev_persisted,
            bucket.pending_delete_stored.clone(),
        )
    };

    if run_refs.is_empty() {
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
            .on_dump_finish(false, t0.elapsed().as_nanos() as u64);
        return Ok(false);
    }

    let mut new_metas: Vec<BatchMeta> = Vec::new();
    let mut new_stored_ids: Vec<BatchId> = Vec::new();
    let mut written_segments: usize = 0;
    let mut written_bytes: usize = 0;

    for seg in segments {
        if seg.num_rows() == 0 {
            continue;
        }
        let new_id = BatchId::new(key_hash, bucket_ts, rand::random());
        supervisor
            .backend
            .batch_put(task_id.clone(), new_id, seg.clone(), key)
            .await?;
        new_stored_ids.push(new_id);
        written_segments += 1;
        written_bytes += seg.get_array_memory_size();

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
            run: BatchRef::Stored(new_id),
            bucket_timestamp: bucket_ts,
            min_pos,
            max_pos,
            row_count: n,
            bytes_estimate: seg.get_array_memory_size(),
        });
    }

    if new_metas.is_empty() {
        supervisor
            .retired
            .retire(task_id.clone(), key, &new_stored_ids);
        supervisor
            .stats
            .on_dump_finish(false, t0.elapsed().as_nanos() as u64);
        return Ok(false);
    }

    supervisor
        .stats
        .on_dump_written(written_segments, written_bytes);

    let published = {
        let mut state = arc.write().await;
        match state.bucket_index_mut().bucket_mut(bucket_ts) {
            None => false,
            Some(bucket) if bucket.version == expected_version => {
                bucket.persisted_segments = new_metas;
                bucket.pending_delete_stored.clear();
                if mode == DumpMode::EvictHot {
                    bucket.hot_base_segments.clear();
                    bucket.hot_deltas.clear();
                }
                true
            }
            Some(_) => false,
        }
    };

    if !published {
        supervisor
            .retired
            .retire(task_id.clone(), key, &new_stored_ids);
        supervisor
            .stats
            .on_dump_finish(false, t0.elapsed().as_nanos() as u64);
        return Ok(false);
    }

    if !prev_persisted.is_empty() || !pending_stored.is_empty() {
        use std::collections::HashSet;
        let mut all: HashSet<BatchId> = HashSet::new();
        all.extend(prev_persisted);
        all.extend(pending_stored);
        let all_vec: Vec<BatchId> = all.into_iter().collect();
        supervisor.retired.retire(task_id.clone(), key, &all_vec);
    }
    if mode == DumpMode::EvictHot && !old_inmem.is_empty() {
        supervisor.write_buffer.remove(&old_inmem);
    }

    supervisor
        .stats
        .on_dump_finish(true, t0.elapsed().as_nanos() as u64);
    Ok(true)
}

pub async fn periodic_dump_to_store<T: BucketIndexState>(
    supervisor: &MaintenanceSupervisor,
    window_states: &DashMap<Key, Arc<RwLock<T>>>,
    task_id: TaskId,
    ts_column_index: usize,
    hot_bucket_count: usize,
    dump_parallelism: usize,
) -> anyhow::Result<()> {
    let mut plans: Vec<DumpPlan<T>> = Vec::new();
    for entry in window_states.iter() {
        let key = entry.key().clone();
        let arc = entry.value().clone();
        let key_hash = key.hash();

        let to_dump: Vec<(Timestamp, u64)> = {
            let guard = arc
                .try_read()
                .expect("Failed to read windows state for periodic dump planning");
            let idx = guard.bucket_index();
            let bucket_ts_list = idx.bucket_timestamps(); // ascending
            if bucket_ts_list.is_empty() {
                Vec::new()
            } else {
                let cold_len = bucket_ts_list.len().saturating_sub(hot_bucket_count);
                let mut out: Vec<(Timestamp, u64)> = Vec::new();
                for &bucket_ts in bucket_ts_list.iter().take(cold_len) {
                    let Some(bucket) = idx.bucket(bucket_ts) else {
                        continue;
                    };
                    let has_inmem = bucket
                        .hot_base_segments
                        .iter()
                        .chain(bucket.hot_deltas.iter())
                        .any(|m| m.run.inmem_id().is_some());
                    if !has_inmem {
                        continue;
                    }
                    let last_dumped = supervisor
                        .last_dumped_version
                        .get(&(task_id.clone(), key_hash, bucket_ts))
                        .map(|e| *e.value())
                        .unwrap_or(0);
                    if bucket.version == last_dumped {
                        continue;
                    }
                    out.push((bucket_ts, bucket.version));
                }
                out
            }
        };

        for (bucket_ts, expected_version) in to_dump {
            plans.push(DumpPlan {
                task_id: task_id.clone(),
                key: key.clone(),
                arc: arc.clone(),
                bucket_ts,
                expected_version,
                key_hash,
            });
        }
    }

    let _ = dump_plans_concurrent(
        supervisor,
        plans,
        ts_column_index,
        DumpMode::EvictHot,
        dump_parallelism,
        true,
    )
    .await?;
    Ok(())
}

pub async fn checkpoint_flush_to_store<T: BucketIndexState>(
    supervisor: &MaintenanceSupervisor,
    window_states: &DashMap<Key, Arc<RwLock<T>>>,
    task_id: TaskId,
    ts_column_index: usize,
    dump_parallelism: usize,
) -> anyhow::Result<()> {
    let mut plans: Vec<DumpPlan<T>> = Vec::new();
    for entry in window_states.iter() {
        let key = entry.key().clone();
        let arc = entry.value().clone();
        let key_hash = key.hash();
        let bucket_ts_list: Vec<(Timestamp, u64)> = {
            let guard = arc
                .try_read()
                .expect("Failed to read windows state for flush planning");
            guard
                .bucket_index()
                .bucket_timestamps()
                .into_iter()
                .filter_map(|bucket_ts| {
                    guard
                        .bucket_index()
                        .bucket(bucket_ts)
                        .map(|b| (bucket_ts, b.version))
                })
                .collect()
        };
        for (bucket_ts, expected_version) in bucket_ts_list {
            plans.push(DumpPlan {
                task_id: task_id.clone(),
                key: key.clone(),
                arc: arc.clone(),
                bucket_ts,
                expected_version,
                key_hash,
            });
        }
    }

    let _ = dump_plans_concurrent(
        supervisor,
        plans,
        ts_column_index,
        DumpMode::KeepHot,
        dump_parallelism,
        false,
    )
    .await?;
    Ok(())
}
