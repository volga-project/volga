use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use std::time::Instant;
use tokio::sync::{Mutex, RwLock};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::common::Key;
use crate::runtime::operators::window::window_operator_state::WindowsState;
use crate::storage::batch_pins::BatchPins;
use crate::storage::batch_retirement::BatchRetirementQueue;
use crate::storage::batch_store::{BatchId, BatchStore, Timestamp};
use crate::storage::compactor::low_level::compact_to_disjoint_segments;
use crate::storage::InMemBatchCache;
use crate::storage::StorageStats;
use crate::runtime::TaskId;

use crate::runtime::operators::window::{Cursor, SEQ_NO_COLUMN_NAME};
use crate::storage::index::bucket_index::BatchMeta;
use crate::storage::index::bucket_index::BatchRef;
use crate::storage::index::InMemBatchId;
use arrow::array::{TimestampMillisecondArray, UInt64Array};
use arrow::record_batch::RecordBatch;

fn debug_assert_metas_disjoint_and_ordered(metas: &[BatchMeta]) {
    if !cfg!(debug_assertions) {
        return;
    }
    if metas.len() <= 1 {
        return;
    }
    for w in metas.windows(2) {
        assert!(
            w[0].min_pos <= w[0].max_pos,
            "invalid meta bounds: min_pos={:?} max_pos={:?}",
            w[0].min_pos,
            w[0].max_pos
        );
        assert!(
            w[1].min_pos <= w[1].max_pos,
            "invalid meta bounds: min_pos={:?} max_pos={:?}",
            w[1].min_pos,
            w[1].max_pos
        );
        assert!(
            w[0].min_pos <= w[1].min_pos,
            "metas must be ordered by min_pos: prev_min={:?} next_min={:?}",
            w[0].min_pos,
            w[1].min_pos
        );
        assert!(
            w[0].max_pos < w[1].min_pos,
            "metas must be disjoint: prev=[{:?}..{:?}] next=[{:?}..{:?}]",
            w[0].min_pos,
            w[0].max_pos,
            w[1].min_pos,
            w[1].max_pos
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DumpMode {
    KeepHot,
    EvictHot,
}

#[derive(Clone)]
struct DumpPlan {
    task_id: TaskId,
    key: Key,
    arc: Arc<RwLock<WindowsState>>,
    bucket_ts: Timestamp,
    expected_version: u64,
    key_hash: u64,
}

/// High-level compaction/dump orchestrator.
///
/// This is a control layer over `BatchStore` + `InMemBatchCache`.
/// Background task wiring will be added as we move dump/compaction policy fully out of the operator.
#[derive(Debug, Clone)]
pub struct Compactor {
    batch_store: Arc<dyn BatchStore>,
    batch_pins: Arc<BatchPins>,
    retired: Arc<BatchRetirementQueue>,
    in_mem: Arc<InMemBatchCache>,
    stats: Arc<StorageStats>,
    // Single-flight locks for per-bucket compaction/dump.
    bucket_locks: DashMap<(TaskId, u64, Timestamp), Arc<Mutex<()>>>,
    // Used by periodic dump policy to avoid rewriting store for buckets that haven't changed.
    // Keyed by (key_hash, bucket_ts) -> last dumped bucket.version.
    last_dumped_version: DashMap<(TaskId, u64, Timestamp), u64>,
}

impl Compactor {
    pub fn new(
        batch_store: Arc<dyn BatchStore>,
        batch_pins: Arc<BatchPins>,
        retired: Arc<BatchRetirementQueue>,
        in_mem: Arc<InMemBatchCache>,
    ) -> Self {
        Self {
            batch_store,
            batch_pins,
            retired,
            in_mem,
            stats: StorageStats::global(),
            bucket_locks: DashMap::new(),
            last_dumped_version: DashMap::new(),
        }
    }

    pub fn batch_store(&self) -> &Arc<dyn BatchStore> {
        &self.batch_store
    }

    pub fn in_mem(&self) -> &Arc<InMemBatchCache> {
        &self.in_mem
    }

    pub fn clear_dump_state(&self) {
        self.last_dumped_version.clear();
    }

    /// Spawn periodic background tasks:
    /// - compaction into hot in-mem disjoint segments (CPU)
    /// - dumping buckets to the store (IO)
    ///
    /// Eager paths still exist (on-read compaction / checkpoint dump).
    pub fn spawn_background_tasks(
        self: Arc<Self>,
        window_states: Arc<DashMap<Key, Arc<RwLock<WindowsState>>>>,
        task_id: TaskId,
        ts_column_index: usize,
        hot_bucket_count: usize,
        dump_parallelism: usize,
        low_watermark_per_mille: u32,
        compaction_interval: Duration,
        dump_interval: Duration,
    ) -> (JoinHandle<()>, JoinHandle<()>) {
        let compactor_for_compact = self.clone();
        let ws_for_compact = window_states.clone();
        let task_id_for_compact = task_id.clone();
        let compact_handle = tokio::spawn(async move {
            let mut tick = tokio::time::interval(compaction_interval);
            loop {
                tick.tick().await;
                let _ = compactor_for_compact
                    .periodic_compact_in_mem(
                        &ws_for_compact,
                        task_id_for_compact.clone(),
                        ts_column_index,
                        hot_bucket_count,
                    )
                    .await;

                if compactor_for_compact.in_mem.is_over_limit() {
                    let _ = compactor_for_compact
                        .relieve_in_mem_pressure(
                            &ws_for_compact,
                            task_id_for_compact.clone(),
                            ts_column_index,
                            hot_bucket_count,
                            low_watermark_per_mille,
                            dump_parallelism,
                        )
                        .await;
                }
            }
        });

        let compactor_for_dump = self.clone();
        let ws_for_dump = window_states.clone();
        let task_id_for_dump = task_id.clone();
        let dump_handle = tokio::spawn(async move {
            let mut tick = tokio::time::interval(dump_interval);
            loop {
                tick.tick().await;
                let _ = compactor_for_dump
                    .periodic_dump_to_store(
                        &ws_for_dump,
                        task_id_for_dump.clone(),
                        ts_column_index,
                        hot_bucket_count,
                        dump_parallelism,
                    )
                    .await;
            }
        });

        (compact_handle, dump_handle)
    }

    async fn dump_plans_concurrent(
        &self,
        plans: Vec<DumpPlan>,
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
            let this = self.clone();
            futs.push(async move {
                let _p = permit.acquire_owned().await.expect("semaphore");

                let lock = this.get_bucket_lock(&plan.task_id, plan.key_hash, plan.bucket_ts);
                let _guard = lock.lock().await;

                let should_dump = {
                    let state = plan.arc.read().await;
                    match state.bucket_index.bucket(plan.bucket_ts) {
                        None => false,
                        Some(bucket) => bucket.version == plan.expected_version,
                    }
                };
                if !should_dump {
                    return Ok::<bool, anyhow::Error>(false);
                }

                let published = this
                    .dump_bucket(
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

    fn get_bucket_lock(&self, task_id: &TaskId, key_hash: u64, bucket_ts: Timestamp) -> Arc<Mutex<()>> {
        let k = (task_id.clone(), key_hash, bucket_ts);
        if let Some(existing) = self.bucket_locks.get(&k) {
            return existing.value().clone();
        }
        let lock = Arc::new(Mutex::new(()));
        self.bucket_locks.insert(k, lock.clone());
        lock
    }

    pub async fn rehydrate_bucket(
        &self,
        key: &Key,
        arc: &Arc<RwLock<WindowsState>>,
        task_id: TaskId,
        bucket_ts: Timestamp,
    ) {
        let lock = self.get_bucket_lock(&task_id, key.hash(), bucket_ts);
        let _guard = lock.lock().await;
        let _ = self
            ._rehydrate_bucket(key, arc, task_id, bucket_ts)
            .await;
    }

    async fn _rehydrate_bucket(
        &self,
        key: &Key,
        arc: &Arc<RwLock<WindowsState>>,
        task_id: TaskId,
        bucket_ts: Timestamp,
    ) -> anyhow::Result<bool> {
        let (expected_version, persisted) = {
            let state = arc.read().await;
            let Some(bucket) = state.bucket_index.bucket(bucket_ts) else {
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

        let _lease = self.batch_pins.clone().pin(task_id.clone(), stored_ids.clone());
        let mut loaded: std::collections::HashMap<BatchId, RecordBatch> = std::collections::HashMap::new();
        for id in stored_ids {
            if let Some(b) = self.batch_store.load_batch(task_id.clone(), id, key).await {
                loaded.insert(id, b);
            }
        }

        let mut new_inmem_ids: Vec<InMemBatchId> = Vec::new();
        let mut new_metas: Vec<BatchMeta> = Vec::new();

        for meta in &persisted {
            let Some(stored_id) = meta.run.stored_batch_id() else { continue; };
            let Some(batch) = loaded.get(&stored_id) else { continue; };
            let inmem_id = self.in_mem.put(batch.clone());
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
                self.in_mem.remove(&new_inmem_ids);
            }
            return Ok(false);
        }

        let published = {
            let mut state = arc.write().await;
            match state.bucket_index.bucket_mut(bucket_ts) {
                None => false,
                Some(bucket)
                    if bucket.version == expected_version && bucket.hot_base_segments.is_empty() =>
                {
                    debug_assert_metas_disjoint_and_ordered(&new_metas);
                    bucket.hot_base_segments = new_metas;
                    true
                }
                Some(_) => false,
            }
        };

        if !published {
            self.in_mem.remove(&new_inmem_ids);
            return Ok(false);
        }

        Ok(true)
    }

    pub async fn compact_bucket(
        &self,
        key: &Key,
        arc: &Arc<RwLock<WindowsState>>,
        task_id: TaskId,
        bucket_ts: Timestamp,
        ts_column_index: usize,
    ) {
        let lock = self.get_bucket_lock(&task_id, key.hash(), bucket_ts);
        let _guard = lock.lock().await;
        let _ = self
            ._compact_bucket(key, arc, task_id, bucket_ts, ts_column_index)
            .await;
    }

    async fn _compact_bucket(
        &self,
        key: &Key,
        arc: &Arc<RwLock<WindowsState>>,
        task_id: TaskId,
        bucket_ts: Timestamp,
        ts_column_index: usize,
    ) -> anyhow::Result<bool> {
        let t0 = Instant::now();
        let (expected_version, run_refs, old_inmem, old_stored) = {
            let state = arc.read().await;
            let Some(bucket) = state.bucket_index.bucket(bucket_ts) else {
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
            self.stats.on_compact_finish(false, t0.elapsed().as_nanos() as u64);
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
                if let Some(b) = self.batch_store.load_batch(task_id.clone(), id, key).await {
                    input_batches.push(b);
                }
            }
        }
        for r in &run_refs {
            if let Some(id) = r.inmem_id() {
                if let Some(b) = self.in_mem.get(id) {
                    input_batches.push((*b).clone());
                }
            }
        }
        if input_batches.is_empty() {
            self.stats.on_compact_finish(false, t0.elapsed().as_nanos() as u64);
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
            self.batch_store.max_batch_size(),
        );
        if segments.is_empty() {
            self.stats.on_compact_finish(false, t0.elapsed().as_nanos() as u64);
            return Ok(false);
        }

        let mut new_metas: Vec<BatchMeta> = Vec::new();
        let mut new_inmem_ids: Vec<InMemBatchId> = Vec::new();

        for seg in segments {
            if seg.num_rows() == 0 {
                continue;
            }
            let inmem_id = self.in_mem.put(seg.clone());
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
            self.in_mem.remove(&new_inmem_ids);
            self.stats.on_compact_finish(false, t0.elapsed().as_nanos() as u64);
            return Ok(false);
        }

        let published = {
            let mut state = arc.write().await;
            match state.bucket_index.bucket_mut(bucket_ts) {
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
            self.in_mem.remove(&new_inmem_ids);
            self.stats.on_compact_finish(false, t0.elapsed().as_nanos() as u64);
            return Ok(false);
        }

        if !old_inmem.is_empty() {
            self.in_mem.remove(&old_inmem);
        }

        self.stats.on_compact_finish(true, t0.elapsed().as_nanos() as u64);
        Ok(true)
    }

    async fn dump_bucket(
        &self,
        key: &Key,
        arc: &Arc<RwLock<WindowsState>>,
        task_id: TaskId,
        bucket_ts: Timestamp,
        ts_column_index: usize,
        mode: DumpMode,
    ) -> anyhow::Result<bool> {
        let t0 = Instant::now();
        self.stats.on_dump_start();
        let key_hash = key.hash();

        let (expected_version, run_refs, old_inmem, prev_persisted, pending_stored) = {
            let state = arc.read().await;
            let Some(bucket) = state.bucket_index.bucket(bucket_ts) else {
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
                } else if let Some(_id) = m.run.stored_batch_id() {
                    // Stored input is part of previous persisted layout.
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
                if let Some(b) = self.batch_store.load_batch(task_id.clone(), id, key).await {
                    input_batches.push(b);
                }
            }
        }
        for r in &run_refs {
            if let Some(id) = r.inmem_id() {
                if let Some(b) = self.in_mem.get(id) {
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
            self.batch_store.max_batch_size(),
        );
        if segments.is_empty() {
            self.stats
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
            self.batch_store
                .put_batch_with_id(task_id.clone(), new_id, seg.clone(), key)
                .await;
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
            self.retired.retire(task_id.clone(), key, &new_stored_ids);
            self.stats
                .on_dump_finish(false, t0.elapsed().as_nanos() as u64);
            return Ok(false);
        }

        self.stats.on_dump_written(written_segments, written_bytes);

        let published = {
            let mut state = arc.write().await;
            match state.bucket_index.bucket_mut(bucket_ts) {
                None => false,
                Some(bucket) if bucket.version == expected_version => {
                    debug_assert_metas_disjoint_and_ordered(&new_metas);
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
            self.retired.retire(task_id.clone(), key, &new_stored_ids);
            self.stats
                .on_dump_finish(false, t0.elapsed().as_nanos() as u64);
            return Ok(false);
        }

        if !prev_persisted.is_empty() || !pending_stored.is_empty() {
            use std::collections::HashSet;
            let mut all: HashSet<BatchId> = HashSet::new();
            all.extend(prev_persisted);
            all.extend(pending_stored);
            let all_vec: Vec<BatchId> = all.into_iter().collect();
            self.retired.retire(task_id.clone(), key, &all_vec);
        }
        if mode == DumpMode::EvictHot && !old_inmem.is_empty() {
            self.in_mem.remove(&old_inmem);
        }

        self.stats
            .on_dump_finish(true, t0.elapsed().as_nanos() as u64);
        Ok(true)
    }

    pub async fn periodic_dump_to_store(
        &self,
        window_states: &DashMap<Key, Arc<RwLock<WindowsState>>>,
        task_id: TaskId,
        ts_column_index: usize,
        hot_bucket_count: usize,
        dump_parallelism: usize,
    ) -> anyhow::Result<()> {
        let mut plans: Vec<DumpPlan> = Vec::new();
        for entry in window_states.iter() {
            let key = entry.key().clone();
            let arc = entry.value().clone();
            let key_hash = key.hash();

            let to_dump: Vec<(Timestamp, u64)> = {
                let guard = arc
                    .try_read()
                    .expect("Failed to read windows state for periodic dump planning");
                let idx = &guard.bucket_index;
                let bucket_ts_list = idx.bucket_timestamps(); // ascending
                if bucket_ts_list.is_empty() {
                    Vec::new()
                } else {
                    let cold_len = bucket_ts_list.len().saturating_sub(hot_bucket_count);
                    let mut out: Vec<(Timestamp, u64)> = Vec::new();
                    for &bucket_ts in bucket_ts_list.iter().take(cold_len) {
                        let Some(bucket) = idx.bucket(bucket_ts) else { continue; };
                        let mut has_inmem = false;
                        for m in bucket
                            .hot_base_segments
                            .iter()
                            .chain(bucket.hot_deltas.iter())
                        {
                            if m.run.inmem_id().is_some() {
                                has_inmem = true;
                                break;
                            }
                        }
                        if !has_inmem {
                            continue;
                        }
                        let last_dumped = self
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

        let _ = self
            .dump_plans_concurrent(
                plans,
                ts_column_index,
                DumpMode::EvictHot,
                dump_parallelism,
                true,
            )
            .await?;
        Ok(())
    }

    /// Periodically compact buckets in memory (no store writes).
    ///
    /// This is the same compaction used by on-read compaction, but scheduled in the background
    /// to keep read-path work small and bounded.
    pub async fn periodic_compact_in_mem(
        &self,
        window_states: &DashMap<Key, Arc<RwLock<WindowsState>>>,
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
                let idx = &guard.bucket_index;
                let bucket_ts_list = idx.bucket_timestamps(); // ascending
                if bucket_ts_list.is_empty() {
                    Vec::new()
                } else {
                    let cutoff = bucket_ts_list
                        .len()
                        .saturating_sub(hot_bucket_count);
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
                self.compact_bucket(&key, &arc, task_id.clone(), bucket_ts, ts_column_index)
                    .await;
            }
        }
        Ok(())
    }

    pub async fn checkpoint_flush_to_store(
        &self,
        window_states: &DashMap<Key, Arc<RwLock<WindowsState>>>,
        task_id: TaskId,
        ts_column_index: usize,
        dump_parallelism: usize,
    ) -> anyhow::Result<()> {
        let mut plans: Vec<DumpPlan> = Vec::new();
        for entry in window_states.iter() {
            let key = entry.key().clone();
            let arc = entry.value().clone();
            let key_hash = key.hash();
            let bucket_ts_list: Vec<(Timestamp, u64)> = {
                let guard = arc
                    .try_read()
                    .expect("Failed to read windows state for flush planning");
                guard
                    .bucket_index
                    .bucket_timestamps()
                    .into_iter()
                    .filter_map(|bucket_ts| {
                        guard
                            .bucket_index
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

        let _ = self
            .dump_plans_concurrent(
                plans,
                ts_column_index,
                DumpMode::KeepHot,
                dump_parallelism,
                false,
            )
            .await?;
        Ok(())
    }

    /// Best-effort memory pressure relief:
    /// - plan exact buckets to dump (coldest first), based on estimated in-mem bytes
    /// - dump them concurrently (bounded) using the same dumper path as periodic dump
    pub async fn relieve_in_mem_pressure(
        &self,
        window_states: &DashMap<Key, Arc<RwLock<WindowsState>>>,
        task_id: TaskId,
        ts_column_index: usize,
        hot_bucket_count: usize,
        low_watermark_per_mille: u32,
        parallelism: usize,
    ) -> anyhow::Result<usize> {
        let limit = self.in_mem.limit_bytes();
        if limit == 0 {
            return Ok(0);
        }
        let current = self.in_mem.bytes();
        if current <= limit {
            return Ok(0);
        }

        let low = (limit.saturating_mul(low_watermark_per_mille as usize)) / 1000;
        let target = low.min(limit);
        let need_free = current.saturating_sub(target);
        if need_free == 0 {
            return Ok(0);
        }

        #[derive(Clone)]
        struct Candidate {
            key: Key,
            arc: Arc<RwLock<WindowsState>>,
            bucket_ts: Timestamp,
            expected_version: u64,
            key_hash: u64,
            is_hot: bool,
            inmem_ids: Vec<InMemBatchId>,
        }

        let mut cands: Vec<Candidate> = Vec::new();
        for entry in window_states.iter() {
            let key = entry.key().clone();
            let arc = entry.value().clone();
            let key_hash = key.hash();
            let guard = match arc.try_read() {
                Ok(g) => g,
                Err(_) => continue,
            };
            let idx = &guard.bucket_index;
            let bucket_ts_list = idx.bucket_timestamps(); // ascending
            let len = bucket_ts_list.len();
            if len == 0 {
                continue;
            }
            let hot_from = len.saturating_sub(hot_bucket_count);
            for (i, bucket_ts) in bucket_ts_list.into_iter().enumerate() {
                let Some(bucket) = idx.bucket(bucket_ts) else { continue; };
                let mut ids: Vec<InMemBatchId> = Vec::new();
                for m in bucket
                    .hot_base_segments
                    .iter()
                    .chain(bucket.hot_deltas.iter())
                {
                    if let Some(id) = m.run.inmem_id() {
                        ids.push(id);
                    }
                }
                if ids.is_empty() {
                    continue;
                }
                cands.push(Candidate {
                    key: key.clone(),
                    arc: arc.clone(),
                    bucket_ts,
                    expected_version: bucket.version,
                    key_hash,
                    is_hot: i >= hot_from,
                    inmem_ids: ids,
                });
            }
        }

        // Coldest first, then oldest within class.
        cands.sort_by_key(|c| (c.is_hot, c.bucket_ts));

        use std::collections::HashSet;
        let mut selected_plans: Vec<DumpPlan> = Vec::new();
        let mut selected_ids: HashSet<InMemBatchId> = HashSet::new();
        let mut freed_estimate: usize = 0;

        for c in cands {
            if freed_estimate >= need_free {
                break;
            }
            let mut add: usize = 0;
            for id in &c.inmem_ids {
                if selected_ids.contains(id) {
                    continue;
                }
                if let Some(sz) = self.in_mem.get_bytes(*id) {
                    add = add.saturating_add(sz);
                    selected_ids.insert(*id);
                }
            }
            if add == 0 {
                continue;
            }
            freed_estimate = freed_estimate.saturating_add(add);
            selected_plans.push(DumpPlan {
                task_id: task_id.clone(),
                key: c.key,
                arc: c.arc,
                bucket_ts: c.bucket_ts,
                expected_version: c.expected_version,
                key_hash: c.key_hash,
            });
        }

        if selected_plans.is_empty() {
            return Ok(0);
        }

        self.stats.on_pressure_relief_start(selected_plans.len());
        self.dump_plans_concurrent(
            selected_plans,
            ts_column_index,
            DumpMode::EvictHot,
            parallelism,
            true,
        )
        .await
        .map(|n| {
            self.stats.on_pressure_relief_dumped(n);
            n
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use arrow::array::StringArray;
    use arrow::array::TimestampMillisecondArray;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use crate::runtime::operators::window::BucketIndex;
    use crate::runtime::operators::window::TimeGranularity;
    use crate::storage::batch_store::InMemBatchStore;

    fn make_batch(ts: &[i64], seq: &[u64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("__seq_no", DataType::UInt64, false),
        ]));
        let ts_arr = TimestampMillisecondArray::from(ts.to_vec());
        let seq_arr = UInt64Array::from(seq.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(ts_arr), Arc::new(seq_arr)]).unwrap()
    }

    fn make_key(s: &str) -> Key {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
        let arr = StringArray::from(vec![s]);
        let rb = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        Key::new(rb).unwrap()
    }

    #[tokio::test]
    async fn compact_on_read_publishes_new_in_mem_and_drops_old_ids() {
        let batch_store: Arc<dyn BatchStore> =
            Arc::new(InMemBatchStore::new(8, TimeGranularity::Seconds(1), 1024));
        let in_mem = Arc::new(InMemBatchCache::new());
        let pins = Arc::new(crate::storage::batch_pins::BatchPins::new());
        let retired = Arc::new(crate::storage::batch_retirement::BatchRetirementQueue::new());
        let compactor = Compactor::new(batch_store, pins, retired, in_mem.clone());

        let key = make_key("k");
        let mut ws = WindowsState {
            window_states: HashMap::new(),
            bucket_index: BucketIndex::new(TimeGranularity::Seconds(1)),
            next_seq_no: 0,
        };

        let bucket_ts: Timestamp = 0;
        let b1 = make_batch(&[0, 1, 2], &[0, 1, 2]);
        let b2 = make_batch(&[2, 3, 4], &[3, 4, 5]); // overlaps at ts=2 -> forces merge
        let old_id1 = in_mem.put(b1.clone());
        let old_id2 = in_mem.put(b2.clone());
        ws.bucket_index.insert_batch_ref(
            bucket_ts,
            BatchRef::InMem(old_id1),
            Cursor::new(0, 0),
            Cursor::new(2, 2),
            b1.num_rows(),
            b1.get_array_memory_size(),
        );
        ws.bucket_index.insert_batch_ref(
            bucket_ts,
            BatchRef::InMem(old_id2),
            Cursor::new(2, 3),
            Cursor::new(4, 5),
            b2.num_rows(),
            b2.get_array_memory_size(),
        );

        let arc = Arc::new(RwLock::new(ws));
        compactor
            .compact_bucket(&key, &arc, Arc::<str>::from("t"), bucket_ts, 0)
            .await;

        assert!(in_mem.get(old_id1).is_none());
        assert!(in_mem.get(old_id2).is_none());

        let state = arc.read().await;
        let bucket = state.bucket_index.bucket(bucket_ts).unwrap();
        assert!(bucket.hot_deltas.is_empty());
        assert!(!bucket.hot_base_segments.is_empty());
    }

    #[tokio::test]
    async fn checkpoint_dump_persists_without_eviction() {
        let batch_store: Arc<dyn BatchStore> =
            Arc::new(InMemBatchStore::new(8, TimeGranularity::Seconds(1), 1024));
        let in_mem = Arc::new(InMemBatchCache::new());
        let pins = Arc::new(crate::storage::batch_pins::BatchPins::new());
        let retired = Arc::new(crate::storage::batch_retirement::BatchRetirementQueue::new());
        let compactor = Compactor::new(batch_store.clone(), pins, retired, in_mem.clone());

        let key = make_key("k");
        let mut ws = WindowsState {
            window_states: HashMap::new(),
            bucket_index: BucketIndex::new(TimeGranularity::Seconds(1)),
            next_seq_no: 0,
        };

        let bucket_ts: Timestamp = 0;
        let b = make_batch(&[0, 1, 2], &[0, 1, 2]);
        let id = in_mem.put(b.clone());
        ws.bucket_index.insert_batch_ref(
            bucket_ts,
            BatchRef::InMem(id),
            Cursor::new(0, 0),
            Cursor::new(2, 2),
            b.num_rows(),
            b.get_array_memory_size(),
        );

        let arc = Arc::new(RwLock::new(ws));
        let published = compactor
            .dump_bucket(
                &key,
                &arc,
                Arc::<str>::from("t"),
                bucket_ts,
                0,
                DumpMode::KeepHot,
            )
            .await
            .unwrap();
        assert!(published);

        let state = arc.read().await;
        let bucket = state.bucket_index.bucket(bucket_ts).unwrap();
        // Hot layout is kept.
        assert!(bucket.persisted_segments.len() >= 1);
        assert!(!bucket.hot_deltas.is_empty());
        // Persisted segments are stored.
        assert!(bucket
            .persisted_segments
            .iter()
            .all(|m| m.run.stored_batch_id().is_some()));
        // Original in-mem batch remains.
        assert!(in_mem.get(id).is_some());
    }
}






