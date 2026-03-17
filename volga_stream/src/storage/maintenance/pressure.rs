use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::RwLock;

use crate::common::Key;
use crate::runtime::TaskId;
use crate::storage::batch::Timestamp;
use crate::storage::index::InMemBatchId;

use super::dump_plans_concurrent;
use super::types::{BucketIndexState, DumpMode, DumpPlan};
use crate::storage::maintenance::MaintenanceSupervisor;

pub async fn relieve_in_mem_pressure<T: BucketIndexState>(
    supervisor: &MaintenanceSupervisor,
    window_states: &DashMap<Key, Arc<RwLock<T>>>,
    task_id: TaskId,
    ts_column_index: usize,
    hot_bucket_count: usize,
    low_watermark_per_mille: u32,
    parallelism: usize,
) -> anyhow::Result<usize> {
    let limit = supervisor.write_buffer.soft_max_bytes();
    if limit == 0 {
        return Ok(0);
    }
    let current = supervisor.write_buffer.used_bytes_for_task(&task_id);
    if current <= limit {
        return Ok(0);
    }

    let low = (limit.saturating_mul(low_watermark_per_mille as usize)) / 1000;
    let target = low.min(limit);
    let need_free = current.saturating_sub(target);
    if need_free == 0 {
        return Ok(0);
    }

    // TODO: switch to per-key eviction planning (evict all hot batches/state for a key).
    #[derive(Clone)]
    struct Candidate<T: BucketIndexState> {
        key: Key,
        arc: Arc<RwLock<T>>,
        bucket_ts: Timestamp,
        expected_version: u64,
        key_hash: u64,
        is_hot: bool,
        inmem_ids: Vec<InMemBatchId>,
        last_access: u64,
    }

    let mut cands: Vec<Candidate<T>> = Vec::new();
    for entry in window_states.iter() {
        let key = entry.key().clone();
        let arc = entry.value().clone();
        let key_hash = key.hash();
        let guard = match arc.try_read() {
            Ok(g) => g,
            Err(_) => continue,
        };
        let idx = guard.bucket_index();
        let bucket_ts_list = idx.bucket_timestamps(); // ascending
        let len = bucket_ts_list.len();
        if len == 0 {
            continue;
        }
        let last_access = supervisor.key_last_access(&task_id, &key);
        let hot_from = len.saturating_sub(hot_bucket_count);
        for (i, bucket_ts) in bucket_ts_list.into_iter().enumerate() {
            let Some(bucket) = idx.bucket(bucket_ts) else {
                continue;
            };
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
            let lock = supervisor.get_bucket_lock(&task_id, key_hash, bucket_ts);
            if lock.try_lock().is_err() {
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
                last_access,
            });
        }
    }

    cands.sort_by_key(|c| (c.is_hot, c.last_access, c.bucket_ts));

    use std::collections::HashSet;
    let mut selected_plans: Vec<DumpPlan<T>> = Vec::new();
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
            if let Some(sz) = supervisor.write_buffer.get_bytes(*id) {
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

    supervisor
        .stats
        .on_pressure_relief_start(selected_plans.len());
    dump_plans_concurrent(
        supervisor,
        selected_plans,
        ts_column_index,
        DumpMode::EvictHot,
        parallelism,
        true,
    )
    .await
    .map(|n| {
        supervisor.stats.on_pressure_relief_dumped(n);
        n
    })
}
