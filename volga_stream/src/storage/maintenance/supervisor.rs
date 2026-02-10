use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::common::Key;
use crate::runtime::TaskId;
use crate::storage::backend::DynStorageBackend;
use crate::storage::batch::Timestamp;
use crate::storage::delete::batch_pins::BatchPins;
use crate::storage::delete::batch_retirement::BatchRetirementQueue;
use crate::storage::StorageStats;
use crate::storage::WriteBuffer;

/// Supervisor for storage maintenance tasks (dump/compact/rehydrate/pressure).
#[derive(Debug)]
pub struct MaintenanceSupervisor {
    pub(crate) backend: DynStorageBackend,
    pub(crate) batch_pins: Arc<BatchPins>,
    pub(crate) retired: Arc<BatchRetirementQueue>,
    pub(crate) write_buffer: Arc<WriteBuffer>,
    pub(crate) stats: Arc<StorageStats>,
    // Single-flight locks for per-bucket operations.
    pub(crate) bucket_locks: DashMap<(TaskId, u64, Timestamp), Arc<Mutex<()>>>,
    // (task_id, key_hash, bucket_ts) -> last dumped bucket.version
    pub(crate) last_dumped_version: DashMap<(TaskId, u64, Timestamp), u64>,
    // (task_id, key) -> last access counter (LRU).
    pub(crate) key_access: DashMap<(TaskId, Key), u64>,
    pub(crate) access_clock: Arc<AtomicU64>,
    pub(crate) task_handles: Arc<DashMap<TaskId, TaskHandles>>,
}

#[derive(Debug, Default)]
pub struct TaskHandles {
    pub deleter: Option<JoinHandle<()>>,
    pub compaction: Option<JoinHandle<()>>,
    pub dump: Option<JoinHandle<()>>,
}

impl Clone for MaintenanceSupervisor {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            batch_pins: self.batch_pins.clone(),
            retired: self.retired.clone(),
            write_buffer: self.write_buffer.clone(),
            stats: self.stats.clone(),
            bucket_locks: self.bucket_locks.clone(),
            last_dumped_version: self.last_dumped_version.clone(),
            key_access: self.key_access.clone(),
            access_clock: self.access_clock.clone(),
            task_handles: self.task_handles.clone(),
        }
    }
}

impl MaintenanceSupervisor {
    pub fn new(
        backend: DynStorageBackend,
        batch_pins: Arc<BatchPins>,
        retired: Arc<BatchRetirementQueue>,
        write_buffer: Arc<WriteBuffer>,
    ) -> Self {
        Self::new_with_stats(
            backend,
            batch_pins,
            retired,
            write_buffer,
            StorageStats::global(),
        )
    }

    pub fn new_with_stats(
        backend: DynStorageBackend,
        batch_pins: Arc<BatchPins>,
        retired: Arc<BatchRetirementQueue>,
        write_buffer: Arc<WriteBuffer>,
        stats: Arc<StorageStats>,
    ) -> Self {
        Self {
            backend,
            batch_pins,
            retired,
            write_buffer,
            stats,
            bucket_locks: DashMap::new(),
            last_dumped_version: DashMap::new(),
            key_access: DashMap::new(),
            access_clock: Arc::new(AtomicU64::new(1)),
            task_handles: Arc::new(DashMap::new()),
        }
    }

    pub fn write_buffer(&self) -> &Arc<WriteBuffer> {
        &self.write_buffer
    }

    pub fn clear_dump_state(&self) {
        self.last_dumped_version.clear();
    }

    pub fn record_key_access(&self, task_id: TaskId, key: &Key) -> u64 {
        let now = self.access_clock.fetch_add(1, Ordering::Relaxed);
        self.key_access.insert((task_id, key.clone()), now);
        now
    }

    pub fn key_last_access(&self, task_id: &TaskId, key: &Key) -> u64 {
        self.key_access
            .get(&(task_id.clone(), key.clone()))
            .map(|e| *e.value())
            .unwrap_or(0)
    }

    pub fn register_task_handles(
        &self,
        task_id: TaskId,
        handles: TaskHandles,
    ) -> Option<TaskHandles> {
        if self.task_handles.contains_key(&task_id) {
            return Some(handles);
        }
        self.task_handles.insert(task_id, handles);
        None
    }

    pub async fn stop_task_handles(&self, task_id: &TaskId) {
        let handles = match self.task_handles.remove(task_id) {
            Some((_, handles)) => handles,
            None => return,
        };
        if let Some(h) = handles.compaction {
            h.abort();
            let _ = h.await;
        }
        if let Some(h) = handles.dump {
            h.abort();
            let _ = h.await;
        }
        if let Some(h) = handles.deleter {
            h.abort();
            let _ = h.await;
        }
    }

    pub(crate) fn get_bucket_lock(
        &self,
        task_id: &TaskId,
        key_hash: u64,
        bucket_ts: Timestamp,
    ) -> Arc<Mutex<()>> {
        let k = (task_id.clone(), key_hash, bucket_ts);
        if let Some(existing) = self.bucket_locks.get(&k) {
            return existing.value().clone();
        }
        let lock = Arc::new(Mutex::new(()));
        self.bucket_locks.insert(k, lock.clone());
        lock
    }
}
