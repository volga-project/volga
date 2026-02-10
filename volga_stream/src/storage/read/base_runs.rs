use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use dashmap::DashMap;

use crate::runtime::TaskId;
use crate::storage::index::InMemBatchId;
use crate::storage::memory_pool::{TenantType, WorkerMemoryPool};
use crate::storage::stats::StorageStats;

/// In-memory cache for persisted base runs.
#[derive(Debug)]
pub struct BaseRuns {
    cache: DashMap<InMemBatchId, CacheEntry>,
    next_id: AtomicU64,
    stats: Arc<StorageStats>,
    mem_pool: Arc<WorkerMemoryPool>,
}

#[derive(Clone, Debug)]
struct CacheEntry {
    task_id: TaskId,
    batch: Arc<RecordBatch>,
    bytes: usize,
}

impl BaseRuns {
    pub fn new(mem_pool: Arc<WorkerMemoryPool>) -> Self {
        Self {
            cache: DashMap::new(),
            next_id: AtomicU64::new(1),
            stats: StorageStats::global(),
            mem_pool,
        }
    }

    pub fn new_with_stats(stats: Arc<StorageStats>, mem_pool: Arc<WorkerMemoryPool>) -> Self {
        Self {
            cache: DashMap::new(),
            next_id: AtomicU64::new(1),
            stats,
            mem_pool,
        }
    }

    pub fn stats(&self) -> &Arc<StorageStats> {
        &self.stats
    }

    pub fn total_limit_bytes(&self) -> usize {
        self.mem_pool.total_limit_bytes()
    }

    pub fn soft_max_bytes(&self) -> usize {
        self.mem_pool.tenant_budget(TenantType::BaseRuns).soft_max_bytes
    }

    pub fn bytes(&self) -> usize {
        self.mem_pool.used_for_tenant(TenantType::BaseRuns)
    }

    pub fn used_bytes_for_task(&self, task_id: &TaskId) -> usize {
        self.mem_pool.used(task_id, TenantType::BaseRuns)
    }

    pub fn is_over_limit(&self, task_id: &TaskId) -> bool {
        self.mem_pool.is_above_soft(task_id, TenantType::BaseRuns)
    }

    pub fn put(&self, task_id: TaskId, batch: RecordBatch) -> InMemBatchId {
        let id = InMemBatchId(self.next_id.fetch_add(1, Ordering::Relaxed));
        let batch = Arc::new(batch);
        let bytes = batch.get_array_memory_size();
        self.mem_pool
            .reserve_over_limit(&task_id, TenantType::BaseRuns, bytes);
        self.cache.insert(
            id,
            CacheEntry {
                task_id,
                batch: batch.clone(),
                bytes,
            },
        );
        self.stats.on_hot_put(bytes);
        id
    }

    pub fn get(&self, id: InMemBatchId) -> Option<Arc<RecordBatch>> {
        self.cache.get(&id).map(|e| e.value().batch.clone())
    }

    pub fn get_bytes(&self, id: InMemBatchId) -> Option<usize> {
        self.cache.get(&id).map(|e| e.value().bytes)
    }

    pub fn remove(&self, ids: &[InMemBatchId]) {
        for id in ids {
            if let Some((_, entry)) = self.cache.remove(id) {
                self.mem_pool
                    .release(&entry.task_id, TenantType::BaseRuns, entry.bytes);
                self.stats.on_hot_remove(entry.bytes);
            }
        }
    }

    pub fn clear(&self) {
        for entry in self.cache.iter() {
            self.mem_pool
                .release(&entry.task_id, TenantType::BaseRuns, entry.bytes);
        }
        self.cache.clear();
        self.next_id.store(1, Ordering::Relaxed);
        self.stats.on_hot_clear();
    }
}
