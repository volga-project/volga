use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use dashmap::DashMap;

use crate::runtime::TaskId;
use crate::storage::index::InMemBatchId;
use crate::storage::memory_pool::{TenantType, WorkerMemoryPool};
use crate::storage::stats::StorageStats;

/// Write-buffer for in-memory `RecordBatch`es referenced by `BatchRef::InMem`.
///
/// This is intentionally store-agnostic and provides:
/// - stable ids (`InMemBatchId`)
/// - MVCC safety via `Arc<RecordBatch>` cloning
/// - basic memory accounting and a soft limit signal
#[derive(Debug)]
pub struct WriteBuffer {
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

impl WriteBuffer {
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
        self.mem_pool.tenant_budget(TenantType::WriteBuffer).soft_max_bytes
    }

    pub fn bytes(&self) -> usize {
        self.mem_pool.used_for_tenant(TenantType::WriteBuffer)
    }

    pub fn used_bytes_for_task(&self, task_id: &TaskId) -> usize {
        self.mem_pool.used(task_id, TenantType::WriteBuffer)
    }

    pub fn is_over_limit(&self, task_id: &TaskId) -> bool {
        self.mem_pool.is_above_soft(task_id, TenantType::WriteBuffer)
    }

    pub fn put(&self, task_id: TaskId, batch: RecordBatch) -> InMemBatchId {
        let id = InMemBatchId(self.next_id.fetch_add(1, Ordering::Relaxed));
        let batch = Arc::new(batch);
        let bytes = batch.get_array_memory_size();
        self.mem_pool
            .reserve_over_limit(&task_id, TenantType::WriteBuffer, bytes);
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
                    .release(&entry.task_id, TenantType::WriteBuffer, entry.bytes);
                self.stats.on_hot_remove(entry.bytes);
            }
        }
    }

    pub fn clear(&self) {
        for entry in self.cache.iter() {
            self.mem_pool
                .release(&entry.task_id, TenantType::WriteBuffer, entry.bytes);
        }
        self.cache.clear();
        self.next_id.store(1, Ordering::Relaxed);
        self.stats.on_hot_clear();
    }
}

impl Default for WriteBuffer {
    fn default() -> Self {
        let stats = StorageStats::global();
        let cfg = crate::storage::budget::StorageBackendConfig::default();
        let mem_pool = WorkerMemoryPool::new(cfg.memory);
        Self::new_with_stats(stats, mem_pool)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_batch(rows: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("__seq_no", DataType::UInt64, false),
        ]));
        let ts = Int64Array::from_iter_values(0..rows);
        let seq = UInt64Array::from_iter_values(0..(rows as u64));
        RecordBatch::try_new(schema, vec![Arc::new(ts), Arc::new(seq)]).unwrap()
    }

    #[test]
    fn put_get_remove_tracks_bytes() {
        let stats = StorageStats::global();
        let cfg = crate::storage::budget::StorageBackendConfig::default();
        let mem_pool = WorkerMemoryPool::new(cfg.memory);
        let cache = WriteBuffer::new_with_stats(stats, mem_pool);
        assert_eq!(cache.bytes(), 0);

        let id1 = cache.put(Arc::<str>::from("t"), make_batch(10));
        assert!(cache.get(id1).is_some());
        let after_1 = cache.bytes();
        assert!(after_1 > 0);

        let id2 = cache.put(Arc::<str>::from("t"), make_batch(10));
        assert!(cache.get(id2).is_some());
        let after_2 = cache.bytes();
        assert!(after_2 >= after_1);

        cache.remove(&[id1]);
        assert!(cache.get(id1).is_none());
        assert!(cache.bytes() < after_2);

        cache.remove(&[id2]);
        assert!(cache.get(id2).is_none());
        assert_eq!(cache.bytes(), 0);
    }
}
