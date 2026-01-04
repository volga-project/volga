use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use dashmap::DashMap;

use crate::runtime::operators::window::state::index::InMemBatchId;

/// Cache for in-memory `RecordBatch`es referenced by `BatchRef::InMem`.
///
/// This is intentionally store-agnostic and provides:
/// - stable ids (`InMemBatchId`)
/// - MVCC safety via `Arc<RecordBatch>` cloning
/// - basic memory accounting and a soft limit signal
#[derive(Debug)]
pub struct InMemBatchCache {
    batches: DashMap<InMemBatchId, Arc<RecordBatch>>,
    next_id: AtomicU64,
    bytes: AtomicUsize,
    limit_bytes: AtomicUsize, // 0 = unlimited
}

impl InMemBatchCache {
    pub fn new() -> Self {
        Self {
            batches: DashMap::new(),
            next_id: AtomicU64::new(1),
            bytes: AtomicUsize::new(0),
            limit_bytes: AtomicUsize::new(0),
        }
    }

    pub fn set_limit_bytes(&self, limit_bytes: usize) {
        self.limit_bytes.store(limit_bytes, Ordering::Relaxed);
    }

    pub fn limit_bytes(&self) -> usize {
        self.limit_bytes.load(Ordering::Relaxed)
    }

    pub fn bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    pub fn is_over_limit(&self) -> bool {
        let limit = self.limit_bytes();
        limit != 0 && self.bytes() > limit
    }

    // TODO we should backpressure and dump to store if over limit
    pub fn put(&self, batch: RecordBatch) -> InMemBatchId {
        let id = InMemBatchId(self.next_id.fetch_add(1, Ordering::Relaxed));
        let arc = Arc::new(batch);
        let sz = arc.get_array_memory_size();
        self.batches.insert(id, arc);
        self.bytes.fetch_add(sz, Ordering::Relaxed);
        id
    }

    pub fn get(&self, id: InMemBatchId) -> Option<Arc<RecordBatch>> {
        self.batches.get(&id).map(|e| e.value().clone())
    }

    pub fn remove(&self, ids: &[InMemBatchId]) {
        for id in ids {
            if let Some((_, b)) = self.batches.remove(id) {
                let sz = b.get_array_memory_size();
                self.bytes.fetch_sub(sz, Ordering::Relaxed);
            }
        }
    }

    pub fn clear(&self) {
        // Recompute bytes after clear to avoid underflow issues on races.
        self.batches.clear();
        self.bytes.store(0, Ordering::Relaxed);
        self.next_id.store(1, Ordering::Relaxed);
    }
}

impl Default for InMemBatchCache {
    fn default() -> Self {
        Self::new()
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
        let cache = InMemBatchCache::new();
        assert_eq!(cache.bytes(), 0);

        let id1 = cache.put(make_batch(10));
        assert!(cache.get(id1).is_some());
        let after_1 = cache.bytes();
        assert!(after_1 > 0);

        let id2 = cache.put(make_batch(10));
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

    #[test]
    fn over_limit_signal() {
        let cache = InMemBatchCache::new();
        cache.set_limit_bytes(1);
        let _id = cache.put(make_batch(10));
        assert!(cache.is_over_limit());
    }
}



