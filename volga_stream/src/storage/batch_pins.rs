use std::sync::Arc;

use dashmap::DashMap;

use crate::storage::batch_store::BatchId;

/// Store-agnostic pin counts for MVCC-style safe reads.
///
/// Readers pin batch ids they plan to load, then unpin when done. Writers/pruners
/// "retire" batch ids from metadata, and physical deletion is deferred until pins drop to zero.
#[derive(Debug, Default)]
pub struct BatchPins {
    counts: DashMap<BatchId, usize>,
}

impl BatchPins {
    pub fn new() -> Self {
        Self {
            counts: DashMap::new(),
        }
    }

    pub fn pin_one(&self, batch_id: BatchId) {
        self.counts
            .entry(batch_id)
            .and_modify(|c| *c = c.saturating_add(1))
            .or_insert(1);
    }

    pub fn unpin_one(&self, batch_id: BatchId) {
        if let Some(mut entry) = self.counts.get_mut(&batch_id) {
            if *entry <= 1 {
                *entry = 0;
            } else {
                *entry -= 1;
            }
        } else {
            // Unbalanced unpin; ignore (caller bug).
            return;
        }

        if let Some(entry) = self.counts.get(&batch_id) {
            if *entry == 0 {
                drop(entry);
                self.counts.remove(&batch_id);
            }
        }
    }

    pub fn is_pinned(&self, batch_id: &BatchId) -> bool {
        self.counts.get(batch_id).map(|c| *c > 0).unwrap_or(false)
    }

    /// Pin `batch_ids` until the returned lease is dropped.
    pub fn pin(self: Arc<Self>, batch_ids: Vec<BatchId>) -> BatchLease {
        for id in &batch_ids {
            self.pin_one(*id);
        }
        BatchLease {
            pins: self,
            batch_ids,
        }
    }
}

/// A lease that pins a set of batches for the duration of a read.
#[derive(Debug)]
pub struct BatchLease {
    pins: Arc<BatchPins>,
    batch_ids: Vec<BatchId>,
}

impl Drop for BatchLease {
    fn drop(&mut self) {
        for id in self.batch_ids.drain(..) {
            self.pins.unpin_one(id);
        }
    }
}

