use std::sync::Arc;

use dashmap::DashMap;

use crate::storage::batch_store::BatchId;
use crate::runtime::TaskId;

/// Store-agnostic pin counts for MVCC-style safe reads.
///
/// Readers pin batch ids they plan to load, then unpin when done. Writers/pruners
/// "retire" batch ids from metadata, and physical deletion is deferred until pins drop to zero.
#[derive(Debug, Default)]
pub struct BatchPins {
    counts: DashMap<(TaskId, BatchId), usize>,
}

impl BatchPins {
    pub fn new() -> Self {
        Self {
            counts: DashMap::new(),
        }
    }

    pub fn pin_one(&self, task_id: TaskId, batch_id: BatchId) {
        self.counts
            .entry((task_id, batch_id))
            .and_modify(|c| *c = c.saturating_add(1))
            .or_insert(1);
    }

    pub fn unpin_one(&self, task_id: TaskId, batch_id: BatchId) {
        let k = (task_id, batch_id);
        if let Some(mut entry) = self.counts.get_mut(&k) {
            if *entry <= 1 {
                *entry = 0;
            } else {
                *entry -= 1;
            }
        } else {
            // Unbalanced unpin; ignore (caller bug).
            return;
        }

        if let Some(entry) = self.counts.get(&k) {
            if *entry == 0 {
                drop(entry);
                self.counts.remove(&k);
            }
        }
    }

    pub fn is_pinned(&self, task_id: &TaskId, batch_id: &BatchId) -> bool {
        self.counts
            .get(&(task_id.clone(), *batch_id))
            .map(|c| *c > 0)
            .unwrap_or(false)
    }

    /// Pin `batch_ids` until the returned lease is dropped.
    pub fn pin(self: Arc<Self>, task_id: TaskId, batch_ids: Vec<BatchId>) -> BatchLease {
        for id in &batch_ids {
            self.pin_one(task_id.clone(), *id);
        }
        BatchLease {
            pins: self,
            task_id,
            batch_ids,
        }
    }
}

/// A lease that pins a set of batches for the duration of a read.
#[derive(Debug)]
pub struct BatchLease {
    pins: Arc<BatchPins>,
    task_id: TaskId,
    batch_ids: Vec<BatchId>,
}

impl Drop for BatchLease {
    fn drop(&mut self) {
        for id in self.batch_ids.drain(..) {
            self.pins.unpin_one(self.task_id.clone(), id);
        }
    }
}




