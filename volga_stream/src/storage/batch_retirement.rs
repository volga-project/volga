use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::task::JoinHandle;

use crate::common::Key;
use crate::storage::batch_pins::BatchPins;
use crate::storage::batch_store::{BatchId, BatchStore};

/// Tracks batches that were removed from metadata and should be deleted once unpinned.
#[derive(Debug, Default)]
pub struct BatchRetirementQueue {
    // batch_id -> partition_key (needed for store deletion)
    retired: DashMap<BatchId, Key>,
}

impl BatchRetirementQueue {
    pub fn new() -> Self {
        Self {
            retired: DashMap::new(),
        }
    }

    /// Schedule batches for eventual deletion.
    pub fn retire(&self, partition_key: &Key, batch_ids: &[BatchId]) {
        for id in batch_ids {
            self.retired.entry(*id).or_insert_with(|| partition_key.clone());
        }
    }

    /// Try deleting up to `max_batches` retired batches that are currently unpinned.
    pub async fn reclaim_once(
        &self,
        store: Arc<dyn BatchStore>,
        pins: &BatchPins,
        max_batches: usize,
    ) {
        if max_batches == 0 {
            return;
        }

        // Collect candidates without holding iter guards across await.
        let mut candidates: Vec<(BatchId, Key)> = Vec::new();
        for entry in self.retired.iter() {
            if candidates.len() >= max_batches {
                break;
            }
            let id = *entry.key();
            if pins.is_pinned(&id) {
                continue;
            }
            candidates.push((id, entry.value().clone()));
        }
        if candidates.is_empty() {
            return;
        }

        let mut by_key: HashMap<Key, Vec<BatchId>> = HashMap::new();
        for (id, key) in &candidates {
            by_key.entry(key.clone()).or_default().push(*id);
        }

        for (key, ids) in by_key {
            store.remove_batches(&ids, &key).await;
        }

        for (id, _) in candidates {
            self.retired.remove(&id);
        }
    }

    /// Spawn a background task that periodically deletes unpinned retired batches.
    pub fn spawn_reclaimer(
        self: Arc<Self>,
        store: Arc<dyn BatchStore>,
        pins: Arc<BatchPins>,
        interval: Duration,
        batch_budget_per_tick: usize,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            loop {
                tick.tick().await;
                self.reclaim_once(store.clone(), &pins, batch_budget_per_tick)
                    .await;
            }
        })
    }
}

