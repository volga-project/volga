use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::common::Key;
use crate::storage::batch_pins::BatchPins;
use crate::storage::batch_store::{BatchId, BatchStore};
use crate::runtime::TaskId;

/// Tracks batches that were removed from metadata and should be deleted once unpinned.
#[derive(Debug, Default)]
pub struct BatchRetirementQueue {
    // (task_id, batch_id) -> partition_key (needed for store deletion)
    retired: DashMap<(TaskId, BatchId), Key>,
}

impl BatchRetirementQueue {
    pub fn new() -> Self {
        Self {
            retired: DashMap::new(),
        }
    }

    /// Schedule batches for eventual deletion.
    pub fn retire(&self, task_id: TaskId, partition_key: &Key, batch_ids: &[BatchId]) {
        for id in batch_ids {
            self.retired
                .entry((task_id.clone(), *id))
                .or_insert_with(|| partition_key.clone());
        }
    }

    /// Try deleting up to `max_batches` retired batches that are currently unpinned.
    pub async fn reclaim_once(
        &self,
        store: Arc<dyn BatchStore>,
        pins: &BatchPins,
        task_id: &TaskId,
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
            let (t, id) = entry.key().clone();
            if &t != task_id {
                continue;
            }
            if pins.is_pinned(task_id, &id) {
                continue;
            }
            candidates.push((id, entry.value().clone()));
        }
        if candidates.is_empty() {
            return;
        }

        // Best-effort bounded fanout: store API is singular.
        let sem = Arc::new(Semaphore::new(8));
        let mut futs: FuturesUnordered<_> = FuturesUnordered::new();
        for (id, key) in &candidates {
            let store = store.clone();
            let key = key.clone();
            let id = *id;
            let sem = sem.clone();
            let task_id = task_id.clone();
            futs.push(async move {
                let _p = sem.acquire_owned().await.expect("semaphore");
                store.remove_batch(task_id, id, &key).await;
            });
        }
        while futs.next().await.is_some() {}

        for (id, _) in candidates {
            self.retired.remove(&(task_id.clone(), id));
        }
    }

    /// Spawn a background task that periodically deletes unpinned retired batches.
    pub fn spawn_reclaimer(
        self: Arc<Self>,
        store: Arc<dyn BatchStore>,
        pins: Arc<BatchPins>,
        task_id: TaskId,
        interval: Duration,
        batch_budget_per_tick: usize,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            loop {
                tick.tick().await;
                self.reclaim_once(store.clone(), &pins, &task_id, batch_budget_per_tick)
                    .await;
            }
        })
    }
}




