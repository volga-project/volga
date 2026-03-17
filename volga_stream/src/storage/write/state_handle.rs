use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::RwLock;

use crate::common::Key;
use crate::runtime::TaskId;
use crate::storage::write::state_cache::StateCache;
use crate::storage::write::state_serde::StateSerde;
use crate::storage::WorkerStorageRuntime;

/// Typed state handle that hides backend KV and memory pool wiring.
#[derive(Debug)]
pub struct StateHandle<V> {
    cache: Arc<StateCache<V>>,
}

impl<V> Clone for StateHandle<V> {
    fn clone(&self) -> Self {
        Self {
            cache: self.cache.clone(),
        }
    }
}

impl<V> StateHandle<V> {
    pub fn new(
        storage: &Arc<WorkerStorageRuntime>,
        task_id: TaskId,
        namespace: &str,
        serde: StateSerde<V>,
    ) -> Self {
        let cache = Arc::new(StateCache::new(
            task_id,
            storage.backend.clone(),
            storage.mem_pool.clone(),
            &serde,
            namespace,
        ));
        Self { cache }
    }

    pub fn cache(&self) -> &Arc<StateCache<V>> {
        &self.cache
    }

    pub fn values(&self) -> &Arc<DashMap<Key, Arc<RwLock<V>>>> {
        self.cache.values()
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn get(&self, key: &Key) -> Option<Arc<RwLock<V>>> {
        self.cache.get(key)
    }

    pub fn get_or_insert(&self, key: &Key, make: impl FnOnce() -> V) -> Arc<RwLock<V>> {
        self.cache.get_or_insert(key, make)
    }

    pub fn put(&self, key: &Key, value: V) -> Arc<RwLock<V>> {
        self.cache.put(key, value)
    }

    pub async fn get_or_load(
        &self,
        key: &Key,
        make: impl FnOnce() -> V,
    ) -> anyhow::Result<Arc<RwLock<V>>> {
        self.cache.get_or_load(key, make).await
    }

    pub fn mark_dirty(&self, key: &Key) {
        self.cache.mark_dirty(key);
    }

    pub async fn flush_all(&self) -> anyhow::Result<()> {
        self.cache.flush_all().await
    }

    pub async fn flush(&self, key: &Key) -> anyhow::Result<()> {
        self.cache.flush_key(key).await
    }

    pub async fn restore_all(&self) -> anyhow::Result<()> {
        self.cache.restore_all().await
    }

    pub async fn evict_lru(&self, target_bytes: usize) -> anyhow::Result<usize> {
        self.cache.evict_lru(target_bytes).await
    }
}
