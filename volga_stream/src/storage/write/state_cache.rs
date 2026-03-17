use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use dashmap::DashMap;
use tokio::sync::RwLock;

use crate::common::Key;
use crate::runtime::TaskId;
use crate::storage::backend::DynStorageBackend;
use crate::storage::memory_pool::{TenantType, WorkerMemoryPool};
use crate::storage::write::state_serde::StateSerde;

pub fn state_index_key(namespace: &str) -> Vec<u8> {
    format!("{namespace}/index").into_bytes()
}

pub fn state_entry_prefix(namespace: &str) -> Vec<u8> {
    format!("{namespace}/entry/").into_bytes()
}

pub struct StateCache<V> {
    task_id: TaskId,
    backend: DynStorageBackend,
    mem_pool: Arc<WorkerMemoryPool>,
    clock: AtomicU64,
    values: Arc<DashMap<Key, Arc<RwLock<V>>>>,
    meta: DashMap<Key, StateMeta>,
    serde: StateSerde<V>,
    index_key: Vec<u8>,
    entry_prefix: Vec<u8>,
}

impl<V> std::fmt::Debug for StateCache<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateCache")
            .field("task_id", &self.task_id)
            .field("len", &self.values.len())
            .field("index_key", &self.index_key)
            .field("entry_prefix", &self.entry_prefix)
            .finish()
    }
}

struct StateMeta {
    bytes: usize,
    dirty: AtomicBool,
    last_access: AtomicU64,
}

impl StateMeta {
    fn new(bytes: usize, dirty: bool, last_access: u64) -> Self {
        Self {
            bytes,
            dirty: AtomicBool::new(dirty),
            last_access: AtomicU64::new(last_access),
        }
    }
}

impl<V> StateCache<V> {
    pub fn new(
        task_id: TaskId,
        backend: DynStorageBackend,
        mem_pool: Arc<WorkerMemoryPool>,
        serde: &StateSerde<V>,
        namespace: &str,
    ) -> Self {
        let index_key = state_index_key(namespace);
        let entry_prefix = state_entry_prefix(namespace);
        Self {
            task_id,
            backend,
            mem_pool,
            clock: AtomicU64::new(1),
            values: Arc::new(DashMap::new()),
            meta: DashMap::new(),
            serde: *serde,
            index_key,
            entry_prefix,
        }
    }

    pub fn values(&self) -> &Arc<DashMap<Key, Arc<RwLock<V>>>> {
        &self.values
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn get(&self, key: &Key) -> Option<Arc<RwLock<V>>> {
        let now = self.clock.fetch_add(1, Ordering::Relaxed);
        if let Some(entry) = self.meta.get(key) {
            entry.last_access.store(now, Ordering::Relaxed);
        }
        self.values.get(key).map(|e| e.value().clone())
    }

    pub fn get_or_insert(&self, key: &Key, make: impl FnOnce() -> V) -> Arc<RwLock<V>> {
        if let Some(existing) = self.get(key) {
            return existing;
        }
        let value = make();
        self.insert(key, value)
    }

    pub async fn get_or_load(&self, key: &Key, make: impl FnOnce() -> V) -> anyhow::Result<Arc<RwLock<V>>> {
        if let Some(existing) = self.get(key) {
            return Ok(existing);
        }
        let entry_key = self.make_entry_key(key);
        let loaded = self
            .backend
            .state_get(self.task_id.clone(), &entry_key)
            .await?;
        let value = if let Some(bytes) = loaded {
            (self.serde.decode)(&bytes)?
        } else {
            make()
        };
        Ok(self.insert(key, value))
    }

    pub fn insert(&self, key: &Key, value: V) -> Arc<RwLock<V>> {
        let now = self.clock.fetch_add(1, Ordering::Relaxed);
        let bytes = (self.serde.estimate)(&value)
            .saturating_add(key.get_memory_size());
        let arc = Arc::new(RwLock::new(value));
        self.values.insert(key.clone(), arc.clone());
        self.meta.insert(key.clone(), StateMeta::new(bytes, true, now));
        self.mem_pool
            .reserve_over_limit(&self.task_id, TenantType::StateCache, bytes);
        arc
    }

    pub fn put(&self, key: &Key, value: V) -> Arc<RwLock<V>> {
        let now = self.clock.fetch_add(1, Ordering::Relaxed);
        let bytes = (self.serde.estimate)(&value)
            .saturating_add(key.get_memory_size());
        let arc = Arc::new(RwLock::new(value));
        if let Some((_, old_meta)) = self.meta.remove(key) {
            self.mem_pool
                .release(&self.task_id, TenantType::StateCache, old_meta.bytes);
        }
        self.values.insert(key.clone(), arc.clone());
        self.meta.insert(key.clone(), StateMeta::new(bytes, true, now));
        self.mem_pool
            .reserve_over_limit(&self.task_id, TenantType::StateCache, bytes);
        arc
    }

    pub fn mark_dirty(&self, key: &Key) {
        if let Some(entry) = self.meta.get(key) {
            entry.dirty.store(true, Ordering::Release);
        }
    }

    pub async fn flush_all(&self) -> anyhow::Result<()> {
        let entries: Vec<(Key, Arc<RwLock<V>>)> = self
            .values
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();

        let mut new_keys: Vec<Vec<u8>> = Vec::with_capacity(entries.len());
        let mut values: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(entries.len());

        for (key, arc) in entries {
            let guard = arc.read().await;
            let bytes = (self.serde.encode)(&*guard)?;
            let key_bytes = key.to_bytes();
            new_keys.push(key_bytes.clone());
            let mut entry_key = Vec::with_capacity(self.entry_prefix.len() + key_bytes.len());
            entry_key.extend_from_slice(&self.entry_prefix);
            entry_key.extend_from_slice(&key_bytes);
            values.push((entry_key, bytes));
        }

        if let Some(prev) = self.backend.state_get(self.task_id.clone(), &self.index_key).await? {
            if let Ok(prev_keys) = bincode::deserialize::<Vec<Vec<u8>>>(&prev) {
                let prev_set: std::collections::HashSet<Vec<u8>> = prev_keys.into_iter().collect();
                let new_set: std::collections::HashSet<Vec<u8>> = new_keys.iter().cloned().collect();
                for k in prev_set.difference(&new_set) {
                    let mut entry_key =
                        Vec::with_capacity(self.entry_prefix.len() + k.len());
                    entry_key.extend_from_slice(&self.entry_prefix);
                    entry_key.extend_from_slice(k);
                    let _ = self
                        .backend
                        .state_delete(self.task_id.clone(), &entry_key)
                        .await;
                }
            }
        }

        let index_bytes = bincode::serialize(&new_keys)?;
        self.backend
            .state_put(self.task_id.clone(), self.index_key.clone(), index_bytes)
            .await?;

        for (entry_key, bytes) in values {
            self.backend
                .state_put(self.task_id.clone(), entry_key, bytes)
                .await?;
        }

        for entry in self.meta.iter() {
            entry.dirty.store(false, Ordering::Release);
        }

        Ok(())
    }

    pub async fn flush_key(&self, key: &Key) -> anyhow::Result<()> {
        let Some(arc) = self.get(key) else {
            return Ok(());
        };
        let guard = arc.read().await;
        let bytes = (self.serde.encode)(&*guard)?;
        let entry_key = self.make_entry_key(key);

        self.backend
            .state_put(self.task_id.clone(), entry_key, bytes)
            .await?;

        let mut keys: Vec<Vec<u8>> = if let Some(prev) =
            self.backend.state_get(self.task_id.clone(), &self.index_key).await?
        {
            bincode::deserialize(&prev).unwrap_or_default()
        } else {
            Vec::new()
        };

        let key_bytes = key.to_bytes();
        if !keys.iter().any(|k| k == &key_bytes) {
            keys.push(key_bytes);
            let index_bytes = bincode::serialize(&keys)?;
            self.backend
                .state_put(self.task_id.clone(), self.index_key.clone(), index_bytes)
                .await?;
        }

        if let Some(entry) = self.meta.get(key) {
            entry.dirty.store(false, Ordering::Release);
        }

        Ok(())
    }

    pub async fn restore_all(&self) -> anyhow::Result<()> {
        self.values.clear();
        self.meta.clear();
        self.mem_pool
            .reset_used(&self.task_id, TenantType::StateCache, 0);

        let Some(index_bytes) = self.backend.state_get(self.task_id.clone(), &self.index_key).await? else {
            return Ok(());
        };
        let keys: Vec<Vec<u8>> = bincode::deserialize(&index_bytes)?;
        let mut total_bytes: usize = 0;

        for key_bytes in keys {
            let mut entry_key = Vec::with_capacity(self.entry_prefix.len() + key_bytes.len());
            entry_key.extend_from_slice(&self.entry_prefix);
            entry_key.extend_from_slice(&key_bytes);
            let bytes = self
                .backend
                .state_get(self.task_id.clone(), &entry_key)
                .await?
                .ok_or_else(|| anyhow::anyhow!("missing state entry in backend state"))?;
            let value = (self.serde.decode)(&bytes)?;
            let key = Key::from_bytes(&key_bytes);
            let est = (self.serde.estimate)(&value).saturating_add(key.get_memory_size());
            let now = self.clock.fetch_add(1, Ordering::Relaxed);
            self.values.insert(key.clone(), Arc::new(RwLock::new(value)));
            self.meta.insert(key, StateMeta::new(est, false, now));
            total_bytes = total_bytes.saturating_add(est);
        }

        self.mem_pool
            .reset_used(&self.task_id, TenantType::StateCache, total_bytes);
        Ok(())
    }

    pub async fn evict_lru(&self, target_bytes: usize) -> anyhow::Result<usize> {
        if target_bytes == 0 {
            return Ok(0);
        }
        let mut entries: Vec<(Key, u64, usize, bool)> = self
            .meta
            .iter()
            .map(|e| {
                (
                    e.key().clone(),
                    e.last_access.load(Ordering::Relaxed),
                    e.bytes,
                    e.dirty.load(Ordering::Acquire),
                )
            })
            .collect();
        if entries.is_empty() {
            return Ok(0);
        }
        entries.sort_by_key(|(_, last_access, _, _)| *last_access);

        let mut freed: usize = 0;
        for (key, _last_access, bytes, dirty) in entries {
            if freed >= target_bytes {
                break;
            }
            if dirty {
                let _ = self.flush_key(&key).await;
            }
            let _ = self.values.remove(&key);
            let _ = self.meta.remove(&key);
            self.mem_pool
                .release(&self.task_id, TenantType::StateCache, bytes);
            freed = freed.saturating_add(bytes);
        }
        Ok(freed)
    }

    fn make_entry_key(&self, key: &Key) -> Vec<u8> {
        let key_bytes = key.to_bytes();
        let mut entry_key = Vec::with_capacity(self.entry_prefix.len() + key_bytes.len());
        entry_key.extend_from_slice(&self.entry_prefix);
        entry_key.extend_from_slice(&key_bytes);
        entry_key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::storage::index::TimeGranularity;
    use crate::storage::backend::inmem::InMemBackend;
    use crate::storage::budget::StorageBackendConfig;
    use crate::storage::write::state_serde::bytes_serde;

    fn make_key(s: &str) -> Key {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
        let array = StringArray::from(vec![s]);
        let batch = arrow::record_batch::RecordBatch::try_new(schema, vec![Arc::new(array)])
            .expect("key batch");
        Key::new(batch).expect("key")
    }

    #[tokio::test]
    async fn flush_and_restore_round_trip() {
        let cfg = StorageBackendConfig::default();
        let backend = Arc::new(InMemBackend::new(TimeGranularity::Seconds(1), 1024));
        let mem_pool = WorkerMemoryPool::new(cfg.memory);
        let serde = bytes_serde();
        let task_id: TaskId = Arc::<str>::from("t");

        let cache = StateCache::new(
            task_id.clone(),
            backend.clone(),
            mem_pool.clone(),
            &serde,
            "test_state",
        );

        let key = make_key("A");
        let arc = cache
            .get_or_load(&key, || b"v1".to_vec())
            .await
            .expect("load");
        {
            let guard = arc.read().await;
            assert_eq!(&*guard, b"v1");
        }
        cache.flush_all().await.expect("flush");

        let cache2 = StateCache::new(task_id.clone(), backend, mem_pool, &serde, "test_state");
        cache2.restore_all().await.expect("restore");

        let arc2 = cache2.get(&key).expect("restored");
        let guard = arc2.read().await;
        assert_eq!(&*guard, b"v1");
    }

    #[tokio::test]
    async fn concurrent_get_or_load_and_flush() {
        let cfg = StorageBackendConfig::default();
        let backend = Arc::new(InMemBackend::new(TimeGranularity::Seconds(1), 1024));
        let mem_pool = WorkerMemoryPool::new(cfg.memory);
        let serde = bytes_serde();
        let task_id: TaskId = Arc::<str>::from("t");

        let cache = Arc::new(StateCache::new(
            task_id,
            backend,
            mem_pool,
            &serde,
            "test_state_concurrent",
        ));

        let key_a = make_key("A");
        let key_b = make_key("B");

        let c1 = cache.clone();
        let k1 = key_a.clone();
        let t1 = tokio::spawn(async move {
            let arc = c1
                .get_or_load(&k1, || b"va".to_vec())
                .await
                .expect("load");
            let guard = arc.read().await;
            assert_eq!(&*guard, b"va");
        });

        let c2 = cache.clone();
        let k2 = key_b.clone();
        let t2 = tokio::spawn(async move {
            let arc = c2
                .get_or_load(&k2, || b"vb".to_vec())
                .await
                .expect("load");
            let guard = arc.read().await;
            assert_eq!(&*guard, b"vb");
        });

        let c3 = cache.clone();
        let t3 = tokio::spawn(async move {
            c3.flush_all().await.expect("flush");
        });

        let _ = tokio::join!(t1, t2, t3);

        let arc_a = cache.get(&key_a).expect("key A");
        let arc_b = cache.get(&key_b).expect("key B");
        let guard_a = arc_a.read().await;
        let guard_b = arc_b.read().await;
        assert_eq!(&*guard_a, b"va");
        assert_eq!(&*guard_b, b"vb");
    }
}
