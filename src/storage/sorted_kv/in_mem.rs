use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use parking_lot::RwLock;

use super::{KvOp, SortedKV, WriteBatch};

#[derive(Debug, Default)]
struct Inner {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
}

/// In-memory SortedKV. Cloning shares the same backend (multiple clients).
#[derive(Debug, Clone)]
pub struct InMemSortedKV {
    inner: Arc<RwLock<Inner>>,
}

impl InMemSortedKV {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner::default())),
        }
    }

    pub fn new_shared() -> Self {
        Self::new()
    }
}

impl Default for InMemSortedKV {
    fn default() -> Self {
        Self::new()
    }
}

fn apply_batch_locked(inner: &mut Inner, batch: WriteBatch) {
    for op in batch.ops {
        match op {
            KvOp::Put { key, value } => {
                inner.map.insert(key, value);
            }
            KvOp::Delete { key } => {
                inner.map.remove(&key);
            }
        }
    }
}

#[async_trait]
impl SortedKV for InMemSortedKV {
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::with_capacity(1);
        batch.put(key.to_vec(), value.to_vec());
        self.write(batch).await
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.inner.read().map.get(key).cloned())
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::with_capacity(1);
        batch.delete(key.to_vec());
        self.write(batch).await
    }

    fn scan<'a>(
        &'a self,
        start: &'a [u8],
        end: &'a [u8],
    ) -> BoxStream<'a, Result<(Vec<u8>, Vec<u8>)>> {
        let start = start.to_vec();
        let end = end.to_vec();
        let items: Vec<(Vec<u8>, Vec<u8>)> = {
            if start >= end {
                Vec::new()
            } else {
                let guard = self.inner.read();
                guard
                    .map
                    .range(start..end)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            }
        };
        stream::iter(items.into_iter().map(Ok)).boxed()
    }

    async fn write(&self, batch: WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let mut guard = self.inner.write();
        apply_batch_locked(&mut guard, batch);
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn two_clients_share_backend() {
        let a = InMemSortedKV::new();
        let b = a.clone();

        a.put(b"k1", b"v1").await.unwrap();
        assert_eq!(b.get(b"k1").await.unwrap().as_deref(), Some(b"v1".as_ref()));

        b.put(b"k2", b"v2").await.unwrap();
        let mut scan = a.scan(b"k1", b"k3");
        let mut keys = Vec::new();
        while let Some(item) = scan.next().await {
            keys.push(item.unwrap().0);
        }
        assert_eq!(keys, vec![b"k1".to_vec(), b"k2".to_vec()]);

        a.delete(b"k1").await.unwrap();
        assert!(b.get(b"k1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn write_batch_is_atomic() {
        let kv = InMemSortedKV::new();
        let mut batch = WriteBatch::new();
        batch.put(b"a", b"1");
        batch.put(b"b", b"2");
        batch.delete(b"missing");
        kv.write(batch).await.unwrap();

        assert_eq!(kv.get(b"a").await.unwrap().as_deref(), Some(b"1".as_ref()));
        assert_eq!(kv.get(b"b").await.unwrap().as_deref(), Some(b"2".as_ref()));
    }
}
