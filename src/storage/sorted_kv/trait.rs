use anyhow::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;

/// Single mutation in a [`WriteBatch`].
#[derive(Debug, Clone)]
pub enum KvOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Atomic multi-key write. All ops become visible together or not at all.
///
/// Prefer this for WO ingest (raw + tiles + meta). See
/// `runtime/operators/window/README.md` for contract + backend mapping.
#[derive(Debug, Clone, Default)]
pub struct WriteBatch {
    pub ops: Vec<KvOp>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            ops: Vec::with_capacity(cap),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.ops.push(KvOp::Put {
            key: key.into(),
            value: value.into(),
        });
    }

    pub fn delete(&mut self, key: impl Into<Vec<u8>>) {
        self.ops.push(KvOp::Delete { key: key.into() });
    }
}

/// Forward-only sorted key-value store.
///
/// Consistency, caching, and durability semantics are backend concerns.
/// Use [`SortedKV::write`] for multi-key atomic updates (ingest).
#[async_trait]
pub trait SortedKV: Send + Sync + std::fmt::Debug {
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    async fn delete(&self, key: &[u8]) -> Result<()>;

    /// Forward scan over half-open `[start, end)`.
    fn scan<'a>(
        &'a self,
        start: &'a [u8],
        end: &'a [u8],
    ) -> BoxStream<'a, Result<(Vec<u8>, Vec<u8>)>>;

    /// Apply all ops atomically (no torn visibility to concurrent readers).
    async fn write(&self, batch: WriteBatch) -> Result<()>;

    async fn flush(&self) -> Result<()>;
}
