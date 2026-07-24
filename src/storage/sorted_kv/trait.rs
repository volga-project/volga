use std::sync::Arc;

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

/// Point-in-time read view for one [`SortedKV::snapshot_partition`] scope.
///
/// All `get` / `scan` on this handle observe one consistent state. Drop releases
/// backend pins (Rocks snapshot, Slate snapshot, Scylla row cache, etc.).
#[async_trait]
pub trait KvSnapshot: Send + Sync + std::fmt::Debug {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Forward scan over half-open `[start, end)`.
    fn scan<'a>(
        &'a self,
        start: &'a [u8],
        end: &'a [u8],
    ) -> BoxStream<'a, Result<(Vec<u8>, Vec<u8>)>>;
}

/// Forward-only sorted key-value store.
///
/// ## Consistency contract
///
/// - **Writes:** [`SortedKV::write`] is atomic — concurrent readers see the batch
///   fully applied or not at all (never a partial batch).
/// - **Multi-op reads:** use [`SortedKV::snapshot_partition`] then get/scan on the
///   returned [`KvSnapshot`]. Scope is one opaque `partition_key` (co-location id
///   for a business key’s rows — not a byte-key prefix of kind-first KV keys).
///
/// Caching and durability are also backend concerns.
#[async_trait]
pub trait SortedKV: Send + Sync + std::fmt::Debug {
    /// Latest-state point read (single op). Prefer [`snapshot_partition`] for
    /// multi-get / multi-scan loads.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Latest-state forward scan over half-open `[start, end)`.
    fn scan<'a>(
        &'a self,
        start: &'a [u8],
        end: &'a [u8],
    ) -> BoxStream<'a, Result<(Vec<u8>, Vec<u8>)>>;

    /// Pin a consistent read of all keys co-located under `partition_key`.
    ///
    /// `partition_key` is an opaque co-location id (e.g. `{ns}/{key_hash}`), not
    /// a SortedKV scan prefix. Backends: Scylla → one partition SELECT; Rocks/Slate
    /// → DB snapshot (may ignore id); InMem → map epoch.
    async fn snapshot_partition(
        &self,
        partition_key: &[u8],
    ) -> Result<Arc<dyn KvSnapshot>>;

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    async fn delete(&self, key: &[u8]) -> Result<()>;

    /// Apply all ops atomically (no torn visibility to concurrent readers).
    async fn write(&self, batch: WriteBatch) -> Result<()>;

    async fn flush(&self) -> Result<()>;
}
