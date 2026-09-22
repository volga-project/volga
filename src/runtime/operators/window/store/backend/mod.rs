use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::api::spec::state::{OperatorStateBackendConfig, RequestStoreConfig};
use crate::common::KeyGroupRange;
use crate::runtime::operators::window::model::{
    Cursor, KeyState, PartitionKey, RawRun, StateNamespace, TileMap, TileRun, WindowTrigger,
};
use crate::runtime::operators::OperatorKind;
use crate::runtime::state::{OperatorStore, StateRegistry, StateSessionHandle};

use super::WindowData;

mod codec;
mod inmem;
mod scylla;
mod version;

pub use inmem::{InMemWindowStore, InMemWindowStoreClient};
pub use scylla::{ScyllaWindowStore, ScyllaWindowStoreClient};
pub use version::{Attempt, CutHistory, Version};

/// Job-level execution attempt. Durable, never reused (#156).
pub type AttemptToken = Attempt;

/// Task-execution identity. Not part of data PKs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WriterId(pub Vec<u8>);

/// Per-task scope created at WO `open`. Trait methods do not take namespace or range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowStoreTaskScope {
    pub namespace: StateNamespace,
    pub max_parallelism: usize,
    pub key_group_range: KeyGroupRange,
    pub writer_id: WriterId,
    pub attempt: Attempt,
    /// When true, restore/complete write `window_kg_meta` (request mode).
    pub request_mode: bool,
}

impl WindowStoreTaskScope {
    pub fn for_test(namespace: StateNamespace) -> Self {
        Self {
            namespace,
            max_parallelism: 1,
            key_group_range: KeyGroupRange::full(1),
            writer_id: WriterId(Vec::new()),
            attempt: 1,
            request_mode: false,
        }
    }
}

/// Op-specific open: share the store via [`StateRegistry`], return a per-task client.
pub fn open_window_operator_store(
    registry: &StateRegistry,
    config: &OperatorStateBackendConfig,
    scope: &WindowStoreTaskScope,
) -> Result<Arc<dyn WindowOperatorStore>> {
    match config {
        OperatorStateBackendConfig::InMemory => {
            let labels = registry.metrics_labels().cloned();
            let store = registry.get_or_insert_store(OperatorKind::Window, move |_session| {
                Arc::new(InMemWindowStore::new().with_metrics_labels(labels))
                    as Arc<dyn OperatorStore>
            });
            let inmem = store
                .as_any()
                .downcast_ref::<InMemWindowStore>()
                .expect("window InMem store type")
                .clone();
            Ok(Arc::new(inmem.client(scope.clone())) as Arc<dyn WindowOperatorStore>)
        }
        OperatorStateBackendConfig::Scylla(cfg) => {
            let cfg = cfg.clone();
            let registered = registry.get_or_insert_store(OperatorKind::Window, move |session| {
                let session = match session {
                    Some(StateSessionHandle::Scylla(session)) => Arc::clone(session),
                    None => panic!("Scylla window store requires StateSessionHandle::Scylla"),
                };
                Arc::new(ScyllaWindowStore::new(cfg.clone(), session)) as Arc<dyn OperatorStore>
            });
            let store = registered
                .as_any()
                .downcast_ref::<ScyllaWindowStore>()
                .expect("window Scylla store type")
                .clone();
            Ok(Arc::new(store.client(scope.clone())) as Arc<dyn WindowOperatorStore>)
        }
    }
}

pub async fn open_window_request_store(
    config: &RequestStoreConfig,
) -> Result<Arc<dyn WindowRequestStore>> {
    match *config {}
}

pub type StateVersion = Version;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowBackendSnapshot {
    /// Development/test-only inline snapshot.
    InMemory { snapshot: Vec<u8> },
    Versioned {
        attempt: Attempt,
        /// Parallel to [`cuts`]: the assignment this blob was captured under.
        range: KeyGroupRange,
        cuts: Vec<CutHistory>,
    },
}

/// Store operations used by the sole Window Operator for a partition.
#[async_trait]
pub trait WindowOperatorStore: OperatorStore {
    async fn load_key_state(&self, partition: &PartitionKey) -> Result<KeyState>;
    async fn load_raw(&self, partition: &PartitionKey, runs: &[RawRun])
        -> Result<Vec<RecordBatch>>;
    async fn load_tiles(&self, partition: &PartitionKey, runs: &[TileRun]) -> Result<TileMap>;
    async fn commit_events(
        &self,
        partition: &PartitionKey,
        ts_column_index: usize,
        events: &RecordBatch,
        tiles: &TileMap,
        meta: &KeyState,
        triggers: &[WindowTrigger],
    ) -> Result<()>;
    /// Due triggers in `(after, through]`.
    async fn load_triggers(
        &self,
        after: Option<Cursor>,
        through: Cursor,
    ) -> Result<Vec<WindowTrigger>>;
    async fn store_key_state(&self, partition: &PartitionKey, state: &KeyState) -> Result<()>;
    /// Complete all pending writes before capturing the returned snapshot.
    async fn checkpoint(&self) -> Result<WindowBackendSnapshot>;
    async fn restore(&self, snapshot: &WindowBackendSnapshot) -> Result<()>;
    /// Request mode: take `cur_attempt` / heal published cut. No-op for InMem.
    async fn prepare_attempt(
        &self,
        restored: &WindowBackendSnapshot,
        committed_wm: Option<i64>,
        retention_floor: Option<i64>,
        restored_checkpoint_id: Option<u64>,
    ) -> Result<()> {
        let _ = (
            restored,
            committed_wm,
            retention_floor,
            restored_checkpoint_id,
        );
        Ok(())
    }
    /// Request mode: publish the cut of a globally completed checkpoint.
    async fn on_checkpoint_complete(
        &self,
        checkpoint_id: u64,
        snapshot: &WindowBackendSnapshot,
        committed_wm: Option<i64>,
        retention_floor: Option<i64>,
    ) -> Result<()> {
        let _ = (checkpoint_id, snapshot, committed_wm, retention_floor);
        Ok(())
    }
}

/// Coherent point-lookup reads used by the Window Request Operator.
#[async_trait]
pub trait WindowRequestStore: Send + Sync + std::fmt::Debug {
    async fn load_window_data(
        &self,
        partition: &PartitionKey,
        raw_runs: &[RawRun],
        tile_runs: &[TileRun],
    ) -> Result<WindowData>;
}
