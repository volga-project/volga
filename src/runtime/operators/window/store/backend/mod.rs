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
mod due;
mod inmem;
mod scylla;

pub use due::{stream_due, trigger_fetch_limit, DueWorkStream};
pub use inmem::{InMemWindowStore, InMemWindowStoreClient};
pub use scylla::{ScyllaWindowStore, ScyllaWindowStoreClient};

/// Job-level execution attempt stamped on published versions.
pub type AttemptToken = Vec<u8>;

/// Task-execution identity stored on the writer head fence (Scylla).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WriterId(pub Vec<u8>);

/// Per-task scope created at WO `open`. Trait methods do not take namespace or range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowStoreTaskScope {
    pub namespace: StateNamespace,
    pub max_parallelism: usize,
    pub key_group_range: KeyGroupRange,
    pub writer_id: WriterId,
    pub attempt: AttemptToken,
}

impl WindowStoreTaskScope {
    pub fn for_test(namespace: StateNamespace) -> Self {
        Self {
            namespace,
            max_parallelism: 1,
            key_group_range: KeyGroupRange::full(1),
            writer_id: WriterId(Vec::new()),
            attempt: Vec::new(),
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
            anyhow::ensure!(
                !scope.attempt.is_empty(),
                "Scylla window store requires execution_attempt_id"
            );
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
    match config {
        RequestStoreConfig::Scylla(cfg) => {
            Ok(Arc::new(ScyllaWindowStore::connect(cfg.clone()).await?)
                as Arc<dyn WindowRequestStore>)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateVersion {
    pub attempt: AttemptToken,
    pub epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowBackendSnapshot {
    /// Development/test-only inline snapshot.
    InMemory { snapshot: Vec<u8> },
    Versioned { version: StateVersion },
}

#[derive(Debug, Clone)]
pub struct DueWindowWork {
    pub partition: PartitionKey,
    pub key_state: KeyState,
    pub triggers: Vec<WindowTrigger>,
}

/// Resume token for [`WindowOperatorStore::load_triggers`]. Opaque to the
/// operator: only the backend that produced it should pass it back.
#[derive(Debug, Clone)]
pub struct TriggerResume {
    pub last: WindowTrigger,
    /// Last raw clustering `(attempt, epoch)` when the store distinguishes
    /// overlay-hidden rows from visible ones. Empty attempt = last visible.
    pub raw_attempt: Vec<u8>,
    pub raw_epoch: i64,
    /// Scylla `(bucket, shard)` walk index. InMem ignores this.
    pub part_idx: usize,
}

impl TriggerResume {
    pub fn after_visible(last: WindowTrigger) -> Self {
        Self {
            last,
            raw_attempt: Vec::new(),
            raw_epoch: 0,
            part_idx: 0,
        }
    }
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
    async fn load_triggers(
        &self,
        after: Option<Cursor>,
        through: Cursor,
        resume: Option<&TriggerResume>,
        limit: usize,
    ) -> Result<(Vec<WindowTrigger>, Option<TriggerResume>)>;
    async fn store_key_state(&self, partition: &PartitionKey, state: &KeyState) -> Result<()>;
    /// Complete all pending writes before capturing the returned snapshot.
    async fn checkpoint(&self) -> Result<WindowBackendSnapshot>;
    async fn restore(&self, snapshot: &WindowBackendSnapshot) -> Result<()>;
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
