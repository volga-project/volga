use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::api::spec::state::{OperatorStateBackendConfig, RequestStoreConfig};
use crate::common::KeyGroupRange;
use crate::runtime::consts::{runtime_consts, WINDOW_PROCESS_PAGE_SIZE};
use crate::runtime::operators::window::model::{
    Cursor, KeyState, PartitionKey, RawRun, StateNamespace, TileMap, TileRun, WindowTrigger,
};
use crate::runtime::operators::OperatorKind;
use crate::runtime::state::{OperatorStore, StateRegistry};

use super::WindowData;

mod inmem;

pub use inmem::{InMemWindowStore, InMemWindowStoreClient};

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
    }
}

pub async fn open_window_request_store(
    config: &RequestStoreConfig,
) -> Result<Arc<dyn WindowRequestStore>> {
    match *config {}
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

/// Drain `(after, through]` for tests. The operator loops `load_triggers` itself.
pub async fn collect_triggers(
    store: &dyn WindowOperatorStore,
    after: Option<Cursor>,
    through: Cursor,
) -> Result<Vec<WindowTrigger>> {
    let limit = runtime_consts().u64(WINDOW_PROCESS_PAGE_SIZE).max(1) as usize;
    let mut resume = None;
    let mut out = Vec::new();
    loop {
        let (triggers, next) = store
            .load_triggers(after, through, resume.as_ref(), limit)
            .await?;
        out.extend(triggers);
        match next {
            Some(token) => resume = Some(token),
            None => break,
        }
    }
    Ok(out)
}

/// Opaque pager token. Only the backend that produced it should pass it back.
#[derive(Debug, Clone)]
pub struct TriggerResume {
    last: WindowTrigger,
}

impl TriggerResume {
    pub(crate) fn after_visible(last: WindowTrigger) -> Self {
        Self { last }
    }

    pub(crate) fn last(&self) -> &WindowTrigger {
        &self.last
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
    /// One hop of due triggers in `(after, through]`.
    ///
    /// Short pages and empty `triggers` with `Some(resume)` are legal. End of
    /// range is `next is None` — do not treat an empty page as EOF.
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
