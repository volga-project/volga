use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};

use crate::api::spec::state::{OperatorStateBackendConfig, RequestStoreConfig};
use crate::runtime::operators::window::model::{
    Cursor, KeyState, PartitionKey, RawRun, StateNamespace, TileMap, TileRun, WindowTrigger,
};
use crate::runtime::operators::OperatorKind;
use crate::runtime::state::{OperatorStore, StateRegistry};

use super::WindowData;

mod inmem;

pub use inmem::InMemWindowStore;

/// Op-specific open: share via [`StateRegistry`].
pub fn open_window_operator_store(
    registry: &StateRegistry,
    config: &OperatorStateBackendConfig,
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
            Ok(Arc::new(inmem) as Arc<dyn WindowOperatorStore>)
        }
    }
}

pub async fn open_window_request_store(
    config: &RequestStoreConfig,
) -> Result<Arc<dyn WindowRequestStore>> {
    match *config {}
}

pub type AttemptToken = Vec<u8>;

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

pub type DueWorkStream<'a> = BoxStream<'a, Result<Vec<DueWindowWork>>>;

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
    fn stream_due<'a>(
        &'a self,
        namespace: &'a StateNamespace,
        after: Option<Cursor>,
        through: Cursor,
    ) -> DueWorkStream<'a>;
    async fn store_key_state(&self, partition: &PartitionKey, state: &KeyState) -> Result<()>;
    /// Complete all pending writes before capturing the returned snapshot.
    async fn checkpoint(&self, namespace: &StateNamespace) -> Result<WindowBackendSnapshot>;
    async fn restore(
        &self,
        namespace: &StateNamespace,
        snapshot: &WindowBackendSnapshot,
    ) -> Result<()>;
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
