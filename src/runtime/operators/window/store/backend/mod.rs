use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::api::spec::state::{OperatorStateBackendConfig, RequestStoreConfig};
use crate::runtime::operators::window::model::{
    Cursor, KeyState, PartitionKey, RawRun, StateNamespace, TileMap, TileRun, WindowTrigger,
};

use super::WindowData;

mod inmem;

pub use inmem::InMemWindowStore;

pub async fn open_window_operator_store(
    config: &OperatorStateBackendConfig,
) -> Result<Arc<dyn WindowOperatorStore>> {
    match config {
        OperatorStateBackendConfig::InMemory => Ok(Arc::new(InMemWindowStore::new())),
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
    InMemory { snapshot: Vec<u8> },
    Versioned { version: StateVersion },
}

#[derive(Debug, Clone)]
pub struct DueWindowWork {
    pub partition: PartitionKey,
    pub key_state: KeyState,
    pub triggers: Vec<WindowTrigger>,
}

#[derive(Debug, Clone)]
pub struct DueContinuation(pub Vec<u8>);

#[derive(Debug, Clone)]
pub struct DuePage {
    pub work: Vec<DueWindowWork>,
    pub continuation: Option<DueContinuation>,
}

/// Store operations used by the sole Window Operator for a partition.
#[async_trait]
pub trait WindowOperatorStore: Send + Sync + std::fmt::Debug {
    async fn load_meta(&self, partition: &PartitionKey) -> Result<KeyState>;
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
    async fn load_due_page(
        &self,
        namespace: &StateNamespace,
        after: Option<Cursor>,
        through: Cursor,
        continuation: Option<DueContinuation>,
    ) -> Result<DuePage>;
    async fn commit_advance(&self, partition: &PartitionKey, meta: &KeyState) -> Result<()>;
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
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
