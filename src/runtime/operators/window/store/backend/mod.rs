use anyhow::Result;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::runtime::operators::window::model::{
    KeyState, PartitionKey, RawRun, StateNamespace, TileMap, TileRun,
};

use super::WindowData;

mod inmem;

pub use inmem::InMemWindowStore;

pub type AttemptToken = Vec<u8>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateVersion {
    pub attempt: AttemptToken,
    pub epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowCheckpointMeta {
    pub version: StateVersion,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowRestoreMeta {
    pub base_version: StateVersion,
}

impl From<WindowCheckpointMeta> for WindowRestoreMeta {
    fn from(meta: WindowCheckpointMeta) -> Self {
        Self {
            base_version: meta.version,
        }
    }
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
    ) -> Result<()>;
    async fn store_meta(&self, partition: &PartitionKey, meta: &KeyState) -> Result<()>;
    async fn flush(&self) -> Result<()> {
        Ok(())
    }
    async fn checkpoint(&self, namespace: &StateNamespace) -> Result<WindowCheckpointMeta>;
    async fn restore(&self, namespace: &StateNamespace, meta: &WindowRestoreMeta) -> Result<()>;
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
