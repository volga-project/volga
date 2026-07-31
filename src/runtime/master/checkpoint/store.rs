//! Storage for completed pipeline checkpoints.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::api::spec::state::CheckpointStoreConfig;
use crate::common::types::PipelineId;
use crate::runtime::checkpoint::CompletedCheckpoint;

pub fn create_checkpoint_store(config: &CheckpointStoreConfig) -> Arc<dyn CheckpointStore> {
    match config {
        CheckpointStoreConfig::InMemory => Arc::new(InMemoryCheckpointStore::default()),
    }
}

#[async_trait]
pub trait CheckpointStore: Send + Sync + std::fmt::Debug {
    async fn save(&self, pipeline_id: &PipelineId, checkpoint: CompletedCheckpoint) -> Result<()>;

    async fn load(
        &self,
        pipeline_id: &PipelineId,
        checkpoint_id: u64,
    ) -> Result<Option<CompletedCheckpoint>>;

    async fn remove(&self, pipeline_id: &PipelineId, checkpoint_id: u64) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct InMemoryCheckpointStore {
    checkpoints: RwLock<HashMap<(PipelineId, u64), CompletedCheckpoint>>,
}

#[async_trait]
impl CheckpointStore for InMemoryCheckpointStore {
    async fn save(&self, pipeline_id: &PipelineId, checkpoint: CompletedCheckpoint) -> Result<()> {
        let checkpoint_id = checkpoint.checkpoint_id;
        self.checkpoints
            .write()
            .await
            .insert((pipeline_id.clone(), checkpoint_id), checkpoint);
        Ok(())
    }

    async fn load(
        &self,
        pipeline_id: &PipelineId,
        checkpoint_id: u64,
    ) -> Result<Option<CompletedCheckpoint>> {
        Ok(self
            .checkpoints
            .read()
            .await
            .get(&(pipeline_id.clone(), checkpoint_id))
            .cloned())
    }

    async fn remove(&self, pipeline_id: &PipelineId, checkpoint_id: u64) -> Result<()> {
        self.checkpoints
            .write()
            .await
            .remove(&(pipeline_id.clone(), checkpoint_id));
        Ok(())
    }
}
