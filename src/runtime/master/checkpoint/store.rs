//! Storage for completed pipeline checkpoints and the job attempt counter.

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

    /// Last attempt persisted for this job, if any.
    async fn load_last_attempt(&self, pipeline_id: &PipelineId) -> Result<Option<u64>>;

    /// Persist `attempt` as the last allocated id. Must happen before workers are configured.
    async fn save_last_attempt(&self, pipeline_id: &PipelineId, attempt: u64) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct InMemoryCheckpointStore {
    checkpoints: RwLock<HashMap<(PipelineId, u64), CompletedCheckpoint>>,
    last_attempt: RwLock<HashMap<PipelineId, u64>>,
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

    async fn load_last_attempt(&self, pipeline_id: &PipelineId) -> Result<Option<u64>> {
        Ok(self.last_attempt.read().await.get(pipeline_id).copied())
    }

    async fn save_last_attempt(&self, pipeline_id: &PipelineId, attempt: u64) -> Result<()> {
        self.last_attempt
            .write()
            .await
            .insert(pipeline_id.clone(), attempt);
        Ok(())
    }
}

/// Next never-reused id: `0` on a fresh job, otherwise last + 1. Persists before return.
pub async fn allocate_attempt(
    store: &dyn CheckpointStore,
    pipeline_id: &PipelineId,
) -> Result<u64> {
    let next = match store.load_last_attempt(pipeline_id).await? {
        Some(id) => id
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("execution_attempt_id overflow"))?,
        None => 0,
    };
    store.save_last_attempt(pipeline_id, next).await?;
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pipeline() -> PipelineId {
        PipelineId("job".to_string())
    }

    #[tokio::test]
    async fn first_attempt_is_zero_and_is_persisted() {
        let store = InMemoryCheckpointStore::default();
        let id = allocate_attempt(&store, &pipeline()).await.unwrap();
        assert_eq!(id, 0);
        assert_eq!(store.load_last_attempt(&pipeline()).await.unwrap(), Some(0));
    }

    #[tokio::test]
    async fn allocate_never_reuses() {
        let store = InMemoryCheckpointStore::default();
        let p = pipeline();
        assert_eq!(allocate_attempt(&store, &p).await.unwrap(), 0);
        assert_eq!(allocate_attempt(&store, &p).await.unwrap(), 1);
        assert_eq!(allocate_attempt(&store, &p).await.unwrap(), 2);
        assert_eq!(allocate_attempt(&store, &p).await.unwrap(), 3);
    }
}
