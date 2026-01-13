use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::control_plane::types::{
    CheckpointMetadata, ExecutionIds, PipelineDesiredState, PipelineEvent, PipelineId, PipelineRun,
    PipelineSpec, PipelineSpecId, PipelineStatus,
};
use crate::api::PipelineSpec as UserPipelineSpec;
use crate::runtime::observability::PipelineSnapshotEntry;

#[async_trait]
pub trait PipelineSpecStore: Send + Sync {
    async fn put_spec(&self, spec: PipelineSpec);
    async fn get_spec(&self, pipeline_spec_id: PipelineSpecId) -> Option<PipelineSpec>;
    async fn list_specs(&self) -> Vec<PipelineSpec>;
}

#[async_trait]
pub trait PipelineRunStore: Send + Sync {
    async fn put_run(&self, run: PipelineRun);
    async fn get_run(&self, pipeline_id: PipelineId) -> Option<PipelineRun>;
    async fn list_runs(&self) -> Vec<PipelineRun>;

    async fn set_desired_state(&self, pipeline_id: PipelineId, desired: PipelineDesiredState);
    async fn get_desired_state(&self, pipeline_id: PipelineId) -> Option<PipelineDesiredState>;

    async fn put_status(&self, pipeline_id: PipelineId, status: PipelineStatus);
    async fn get_status(&self, pipeline_id: PipelineId) -> Option<PipelineStatus>;
}

#[async_trait]
pub trait PipelineEventStore: Send + Sync {
    async fn append_event(&self, pipeline_id: PipelineId, event: PipelineEvent);
    async fn list_events(&self, pipeline_id: PipelineId) -> Vec<PipelineEvent>;
}

#[async_trait]
pub trait CheckpointMetadataStore: Send + Sync {
    async fn put_checkpoint(&self, pipeline_id: PipelineId, meta: CheckpointMetadata);
    async fn list_checkpoints(&self, pipeline_id: PipelineId) -> Vec<CheckpointMetadata>;
    async fn latest_checkpoint(&self, pipeline_id: PipelineId) -> Option<CheckpointMetadata>;
}

#[async_trait]
pub trait PipelineSnapshotStore: Send + Sync {
    async fn append_snapshot(&self, pipeline_id: PipelineId, entry: PipelineSnapshotEntry);
    async fn list_snapshots(&self, pipeline_id: PipelineId) -> Vec<PipelineSnapshotEntry>;
    async fn latest_snapshot(&self, pipeline_id: PipelineId) -> Option<PipelineSnapshotEntry>;
}

#[derive(Debug, Default)]
struct InMemoryInner {
    specs: HashMap<PipelineSpecId, PipelineSpec>,
    runs: HashMap<PipelineId, PipelineRun>,
    desired: HashMap<PipelineId, PipelineDesiredState>,
    status: HashMap<PipelineId, PipelineStatus>,
    events: HashMap<PipelineId, Vec<PipelineEvent>>,
    checkpoints: HashMap<PipelineId, Vec<CheckpointMetadata>>,
    execution_ids_by_pipeline: HashMap<PipelineId, ExecutionIds>,
    snapshots: HashMap<PipelineId, Vec<PipelineSnapshotEntry>>,
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryStore {
    inner: Arc<RwLock<InMemoryInner>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn put_execution_ids(&self, pipeline_id: PipelineId, execution_ids: ExecutionIds) {
        let mut inner = self.inner.write().await;
        inner.execution_ids_by_pipeline.insert(pipeline_id, execution_ids);
    }

    pub async fn get_execution_ids(&self, pipeline_id: PipelineId) -> Option<ExecutionIds> {
        let inner = self.inner.read().await;
        inner.execution_ids_by_pipeline.get(&pipeline_id).cloned()
    }
}

#[async_trait]
impl PipelineSpecStore for InMemoryStore {
    async fn put_spec(&self, spec: PipelineSpec) {
        let mut guard = self.inner.write().await;
        guard.specs.insert(spec.pipeline_spec_id, spec);
    }

    async fn get_spec(&self, pipeline_spec_id: PipelineSpecId) -> Option<PipelineSpec> {
        let guard = self.inner.read().await;
        guard.specs.get(&pipeline_spec_id).cloned()
    }

    async fn list_specs(&self) -> Vec<PipelineSpec> {
        let guard = self.inner.read().await;
        guard.specs.values().cloned().collect()
    }
}

#[async_trait]
impl PipelineRunStore for InMemoryStore {
    async fn put_run(&self, run: PipelineRun) {
        let mut guard = self.inner.write().await;
        guard.runs.insert(run.execution_ids.pipeline_id, run);
    }

    async fn get_run(&self, pipeline_id: PipelineId) -> Option<PipelineRun> {
        let guard = self.inner.read().await;
        guard.runs.get(&pipeline_id).cloned()
    }

    async fn list_runs(&self) -> Vec<PipelineRun> {
        let guard = self.inner.read().await;
        guard.runs.values().cloned().collect()
    }

    async fn set_desired_state(&self, pipeline_id: PipelineId, desired: PipelineDesiredState) {
        let mut guard = self.inner.write().await;
        guard.desired.insert(pipeline_id, desired);
    }

    async fn get_desired_state(&self, pipeline_id: PipelineId) -> Option<PipelineDesiredState> {
        let guard = self.inner.read().await;
        guard.desired.get(&pipeline_id).cloned()
    }

    async fn put_status(&self, pipeline_id: PipelineId, status: PipelineStatus) {
        let mut guard = self.inner.write().await;
        guard.status.insert(pipeline_id, status);
    }

    async fn get_status(&self, pipeline_id: PipelineId) -> Option<PipelineStatus> {
        let guard = self.inner.read().await;
        guard.status.get(&pipeline_id).cloned()
    }
}

#[async_trait]
impl PipelineEventStore for InMemoryStore {
    async fn append_event(&self, pipeline_id: PipelineId, event: PipelineEvent) {
        let mut guard = self.inner.write().await;
        guard.events.entry(pipeline_id).or_default().push(event);
    }

    async fn list_events(&self, pipeline_id: PipelineId) -> Vec<PipelineEvent> {
        let guard = self.inner.read().await;
        guard.events.get(&pipeline_id).cloned().unwrap_or_default()
    }
}

#[async_trait]
impl CheckpointMetadataStore for InMemoryStore {
    async fn put_checkpoint(&self, pipeline_id: PipelineId, meta: CheckpointMetadata) {
        let mut guard = self.inner.write().await;
        guard.checkpoints.entry(pipeline_id).or_default().push(meta);
    }

    async fn list_checkpoints(&self, pipeline_id: PipelineId) -> Vec<CheckpointMetadata> {
        let guard = self.inner.read().await;
        guard.checkpoints.get(&pipeline_id).cloned().unwrap_or_default()
    }

    async fn latest_checkpoint(&self, pipeline_id: PipelineId) -> Option<CheckpointMetadata> {
        let guard = self.inner.read().await;
        guard.checkpoints.get(&pipeline_id).and_then(|v| v.last().cloned())
    }
}

#[async_trait]
impl PipelineSnapshotStore for InMemoryStore {
    async fn append_snapshot(&self, pipeline_id: PipelineId, entry: PipelineSnapshotEntry) {
        let mut guard = self.inner.write().await;
        let retention_ms = guard
            .execution_ids_by_pipeline
            .get(&pipeline_id)
            .and_then(|ids| guard.specs.get(&ids.pipeline_spec_id))
            .and_then(|stored| serde_json::from_slice::<UserPipelineSpec>(&stored.spec_bytes).ok())
            .map(|spec| spec.worker_runtime.history_retention_window().as_millis() as u64)
            .unwrap_or(10 * 60 * 1000);

        let min_ts_ms = entry.ts_ms.saturating_sub(retention_ms.max(1));
        let snaps = guard.snapshots.entry(pipeline_id).or_default();
        snaps.retain(|e| e.ts_ms >= min_ts_ms);
        snaps.push(entry);
    }

    async fn list_snapshots(&self, pipeline_id: PipelineId) -> Vec<PipelineSnapshotEntry> {
        let guard = self.inner.read().await;
        guard.snapshots.get(&pipeline_id).cloned().unwrap_or_default()
    }

    async fn latest_snapshot(&self, pipeline_id: PipelineId) -> Option<PipelineSnapshotEntry> {
        let guard = self.inner.read().await;
        guard.snapshots.get(&pipeline_id).and_then(|v| v.last().cloned())
    }
}

