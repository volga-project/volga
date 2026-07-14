use std::sync::Arc;

use anyhow::Result;

use super::cluster::ClusterInner;
use super::{FaultAction, WorkerKillMode};
use crate::runtime::master::LifecycleEventRecord;
use crate::storage::InMemoryStorageSnapshot;

#[derive(Clone)]
pub struct MasterHandle {
    inner: Arc<ClusterInner>,
}

impl MasterHandle {
    pub(crate) fn new(inner: Arc<ClusterInner>) -> Self {
        Self { inner }
    }

    pub async fn kill(&self) -> Result<()> {
        self.inner
            .backend
            .lock()
            .await
            .apply_fault(FaultAction::KillMaster)
            .await
    }

    pub async fn restart(&self) -> Result<()> {
        self.inner
            .backend
            .lock()
            .await
            .apply_fault(FaultAction::RestartMaster)
            .await
    }

    pub async fn lifecycle_events_since(
        &self,
        sequence: u64,
    ) -> Result<Vec<LifecycleEventRecord>> {
        self.inner
            .backend
            .lock()
            .await
            .lifecycle_events_since(sequence)
            .await
    }
}

#[derive(Clone)]
pub struct WorkerHandle {
    worker_id: String,
    inner: Arc<ClusterInner>,
}

impl WorkerHandle {
    pub(crate) fn new(worker_id: String, inner: Arc<ClusterInner>) -> Self {
        Self { worker_id, inner }
    }

    pub fn id(&self) -> &str {
        self.worker_id.as_str()
    }

    pub async fn kill(&self) -> Result<()> {
        self.kill_with(WorkerKillMode::Abrupt).await
    }

    pub async fn kill_with(&self, mode: WorkerKillMode) -> Result<()> {
        self.inner
            .backend
            .lock()
            .await
            .apply_fault(FaultAction::KillWorker {
                worker_id: self.worker_id.clone(),
                mode,
            })
            .await
    }

    pub async fn restart(&self) -> Result<()> {
        self.inner
            .backend
            .lock()
            .await
            .apply_fault(FaultAction::RestartWorker {
                worker_id: self.worker_id.clone(),
            })
            .await
    }

}

#[derive(Clone)]
pub struct StorageHandle {
    inner: Arc<ClusterInner>,
}

impl StorageHandle {
    pub(crate) fn new(inner: Arc<ClusterInner>) -> Self {
        Self { inner }
    }

    pub async fn snapshot(&self) -> Result<InMemoryStorageSnapshot> {
        self.inner.backend.lock().await.storage_snapshot().await
    }
}
