use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::Mutex;

use super::handles::{MasterHandle, StorageHandle, WorkerHandle};
use super::backend::ClusterBackend;
use super::{DockerCluster, KubeCluster, LocalCluster, PipelineLaunchSpec, RuntimeEnv};

pub(crate) struct ClusterInner {
    pub(crate) backend: Mutex<Box<dyn ClusterBackend>>,
}

#[derive(Clone)]
pub struct TestCluster {
    inner: Arc<ClusterInner>,
    worker_ids: BTreeSet<String>,
}

impl TestCluster {
    pub async fn launch(env: RuntimeEnv, launch: PipelineLaunchSpec) -> Result<Self> {
        let mut backend: Box<dyn ClusterBackend> = match env {
            RuntimeEnv::Local => Box::new(LocalCluster::new()),
            RuntimeEnv::Docker => Box::new(DockerCluster::new()),
            RuntimeEnv::Kube => Box::new(KubeCluster::new()),
        };
        let worker_ids = backend.launch(launch).await?.into_iter().collect();
        Ok(Self {
            inner: Arc::new(ClusterInner {
                backend: Mutex::new(backend),
            }),
            worker_ids,
        })
    }

    pub fn master(&self) -> MasterHandle {
        MasterHandle::new(self.inner.clone())
    }

    pub fn worker(&self, worker_id: &str) -> Option<WorkerHandle> {
        self.worker_ids
            .contains(worker_id)
            .then(|| WorkerHandle::new(worker_id.to_owned(), self.inner.clone()))
    }

    pub fn worker_ids(&self) -> Vec<String> {
        self.worker_ids.iter().cloned().collect()
    }

    pub fn storage(&self) -> StorageHandle {
        StorageHandle::new(self.inner.clone())
    }

    pub async fn start_execution(&self) -> Result<()> {
        self.inner.backend.lock().await.start_execution().await
    }

    pub async fn wait_for_completion(&self) -> Result<()> {
        self.inner.backend.lock().await.wait_for_completion().await
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.inner.backend.lock().await.shutdown().await
    }
}
