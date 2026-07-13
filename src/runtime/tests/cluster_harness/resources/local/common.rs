use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use crate::common::test_utils::gen_unique_grpc_port;
use crate::orchestrator::local::LocalWorkerReplacement;
use crate::orchestrator::orchestrator::{MasterOrchestrator, WorkerOrchestrator};
use crate::runtime::master::server::MasterServer;
use crate::runtime::worker_server::WorkerServer;
use crate::storage::{InMemoryStorageClient, InMemoryStorageServer, InMemoryStorageSnapshot};
use tokio::sync::Mutex;

pub(super) struct LocalStorage {
    pub(super) addr: String,
    server: InMemoryStorageServer,
}

impl LocalStorage {
    pub(super) async fn start() -> Result<Self> {
        let addr = format!("127.0.0.1:{}", gen_unique_grpc_port());
        let mut server = InMemoryStorageServer::new();
        server.start(&addr).await?;
        Ok(Self { addr, server })
    }

    pub(super) fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub(super) async fn snapshot(&self) -> Result<InMemoryStorageSnapshot> {
        InMemoryStorageClient::new(self.endpoint())
            .await?
            .snapshot()
            .await
    }

    pub(super) async fn stop(&mut self) {
        self.server.stop().await;
    }

}

pub(super) struct LocalMaster {
    pub(super) addr: String,
    pub(super) server: MasterServer,
}

impl LocalMaster {
    pub(super) fn new(addr: String, orchestrator: Arc<dyn MasterOrchestrator>) -> Self {
        Self {
            addr,
            server: MasterServer::new(orchestrator),
        }
    }

    pub(super) async fn stop(&mut self) {
        self.server.stop().await;
    }
}

pub(super) struct WorkerServerSlot {
    pub(super) id: String,
    pub(super) addr: String,
    pub(super) server: Option<WorkerServer>,
}

impl WorkerServerSlot {
    pub(super) async fn start(
        &mut self,
        orchestrator: Arc<dyn WorkerOrchestrator>,
    ) -> Result<()> {
        let mut server = WorkerServer::new(self.id.clone(), orchestrator);
        server.start(&self.addr).await?;
        server.register_with_master().await?;
        self.server = Some(server);
        Ok(())
    }

    pub(super) async fn stop(&mut self) {
        if let Some(mut server) = self.server.take() {
            server.stop().await;
        }
    }

    pub(super) async fn crash(&mut self) {
        if let Some(mut server) = self.server.take() {
            server.crash_for_testing().await;
        }
    }
}

pub(super) struct LocalWorkerPool {
    workers: Mutex<Vec<WorkerServerSlot>>,
    worker_orchestrator: Arc<dyn WorkerOrchestrator>,
}

impl LocalWorkerPool {
    pub(super) fn new(
        workers: Vec<WorkerServerSlot>,
        worker_orchestrator: Arc<dyn WorkerOrchestrator>,
    ) -> Self {
        Self {
            workers: Mutex::new(workers),
            worker_orchestrator,
        }
    }

    pub(super) async fn start_all(&self) -> Result<()> {
        let mut workers = self.workers.lock().await;
        for worker in workers.iter_mut() {
            worker.start(self.worker_orchestrator.clone()).await?;
        }
        Ok(())
    }

    pub(super) async fn stop_all(&self) {
        let mut workers = self.workers.lock().await;
        for worker in workers.iter_mut() {
            worker.stop().await;
        }
    }

    pub(super) async fn crash(&self, worker_id: &str) -> Result<()> {
        let mut workers = self.workers.lock().await;
        self.worker_mut(&mut workers, worker_id)?.crash().await;
        Ok(())
    }

    pub(super) async fn restart(&self, worker_id: &str) -> Result<()> {
        let mut workers = self.workers.lock().await;
        let worker = self.worker_mut(&mut workers, worker_id)?;
        worker.stop().await;
        worker.start(self.worker_orchestrator.clone()).await
    }

    fn worker_mut<'a>(
        &self,
        workers: &'a mut [WorkerServerSlot],
        worker_id: &str,
    ) -> Result<&'a mut WorkerServerSlot> {
        workers
            .iter_mut()
            .find(|worker| worker.id == worker_id)
            .ok_or_else(|| anyhow::anyhow!("unknown local worker {worker_id}"))
    }
}

#[async_trait]
impl LocalWorkerReplacement for LocalWorkerPool {
    async fn replace_workers(&self, worker_ids: &[String]) -> Result<()> {
        for worker_id in worker_ids {
            self.restart(worker_id).await?;
        }
        Ok(())
    }
}
