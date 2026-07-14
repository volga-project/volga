use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::{oneshot, Mutex};

use crate::common::test_utils::gen_unique_grpc_port;
use crate::orchestrator::local::LocalWorkerReplacement;
use crate::orchestrator::orchestrator::{MasterOrchestrator, WorkerOrchestrator};
use crate::runtime::master::server::MasterServer;
use crate::runtime::tests::cluster_harness::WorkerKillMode;
use crate::runtime::worker_server::WorkerServer;
use crate::storage::{InMemoryStorageClient, InMemoryStorageServer, InMemoryStorageSnapshot};

const WORKER_TASK_STOP_TIMEOUT: Duration = Duration::from_secs(10);
const WORKER_CRASH_TIMEOUT: Duration = Duration::from_secs(5);

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
    process: Option<JoinHandle<()>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    crash_tx: Option<oneshot::Sender<bool>>,
}

impl WorkerServerSlot {
    pub(super) fn new(id: String, addr: String) -> Self {
        Self {
            id,
            addr,
            process: None,
            shutdown_tx: None,
            crash_tx: None,
        }
    }

    pub(super) async fn start(
        &mut self,
        orchestrator: Arc<dyn WorkerOrchestrator>,
    ) -> Result<()> {
        if self.process.is_some() {
            return Ok(());
        }
        let (process, shutdown_tx, crash_tx) =
            spawn_worker_thread(self.id.clone(), self.addr.clone(), orchestrator)?;
        self.process = Some(process);
        self.shutdown_tx = Some(shutdown_tx);
        self.crash_tx = Some(crash_tx);
        Ok(())
    }

    pub(super) async fn stop(&mut self) {
        if self.process.is_none() {
            return;
        }
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.crash_tx.take();
        if let Some(handle) = self.process.take() {
            join_thread(handle, WORKER_TASK_STOP_TIMEOUT).await;
        }
    }

    pub(super) async fn crash(&mut self, mode: WorkerKillMode) {
        if self.process.is_none() {
            return;
        }
        if let Some(tx) = self.crash_tx.take() {
            let inject_panic = matches!(mode, WorkerKillMode::Panic);
            let _ = tx.send(inject_panic);
        }
        self.shutdown_tx.take();
        if let Some(handle) = self.process.take() {
            join_thread(handle, WORKER_CRASH_TIMEOUT).await;
        }
    }
}

async fn join_thread(handle: JoinHandle<()>, timeout: Duration) {
    let join = tokio::task::spawn_blocking(move || {
        let _ = handle.join();
    });
    let _ = tokio::time::timeout(timeout, join).await;
}

fn spawn_worker_thread(
    worker_id: String,
    addr: String,
    orchestrator: Arc<dyn WorkerOrchestrator>,
) -> Result<(JoinHandle<()>, oneshot::Sender<()>, oneshot::Sender<bool>)> {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (crash_tx, crash_rx) = oneshot::channel();
    let thread_name = format!("volga-worker-{worker_id}");
    let handle = std::thread::Builder::new()
        .name(thread_name.clone())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name(format!("{thread_name}-rt"))
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    eprintln!("[WORKER_SERVER] failed to build runtime for {worker_id}: {error}");
                    return;
                }
            };
            runtime.block_on(run_worker_process(
                worker_id,
                addr,
                orchestrator,
                shutdown_rx,
                crash_rx,
            ));
        })?;
    Ok((handle, shutdown_tx, crash_tx))
}

async fn run_worker_process(
    worker_id: String,
    addr: String,
    orchestrator: Arc<dyn WorkerOrchestrator>,
    shutdown_rx: oneshot::Receiver<()>,
    crash_rx: oneshot::Receiver<bool>,
) {
    let mut server = WorkerServer::new(worker_id, orchestrator);
    if let Err(error) = server.start(&addr).await {
        eprintln!("[WORKER_SERVER] failed to start on {addr}: {error}");
        return;
    }
    if let Err(error) = server.register_with_master().await {
        eprintln!("[WORKER_SERVER] failed to register on {addr}: {error}");
        return;
    }
    server.run_until_stopped(shutdown_rx, crash_rx).await;
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

    pub(super) async fn crash(&self, worker_id: &str, mode: WorkerKillMode) -> Result<()> {
        let mut workers = self.workers.lock().await;
        self.worker_mut(&mut workers, worker_id)?.crash(mode).await;
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
