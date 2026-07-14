use std::sync::Arc;

use anyhow::{anyhow, Result};
use uuid::Uuid;

use super::common::{LocalMaster, LocalStorage, LocalWorkerPool, WorkerServerSlot};
use crate::api::compile_logical_graph;
use crate::api::spec::connectors::SinkSpec;
use crate::orchestrator::local::{LocalTestOrchestrator, LocalWorkerOrchestrator};
use crate::orchestrator::orchestrator::{MasterOrchestrator, WorkerOrchestrator};
use crate::runtime::master::MasterConfig;
use crate::runtime::master::LifecycleEventRecord;
use crate::storage::InMemoryStorageSnapshot;
use crate::runtime::tests::cluster_harness::backend::ClusterBackend;
use crate::runtime::tests::cluster_harness::{FaultAction, PipelineLaunchSpec, WorkerKillMode};
use async_trait::async_trait;
use anyhow::Context;

pub struct LocalClusterResources {
    storage: LocalStorage,
    master: LocalMaster,
    workers: Arc<LocalWorkerPool>,
    worker_ids: Vec<String>,
    execution: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
}

pub struct LocalCluster {
    resources: Option<LocalClusterResources>,
}

impl LocalCluster {
    pub fn new() -> Self {
        Self { resources: None }
    }
}

#[async_trait]
impl ClusterBackend for LocalCluster {
    async fn launch(&mut self, launch: PipelineLaunchSpec) -> Result<Vec<String>> {
        let resources = LocalClusterResources::launch(launch).await?;
        let worker_ids = resources.worker_ids();
        self.resources = Some(resources);
        Ok(worker_ids)
    }

    async fn start_execution(&mut self) -> Result<()> {
        self.resources
            .as_mut()
            .context("local cluster is not launched")?
            .start_execution()
    }

    async fn wait_for_completion(&mut self) -> Result<()> {
        self.resources
            .as_mut()
            .context("local cluster is not launched")?
            .wait_for_completion()
            .await
    }

    async fn shutdown(&mut self) -> Result<()> {
        if let Some(resources) = self.resources.as_mut() {
            resources.shutdown().await;
        }
        self.resources = None;
        Ok(())
    }

    async fn storage_snapshot(&mut self) -> Result<InMemoryStorageSnapshot> {
        self.resources
            .as_ref()
            .context("local cluster is not launched")?
            .storage_snapshot()
            .await
    }

    async fn lifecycle_events_since(
        &mut self,
        sequence: u64,
    ) -> Result<Vec<LifecycleEventRecord>> {
        Ok(self
            .resources
            .as_ref()
            .context("local cluster is not launched")?
            .master
            .server
            .lifecycle_events_since(sequence)
            .await)
    }

    async fn apply_fault(&mut self, fault: FaultAction) -> Result<()> {
        let resources = self.resources.as_mut().context("local cluster is not launched")?;
        match fault {
            FaultAction::KillWorker { worker_id, mode } => {
                resources.kill_worker(&worker_id, mode).await
            }
            FaultAction::RestartWorker { worker_id } => resources.restart_worker(&worker_id).await,
            FaultAction::KillMaster => {
                resources.kill_master().await;
                Ok(())
            }
            fault => Err(anyhow!("fault {fault:?} is not supported locally")),
        }
    }
}

impl LocalClusterResources {
    pub async fn launch(launch: PipelineLaunchSpec) -> Result<Self> {
        let storage = LocalStorage::start().await?;
        let mut spec = launch.pipeline;
        spec.sink = Some(SinkSpec::InMemoryStorageGrpc {
            server_addr: storage.endpoint(),
        });
        let logical_graph = compile_logical_graph(&spec, None);
        let local_orchestrator = LocalTestOrchestrator::new(
            launch.worker_count,
            Uuid::new_v4().to_string(),
        )
        .with_spec(spec.clone());
        let worker_nodes = local_orchestrator.get_worker_nodes().await;
        let expected_workers = local_orchestrator.get_num_expected_workers().await;
        let master_addr = format!("127.0.0.1:{}", crate::common::test_utils::gen_unique_grpc_port());
        let worker_orchestrator: Arc<dyn WorkerOrchestrator> =
            Arc::new(LocalWorkerOrchestrator::new(master_addr.clone()));
        let workers = worker_nodes
            .values()
            .map(|node| WorkerServerSlot::new(
                node.worker_id.clone(),
                format!("{}:{}", node.worker_ip, node.worker_port),
            ))
            .collect::<Vec<_>>();
        let worker_ids = workers.iter().map(|worker| worker.id.clone()).collect();
        let workers = Arc::new(LocalWorkerPool::new(workers, worker_orchestrator));
        let master_orchestrator: Arc<dyn MasterOrchestrator> = Arc::new(
            local_orchestrator.with_replacement(workers.clone()),
        );
        let mut master = LocalMaster::new(master_addr, master_orchestrator);
        master
            .server
            .configure(MasterConfig::with_spec(
                spec,
                logical_graph.to_execution_graph(),
                expected_workers,
            ))
            .await;
        master.server.start(&master.addr).await?;

        workers.start_all().await?;
        Ok(Self {
            storage,
            master,
            workers,
            worker_ids,
            execution: None,
        })
    }

    pub fn worker_ids(&self) -> Vec<String> {
        self.worker_ids.clone()
    }

    pub fn start_execution(&mut self) -> Result<()> {
        if self.execution.is_some() {
            return Err(anyhow!("local execution is already running"));
        }
        let master = self.master.server.master();
        self.execution = Some(tokio::spawn(async move { master.execute().await }));
        Ok(())
    }

    pub async fn wait_for_completion(&mut self) -> Result<()> {
        self.execution
            .take()
            .ok_or_else(|| anyhow!("local execution was not started"))?
            .await
            .map_err(anyhow::Error::from)?
    }

    pub async fn storage_snapshot(&self) -> Result<InMemoryStorageSnapshot> {
        self.storage.snapshot().await
    }

    pub async fn kill_worker(&mut self, worker_id: &str, mode: WorkerKillMode) -> Result<()> {
        self.workers.crash(worker_id, mode).await
    }

    pub async fn restart_worker(&mut self, worker_id: &str) -> Result<()> {
        self.workers.restart(worker_id).await
    }

    pub async fn kill_master(&mut self) {
        self.master.stop().await;
    }

    pub async fn shutdown(&mut self) {
        if let Some(execution) = self.execution.take() {
            execution.abort();
        }
        self.workers.stop_all().await;
        self.master.stop().await;
        self.storage.stop().await;
    }
}
