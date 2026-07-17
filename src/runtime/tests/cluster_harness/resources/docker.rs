use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use uuid::Uuid;

use crate::common::test_utils::gen_unique_grpc_port;
use crate::runtime::master::server::master_service::master_service_client::MasterServiceClient;
use crate::runtime::master::server::master_service::GetLatestPipelineSnapshotRequest;
use crate::runtime::master::server::master_service::GetLifecycleEventsRequest;
use crate::runtime::master::LifecycleEvent;
use crate::runtime::master::LifecycleEventRecord;
use crate::runtime::observability::{PipelineSnapshot, StreamTaskStatus};
use crate::runtime::tests::cluster_harness::backend::ClusterBackend;
use crate::runtime::tests::cluster_harness::PipelineLaunchSpec;
use crate::runtime::tests::cluster_harness::FaultAction;
use crate::storage::InMemoryStorageSnapshot;
use crate::storage::InMemoryStorageClient;
use async_trait::async_trait;

struct DockerResources {
    compose_project: String,
    compose_file: PathBuf,
    compose_env_file: PathBuf,
    spec_file: PathBuf,
    master_port: u16,
    storage_port: u16,
}

impl DockerResources {
    fn start(spec_json: &str, worker_count: usize) -> Result<Self> {
        const MASTER_CONTAINER_PORT: u16 = 50051;
        const WORKER_CONTROL_PORT: u16 = 50052;
        const WORKER_TRANSPORT_PORT: u16 = 60052;
        const STORAGE_CONTAINER_PORT: u16 = 50071;

        let master_port = gen_unique_grpc_port();
        let storage_port = gen_unique_grpc_port();
        let compose_project = format!("volga-harness-{}", Uuid::new_v4().simple());
        let compose_file =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docker-compose.test.yaml");
        let compose_env_file = std::env::temp_dir().join(format!("{compose_project}-compose.env"));
        let spec_file = std::env::temp_dir().join(format!("{compose_project}-pipeline-spec.json"));
        fs::write(&spec_file, spec_json).context("failed to write pipeline spec")?;
        fs::write(
            &compose_env_file,
            format!(
                "MASTER_PORT={master_port}\n\
                 MASTER_CONTAINER_PORT={MASTER_CONTAINER_PORT}\n\
                 SPEC_FILE={}\n\
                 PIPELINE_ID={}\n\
                 WORKER_COUNT={worker_count}\n\
                 WORKER_HOST_PREFIX=worker-\n\
                 WORKER_CONTROL_PORT={WORKER_CONTROL_PORT}\n\
                 WORKER_TRANSPORT_PORT={WORKER_TRANSPORT_PORT}\n\
                 STORAGE_PORT={storage_port}\n\
                 STORAGE_CONTAINER_PORT={STORAGE_CONTAINER_PORT}\n",
                spec_file.display(),
                Uuid::new_v4(),
            ),
        )
        .context("failed to write compose env")?;

        let resources = Self {
            compose_project,
            compose_file,
            compose_env_file,
            spec_file,
            master_port,
            storage_port,
        };
        resources.compose(&["up", "-d"])?;
        Ok(resources)
    }

    fn compose(&self, args: &[&str]) -> Result<()> {
        let output = Command::new("docker")
            .args([
                "compose",
                "-f",
                self.compose_file.to_str().unwrap(),
                "--env-file",
                self.compose_env_file.to_str().unwrap(),
                "-p",
                &self.compose_project,
            ])
            .args(args)
            .output()
            .context("failed to run docker compose")?;
        if !output.status.success() {
            return Err(anyhow!(
                "docker compose {:?} failed: {}",
                args,
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        Ok(())
    }

    fn stop(self) {
        let _ = self.compose(&["down", "--volumes", "--remove-orphans"]);
        let _ = fs::remove_file(self.compose_env_file);
        let _ = fs::remove_file(self.spec_file);
    }
}

pub struct DockerClusterResources {
    resources: Option<DockerResources>,
    expected_workers: usize,
}

pub struct DockerCluster {
    resources: Option<DockerClusterResources>,
}

impl DockerCluster {
    pub fn new() -> Self {
        Self { resources: None }
    }
}

#[async_trait]
impl ClusterBackend for DockerCluster {
    async fn launch(&mut self, launch: PipelineLaunchSpec) -> Result<Vec<String>> {
        let resources = DockerClusterResources::launch(launch)?;
        let worker_ids = resources.worker_ids();
        self.resources = Some(resources);
        Ok(worker_ids)
    }

    async fn start_execution(&mut self) -> Result<()> {
        Ok(())
    }

    async fn wait_for_completion(&mut self) -> Result<()> {
        self.resources
            .as_mut()
            .context("docker cluster is not launched")?
            .wait_for_completion()
            .await
    }

    async fn shutdown(&mut self) -> Result<()> {
        if let Some(resources) = self.resources.as_mut() {
            resources.shutdown();
        }
        self.resources = None;
        Ok(())
    }

    async fn storage_snapshot(&mut self) -> Result<InMemoryStorageSnapshot> {
        self.resources
            .as_ref()
            .context("docker cluster is not launched")?
            .storage_snapshot()
            .await
    }

    async fn lifecycle_events_since(
        &mut self,
        sequence: u64,
    ) -> Result<Vec<LifecycleEventRecord>> {
        let resources = self
            .resources
            .as_ref()
            .context("docker cluster is not launched")?
            .resources()?;
        fetch_lifecycle_events(resources.master_port, sequence).await
    }

    async fn latest_pipeline_snapshot(
        &mut self,
    ) -> Result<Option<crate::runtime::observability::PipelineSnapshot>> {
        let port = self
            .resources
            .as_ref()
            .context("docker cluster is not launched")?
            .resources()?
            .master_port;
        docker_master_latest_pipeline_snapshot(port).await
    }

    async fn apply_fault(&mut self, fault: FaultAction) -> Result<()> {
        let resources = self.resources.as_ref().context("docker cluster is not launched")?;
        match fault {
            FaultAction::KillWorker { worker_id, mode: _ } => resources.kill(&worker_id),
            FaultAction::RestartWorker { worker_id } => resources.restart(&worker_id),
            FaultAction::KillMaster => resources.kill("master"),
            FaultAction::RestartMaster => resources.restart("master"),
        }
    }
}

impl DockerClusterResources {
    pub fn launch(launch: PipelineLaunchSpec) -> Result<Self> {
        let mut spec = launch.pipeline;
        super::install_in_memory_sink(&mut spec, "http://storage:50071");
        Ok(Self {
            resources: Some(DockerResources::start(
                &serde_json::to_string(&spec)?,
                launch.worker_count,
            )?),
            expected_workers: launch.worker_count,
        })
    }

    pub fn worker_ids(&self) -> Vec<String> {
        (0..self.expected_workers)
            .map(|index| format!("worker-{index}"))
            .collect()
    }

    pub async fn wait_for_completion(&mut self) -> Result<()> {
        let resources = self.resources()?;
        let mut master = connect_master(resources.master_port).await?;
        let start = tokio::time::Instant::now();
        loop {
            let response = master
                .get_latest_pipeline_snapshot(tonic::Request::new(
                    GetLatestPipelineSnapshotRequest {},
                ))
                .await?
                .into_inner();
            if response.has_snapshot {
                let snapshot: PipelineSnapshot = bincode::deserialize(&response.snapshot_bytes)?;
                let all_workers_have_tasks = snapshot
                    .worker_states
                    .values()
                    .all(|worker| !worker.task_statuses.is_empty());
                let all_done = snapshot.worker_states.values().all(|worker| {
                    worker.task_statuses.values().all(|status| {
                        matches!(
                            status,
                            StreamTaskStatus::Finished | StreamTaskStatus::Closed
                        )
                    })
                });
                if all_workers_have_tasks
                    && all_done
                    && snapshot.worker_states.len() == self.expected_workers
                {
                    break;
                }
            }
            if start.elapsed() > Duration::from_secs(20) {
                return Err(anyhow!("timeout waiting for docker pipeline"));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Ok(())
    }

    pub async fn storage_snapshot(&self) -> Result<InMemoryStorageSnapshot> {
        let endpoint = format!("http://127.0.0.1:{}", self.resources()?.storage_port);
        InMemoryStorageClient::new(endpoint).await?.snapshot().await
    }

    pub fn kill(&self, service: &str) -> Result<()> {
        self.resources()?.compose(&["kill", service])
    }

    pub fn restart(&self, service: &str) -> Result<()> {
        self.resources()?.compose(&["restart", service])
    }

    pub fn shutdown(&mut self) {
        if let Some(resources) = self.resources.take() {
            resources.stop();
        }
    }

    fn resources(&self) -> Result<&DockerResources> {
        self.resources
            .as_ref()
            .context("docker resources are stopped")
    }
}

async fn connect_master(port: u16) -> Result<MasterServiceClient<tonic::transport::Channel>> {
    let endpoint = format!("http://127.0.0.1:{port}");
    let start = tokio::time::Instant::now();
    loop {
        match MasterServiceClient::connect(endpoint.clone()).await {
            Ok(client) => return Ok(client),
            Err(error) if start.elapsed() > Duration::from_secs(20) => {
                return Err(anyhow!("failed to connect to master: {error}"));
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
}

async fn fetch_lifecycle_events(
    port: u16,
    sequence: u64,
) -> Result<Vec<LifecycleEventRecord>> {
    let mut client = connect_master(port).await?;
    client
        .get_lifecycle_events(tonic::Request::new(GetLifecycleEventsRequest {
            after_sequence: sequence,
        }))
        .await?
        .into_inner()
        .events
        .into_iter()
        .map(|record| {
            let event: LifecycleEvent = bincode::deserialize(&record.event_bytes)
                .with_context(|| {
                    format!(
                        "failed to decode lifecycle event sequence={} bytes={}",
                        record.sequence,
                        record.event_bytes.len()
                    )
                })?;
            Ok(LifecycleEventRecord {
                sequence: record.sequence,
                event,
            })
        })
        .collect()
}

async fn docker_master_latest_pipeline_snapshot(
    port: u16,
) -> Result<Option<crate::runtime::observability::PipelineSnapshot>> {
    let mut client = connect_master(port).await?;
    let response = client
        .get_latest_pipeline_snapshot(tonic::Request::new(
            crate::runtime::master::server::master_service::GetLatestPipelineSnapshotRequest {},
        ))
        .await?
        .into_inner();
    if !response.has_snapshot {
        return Ok(None);
    }
    Ok(Some(bincode::deserialize(&response.snapshot_bytes)?))
}
