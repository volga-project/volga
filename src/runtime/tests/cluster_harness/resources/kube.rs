use std::fs;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use uuid::Uuid;

use crate::api::PipelineSpec;
use crate::common::test_utils::gen_unique_grpc_port;
use crate::runtime::tests::cluster_harness::PipelineLaunchSpec;
use crate::runtime::tests::cluster_harness::backend::ClusterBackend;
use crate::runtime::tests::cluster_harness::FaultAction;
use crate::storage::InMemoryStorageSnapshot;
use crate::runtime::master::LifecycleEvent;
use crate::runtime::master::LifecycleEventRecord;
use crate::runtime::master::server::master_service::master_service_client::MasterServiceClient;
use crate::runtime::master::server::master_service::GetLifecycleEventsRequest;
use crate::storage::InMemoryStorageClient;
use async_trait::async_trait;

struct KubeResources {
    pipeline_name: String,
    manifest_path: PathBuf,
    storage_port_forward: Child,
    master_port_forward: Child,
    master_port: u16,
}

impl KubeResources {
    fn stop(mut self) {
        let _ = kubectl(&[
            "-n",
            "default",
            "delete",
            "volgapipeline",
            &self.pipeline_name,
            "--ignore-not-found=true",
        ]);
        let _ = fs::remove_file(self.manifest_path);
        let _ = self.storage_port_forward.kill();
        let _ = self.master_port_forward.kill();
    }
}

pub struct KubeClusterResources {
    resources: Option<KubeResources>,
    storage_endpoint: String,
    expected_output_rows: usize,
    worker_ids: Vec<String>,
}

pub struct KubeCluster {
    resources: Option<KubeClusterResources>,
}

impl KubeCluster {
    pub fn new() -> Self {
        Self { resources: None }
    }
}

#[async_trait]
impl ClusterBackend for KubeCluster {
    async fn launch(&mut self, launch: PipelineLaunchSpec) -> Result<Vec<String>> {
        let resources = KubeClusterResources::launch(launch)?;
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
            .context("kube cluster is not launched")?
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
            .context("kube cluster is not launched")?
            .storage_snapshot()
            .await
    }

    async fn lifecycle_events_since(
        &mut self,
        sequence: u64,
    ) -> Result<Vec<LifecycleEventRecord>> {
        let master_port = self
            .resources
            .as_ref()
            .context("kube cluster is not launched")?
            .resources()?
            .master_port;
        fetch_lifecycle_events(master_port, sequence).await
    }

    async fn latest_pipeline_snapshot(
        &mut self,
    ) -> Result<Option<crate::runtime::observability::PipelineSnapshot>> {
        let master_port = self
            .resources
            .as_ref()
            .context("kube cluster is not launched")?
            .resources()?
            .master_port;
        master_latest_pipeline_snapshot(master_port).await
    }

    async fn stop_sources(&mut self) -> Result<()> {
        let master_port = self
            .resources
            .as_ref()
            .context("kube cluster is not launched")?
            .resources()?
            .master_port;
        master_stop_sources(master_port).await
    }

    async fn apply_fault(&mut self, fault: FaultAction) -> Result<()> {
        let resources = self.resources.as_ref().context("kube cluster is not launched")?;
        match fault {
            FaultAction::KillWorker { worker_id, mode: _ }
            | FaultAction::RestartWorker { worker_id } => resources.delete_worker_pod(&worker_id),
            FaultAction::KillMaster | FaultAction::RestartMaster => resources.kill_master(),
        }
    }
}

impl KubeClusterResources {
    pub fn launch(launch: PipelineLaunchSpec) -> Result<Self> {
        start_storage()?;
        let storage_port = gen_unique_grpc_port();
        let storage_port_forward = start_storage_port_forward(storage_port)?;
        let pipeline_name = format!("kube-harness-{}", Uuid::new_v4().simple());
        let manifest_path = write_pipeline_manifest(
            &pipeline_name,
            launch.pipeline,
            launch.worker_count,
            launch.kube_worker_health_poll,
            launch.runtime_consts_profile,
        )?;
        kubectl(&["apply", "-f", manifest_path.to_str().unwrap()])?;
        wait_for_pipeline(&pipeline_name)?;
        let master_port = gen_unique_grpc_port();
        let master_port_forward = start_master_port_forward(&pipeline_name, master_port)?;
        wait_for_local_port(storage_port, Duration::from_secs(30))?;
        wait_for_local_port(master_port, Duration::from_secs(30))?;

        Ok(Self {
            resources: Some(KubeResources {
                pipeline_name: pipeline_name.clone(),
                manifest_path,
                storage_port_forward,
                master_port_forward,
                master_port,
            }),
            storage_endpoint: format!("http://127.0.0.1:{storage_port}"),
            expected_output_rows: launch.expected_output_rows,
            worker_ids: (0..launch.worker_count)
                .map(|index| format!("{pipeline_name}-worker-{index}"))
                .collect(),
        })
    }

    pub fn worker_ids(&self) -> Vec<String> {
        self.worker_ids.clone()
    }

    pub async fn wait_for_completion(&mut self) -> Result<()> {
        let start = tokio::time::Instant::now();
        loop {
            if self.storage_snapshot().await?.row_count() >= self.expected_output_rows {
                return Ok(());
            }
            if start.elapsed() > Duration::from_secs(120) {
                return Err(anyhow!("timeout waiting for kube output"));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    pub async fn storage_snapshot(&self) -> Result<InMemoryStorageSnapshot> {
        InMemoryStorageClient::new(self.storage_endpoint.clone())
            .await?
            .snapshot()
            .await
    }

    pub fn delete_worker_pod(&self, worker_id: &str) -> Result<()> {
        // grace-period=0: stop serving ASAP so in-flight checkpoints cannot complete
        // during the default termination window (mid-flight kill tests).
        kubectl(&[
            "-n",
            "default",
            "delete",
            "pod",
            worker_id,
            "--grace-period=0",
            "--force",
        ])?;
        Ok(())
    }

    pub fn kill_master(&self) -> Result<()> {
        let pipeline_name = &self.resources()?.pipeline_name;
        kubectl(&[
            "-n",
            "default",
            "delete",
            "pod",
            &format!("{pipeline_name}-master"),
        ])?;
        Ok(())
    }

    pub fn shutdown(&mut self) {
        if let Some(resources) = self.resources.take() {
            resources.stop();
        }
    }

    fn resources(&self) -> Result<&KubeResources> {
        self.resources
            .as_ref()
            .context("kube resources are stopped")
    }
}

fn start_storage() -> Result<()> {
    let manifest_dir =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("kubevolga/config/test-storage");
    kubectl(&[
        "-n",
        "default",
        "delete",
        "deployment/volga-test-storage",
        "--ignore-not-found=true",
    ])?;
    kubectl(&["apply", "-k", manifest_dir.to_str().unwrap()])?;
    kubectl(&[
        "-n",
        "default",
        "rollout",
        "status",
        "deployment/volga-test-storage",
        "--timeout=120s",
    ])?;
    Ok(())
}

fn write_pipeline_manifest(
    pipeline_name: &str,
    mut pipeline: PipelineSpec,
    worker_count: usize,
    kube_worker_health_poll: bool,
    runtime_consts_profile: crate::runtime::consts::RuntimeConstsProfile,
) -> Result<PathBuf> {
    super::install_in_memory_sink(
        &mut pipeline,
        "http://volga-test-storage.default.svc.cluster.local:50071",
    );
    let sample_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("kubevolga/config/samples/volga_v1alpha1_pipeline.yaml");
    let mut manifest: Value = serde_yaml::from_str(&fs::read_to_string(sample_path)?)?;
    manifest["metadata"]["name"] = Value::String(pipeline_name.to_string());
    manifest["metadata"]["namespace"] = Value::String("default".to_string());
    if !manifest["metadata"]["annotations"].is_object() {
        manifest["metadata"]["annotations"] = Value::Object(Default::default());
    }
    let annotations = manifest["metadata"]["annotations"].as_object_mut().unwrap();
    annotations.insert(
        crate::orchestrator::kube::KUBE_WORKER_HEALTH_POLL_ANNOTATION.to_string(),
        Value::String(if kube_worker_health_poll {
            "true".to_string()
        } else {
            "false".to_string()
        }),
    );
    annotations.insert(
        crate::orchestrator::kube::RUNTIME_CONSTS_PROFILE_ANNOTATION.to_string(),
        Value::String(runtime_consts_profile.as_str().to_string()),
    );
    manifest["spec"]["pipelineSpec"] = serde_json::to_value(pipeline)?;
    manifest["spec"]["workers"]["replicas"] = Value::Number(worker_count.into());

    let path = std::env::temp_dir().join(format!("{pipeline_name}-pipeline.json"));
    fs::write(&path, serde_json::to_vec_pretty(&manifest)?)?;
    Ok(path)
}

fn wait_for_pipeline(pipeline_name: &str) -> Result<()> {
    let start = std::time::Instant::now();
    loop {
        let phase = kubectl(&[
            "-n",
            "default",
            "get",
            "volgapipeline",
            pipeline_name,
            "-o",
            "jsonpath={.status.phase}",
        ])?;
        if phase.trim() == "Running" {
            return Ok(());
        }
        if start.elapsed() > Duration::from_secs(180) {
            return Err(anyhow!("timeout waiting for kube pipeline"));
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}

fn start_storage_port_forward(local_port: u16) -> Result<Child> {
    Command::new("kubectl")
        .args([
            "-n",
            "default",
            "port-forward",
            "svc/volga-test-storage",
            &format!("{local_port}:50071"),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to start kubectl port-forward")
}

fn start_master_port_forward(pipeline_name: &str, local_port: u16) -> Result<Child> {
    Command::new("kubectl")
        .args([
            "-n",
            "default",
            "port-forward",
            &format!("pod/{pipeline_name}-master"),
            &format!("{local_port}:50051"),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to start master port-forward")
}

fn wait_for_local_port(port: u16, timeout: Duration) -> Result<()> {
    let start = std::time::Instant::now();
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return Ok(());
        }
        if start.elapsed() > timeout {
            return Err(anyhow!(
                "timeout waiting for local port-forward 127.0.0.1:{port}"
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

async fn fetch_lifecycle_events(
    master_port: u16,
    sequence: u64,
) -> Result<Vec<LifecycleEventRecord>> {
    let mut client =
        MasterServiceClient::connect(format!("http://127.0.0.1:{master_port}")).await?;
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

async fn master_latest_pipeline_snapshot(
    master_port: u16,
) -> Result<Option<crate::runtime::observability::PipelineSnapshot>> {
    let mut client =
        MasterServiceClient::connect(format!("http://127.0.0.1:{master_port}")).await?;
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

async fn master_stop_sources(master_port: u16) -> Result<()> {
    let mut client =
        MasterServiceClient::connect(format!("http://127.0.0.1:{master_port}")).await?;
    let response = client
        .stop_sources(tonic::Request::new(
            crate::runtime::master::server::master_service::StopSourcesRequest {},
        ))
        .await?
        .into_inner();
    if !response.success {
        return Err(anyhow!("stop_sources failed: {}", response.error_message));
    }
    Ok(())
}

fn kubectl(args: &[&str]) -> Result<String> {
    let output = Command::new("kubectl")
        .args(args)
        .output()
        .with_context(|| format!("failed to execute kubectl {args:?}"))?;
    if !output.status.success() {
        return Err(anyhow!(
            "kubectl {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}
