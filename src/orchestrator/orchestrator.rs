use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::api::PipelineSpec;
use crate::common::failure::FailureEvent;
use crate::common::test_utils::gen_unique_grpc_port;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerNode {
    pub worker_id: String,
    pub worker_ip: String,
    pub worker_port: u16,
    pub transport_port: u16,
}

impl WorkerNode {
    pub fn new(worker_id: String, worker_ip: String, worker_port: u16, transport_port: u16) -> Self {
        Self {
            worker_id,
            worker_ip,
            worker_port,
            transport_port,
        }
    }
}

/// Aborts a background health-poll task when dropped.
pub struct WorkerHealthWatchHandle {
    join: JoinHandle<()>,
}

impl WorkerHealthWatchHandle {
    pub fn new(join: JoinHandle<()>) -> Self {
        Self { join }
    }
}

impl Drop for WorkerHealthWatchHandle {
    fn drop(&mut self) {
        self.join.abort();
    }
}

#[async_trait]
pub trait MasterOrchestrator: Send + Sync {
    async fn get_worker_nodes(&self) -> HashMap<String, WorkerNode>;
    async fn get_pipeline_id(&self) -> String;
    async fn get_spec(&self) -> PipelineSpec;
    async fn get_num_expected_workers(&self) -> usize;

    /// Request the orchestrator to physically replace the given workers (e.g. a dead pod).
    ///
    /// Default is a no-op: recovery reuses the existing worker process in-place, so this
    /// is only needed for workers the orchestrator reports as gone. Orchestrators that own
    /// pod lifecycle (Kubernetes) override this to delete the pods so they are recreated.
    async fn request_replacement(&self, _worker_ids: &[String]) -> Result<()> {
        Ok(())
    }

    async fn record_lifecycle_event(
        &self,
        _sequence: u64,
        _event_json: &str,
    ) -> Result<()> {
        Ok(())
    }

    /// Kube-only: poll worker pod health and send [`FailureEvent`]s. Default: no-op.
    /// Drop the handle to stop polling (end of attempt `run`).
    fn run_health_poll(
        &self,
        _worker_ids: HashSet<String>,
        _failure_tx: mpsc::Sender<FailureEvent>,
    ) -> Option<WorkerHealthWatchHandle> {
        None
    }

    /// Apply launch-time tuning from the environment / pipeline metadata before execute.
    async fn bootstrap(&self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait WorkerOrchestrator: Send + Sync {
    async fn get_master_service_addr(&self) -> String;
    async fn resolve_worker_id(&self) -> Result<String>;
}

pub fn mock_worker_nodes(num_nodes: usize) -> Vec<WorkerNode> {
    (0..num_nodes)
        .map(|i| {
            WorkerNode::new(
                format!("worker-{}", i + 1),
                "127.0.0.1".to_string(),
                gen_unique_grpc_port(),
                gen_unique_grpc_port(),
            )
        })
        .collect()
}

/// Default kube health poll interval (tests use a shorter value via cfg).
pub fn worker_health_poll_interval() -> Duration {
    if cfg!(test) {
        Duration::from_millis(250)
    } else {
        Duration::from_millis(500)
    }
}
