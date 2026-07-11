use std::collections::HashMap;
use anyhow::Result;
use async_trait::async_trait;
use crate::api::PipelineSpec;
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
}

#[async_trait]
pub trait WorkerOrchestrator: Send + Sync {
    async fn get_master_service_addr(&self) -> String;
    async fn resolve_worker_id(&self) -> Result<String>;
}

pub fn mock_worker_nodes(num_nodes: usize) -> Vec<WorkerNode> {
    (0..num_nodes)
        .map(|i| WorkerNode::new(
            format!("worker-{}", i + 1),
            "127.0.0.1".to_string(),
            gen_unique_grpc_port(),
            gen_unique_grpc_port(),
        ))
        .collect()
}