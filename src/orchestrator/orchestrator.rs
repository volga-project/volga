use std::collections::HashMap;

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
pub trait Orchestrator: Send + Sync {
    async fn get_worker_nodes(&self) -> HashMap<String, WorkerNode>;
    async fn get_pipeline_id(&self) -> String {
        panic!("Orchestrator::get_pipeline_id is not implemented for this orchestrator")
    }
    async fn get_spec(&self) -> PipelineSpec {
        panic!("Orchestrator::get_spec is not configured for this orchestrator")
    }
    async fn get_num_expected_workers(&self) -> usize {
        panic!("Orchestrator::get_num_expected_workers is not implemented for this orchestrator")
    }
    async fn get_master_service_addr(&self) -> String {
        panic!("Orchestrator::get_master_service_addr is not implemented for this orchestrator")
    }
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

#[derive(Debug, Clone)]
pub struct LocalOrchestrator {
    worker_nodes: HashMap<String, WorkerNode>,
    pipeline_id: String,
    spec: Option<PipelineSpec>,
    num_expected_workers: usize,
    master_service_addr: String,
}

impl LocalOrchestrator {
    pub fn new(num_workers: usize, pipeline_id: String) -> Self {
        let worker_nodes = mock_worker_nodes(num_workers.max(1));
        let worker_nodes = worker_nodes
            .into_iter()
            .map(|n| (n.worker_id.clone(), n))
            .collect::<HashMap<_, _>>();
        let expected_workers = worker_nodes.len();
        Self {
            worker_nodes,
            pipeline_id,
            spec: None,
            num_expected_workers: expected_workers,
            master_service_addr: String::new(),
        }
    }

    pub fn with_spec(mut self, spec: PipelineSpec) -> Self {
        self.spec = Some(spec);
        self
    }

    pub fn with_master_service_addr(mut self, addr: String) -> Self {
        self.master_service_addr = addr;
        self
    }
}

#[async_trait]
impl Orchestrator for LocalOrchestrator {
    async fn get_worker_nodes(&self) -> HashMap<String, WorkerNode> {
        self.worker_nodes.clone()
    }

    async fn get_pipeline_id(&self) -> String {
        self.pipeline_id.clone()
    }

    async fn get_spec(&self) -> PipelineSpec {
        self.spec
            .clone()
            .expect("LocalClusterProvider spec is not configured")
    }

    async fn get_num_expected_workers(&self) -> usize {
        self.num_expected_workers
    }

    async fn get_master_service_addr(&self) -> String {
        self.master_service_addr.clone()
    }
}