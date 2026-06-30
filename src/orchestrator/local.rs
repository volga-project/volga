use std::collections::HashMap;

use async_trait::async_trait;

use crate::api::PipelineSpec;

use super::orchestrator::{MasterOrchestrator, WorkerNode, WorkerOrchestrator, mock_worker_nodes};

#[derive(Debug, Clone)]
pub struct LocalMasterOrchestrator {
    worker_nodes: HashMap<String, WorkerNode>,
    pipeline_id: String,
    spec: Option<PipelineSpec>,
    num_expected_workers: usize,
}

#[derive(Debug, Clone)]
pub struct LocalWorkerOrchestrator {
    master_service_addr: String,
}

impl LocalMasterOrchestrator {
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
        }
    }

    pub fn with_spec(mut self, spec: PipelineSpec) -> Self {
        self.spec = Some(spec);
        self
    }
}

impl LocalWorkerOrchestrator {
    pub fn new(master_service_addr: String) -> Self {
        Self { master_service_addr }
    }
}

#[async_trait]
impl MasterOrchestrator for LocalMasterOrchestrator {
    async fn get_worker_nodes(&self) -> HashMap<String, WorkerNode> {
        self.worker_nodes.clone()
    }

    async fn get_pipeline_id(&self) -> String {
        self.pipeline_id.clone()
    }

    async fn get_spec(&self) -> PipelineSpec {
        self
            .spec
            .clone()
            .expect("LocalMasterOrchestrator spec is not configured")
    }

    async fn get_num_expected_workers(&self) -> usize {
        self.num_expected_workers
    }
}

#[async_trait]
impl WorkerOrchestrator for LocalWorkerOrchestrator {
    async fn get_master_service_addr(&self) -> String {
        self.master_service_addr.clone()
    }

    async fn resolve_worker_id(&self) -> anyhow::Result<String> {
        panic!("LocalWorkerOrchestrator::resolve_worker_id is not used; local worker id is set explicitly")
    }
}
