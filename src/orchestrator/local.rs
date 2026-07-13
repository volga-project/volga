use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use anyhow::Result;

use crate::api::PipelineSpec;

use super::orchestrator::{MasterOrchestrator, WorkerNode, WorkerOrchestrator, mock_worker_nodes};

#[async_trait]
pub trait LocalWorkerReplacement: Send + Sync {
    async fn replace_workers(&self, worker_ids: &[String]) -> Result<()>;
}

#[derive(Clone)]
pub struct LocalTestOrchestrator {
    worker_nodes: Arc<Mutex<HashMap<String, WorkerNode>>>,
    pipeline_id: String,
    spec: Option<PipelineSpec>,
    num_expected_workers: usize,
    replacement: Option<Arc<dyn LocalWorkerReplacement>>,
    replacement_calls: Arc<Mutex<Vec<Vec<String>>>>,
}

#[derive(Debug, Clone)]
pub struct LocalWorkerOrchestrator {
    master_service_addr: String,
}

impl LocalTestOrchestrator {
    pub fn new(num_workers: usize, pipeline_id: String) -> Self {
        let worker_nodes = mock_worker_nodes(num_workers.max(1));
        let worker_nodes = worker_nodes
            .into_iter()
            .map(|n| (n.worker_id.clone(), n))
            .collect::<HashMap<_, _>>();
        let expected_workers = worker_nodes.len();
        Self {
            worker_nodes: Arc::new(Mutex::new(worker_nodes)),
            pipeline_id,
            spec: None,
            num_expected_workers: expected_workers,
            replacement: None,
            replacement_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn with_spec(mut self, spec: PipelineSpec) -> Self {
        self.spec = Some(spec);
        self
    }

    pub fn with_replacement(mut self, replacement: Arc<dyn LocalWorkerReplacement>) -> Self {
        self.replacement = Some(replacement);
        self
    }

    pub fn replacement_calls(&self) -> Vec<Vec<String>> {
        self.replacement_calls.lock().unwrap().clone()
    }
}

impl LocalWorkerOrchestrator {
    pub fn new(master_service_addr: String) -> Self {
        Self { master_service_addr }
    }
}

#[async_trait]
impl MasterOrchestrator for LocalTestOrchestrator {
    async fn get_worker_nodes(&self) -> HashMap<String, WorkerNode> {
        self.worker_nodes.lock().unwrap().clone()
    }

    async fn get_pipeline_id(&self) -> String {
        self.pipeline_id.clone()
    }

    async fn get_spec(&self) -> PipelineSpec {
        self
            .spec
            .clone()
            .expect("LocalTestOrchestrator spec is not configured")
    }

    async fn get_num_expected_workers(&self) -> usize {
        self.num_expected_workers
    }

    async fn request_replacement(&self, worker_ids: &[String]) -> Result<()> {
        self.replacement_calls
            .lock()
            .unwrap()
            .push(worker_ids.to_vec());
        if let Some(replacement) = &self.replacement {
            replacement.replace_workers(worker_ids).await?;
        }
        Ok(())
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
