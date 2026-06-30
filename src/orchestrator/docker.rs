use std::collections::HashMap;
use std::env;
use std::fs;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;

use crate::api::PipelineSpec;

use super::orchestrator::{MasterOrchestrator, WorkerNode, WorkerOrchestrator};

fn parse_pipeline_spec_from_env() -> Result<PipelineSpec> {
    if let Ok(spec_path) = env::var("VOLGA_PIPELINE_SPEC_PATH") {
        let content = fs::read_to_string(&spec_path)
            .with_context(|| format!("failed to read VOLGA_PIPELINE_SPEC_PATH={}", spec_path))?;
        let spec: PipelineSpec =
            serde_json::from_str(&content).context("failed to parse pipeline spec file JSON")?;
        return Ok(spec);
    }
    if let Ok(spec_json) = env::var("VOLGA_PIPELINE_SPEC_JSON") {
        let spec: PipelineSpec =
            serde_json::from_str(&spec_json).context("failed to parse VOLGA_PIPELINE_SPEC_JSON")?;
        return Ok(spec);
    }
    bail!("pipeline spec is required: set VOLGA_PIPELINE_SPEC_JSON or VOLGA_PIPELINE_SPEC_PATH");
}

fn parse_env_u16(name: &str) -> u16 {
    let v = env::var(name).unwrap_or_else(|_| panic!("{name} is required for docker orchestrator"));
    v.parse::<u16>()
        .unwrap_or_else(|_| panic!("failed to parse {name}='{v}' as u16"))
}

fn parse_env_usize(name: &str) -> usize {
    let v = env::var(name).unwrap_or_else(|_| panic!("{name} is required for docker orchestrator"));
    v.parse::<usize>()
        .unwrap_or_else(|_| panic!("failed to parse {name}='{v}' as usize"))
}

#[derive(Clone, Debug)]
pub struct DockerMasterOrchestrator {
    worker_nodes: HashMap<String, WorkerNode>,
    pipeline_id: String,
    spec: PipelineSpec,
}

#[derive(Clone, Debug)]
pub struct DockerWorkerOrchestrator {
    master_service_addr: String,
    worker_host_prefix: String,
}

impl DockerMasterOrchestrator {
    pub fn from_env() -> Result<Self> {
        let pipeline_id = env::var("VOLGA_PIPELINE_ID")
            .unwrap_or_else(|_| panic!("VOLGA_PIPELINE_ID is required for docker orchestrator"));
        let worker_count = parse_env_usize("VOLGA_WORKER_COUNT").max(1);
        let worker_host_prefix = env::var("VOLGA_WORKER_HOST_PREFIX")
            .unwrap_or_else(|_| panic!("VOLGA_WORKER_HOST_PREFIX is required for docker orchestrator"));
        let worker_port = parse_env_u16("VOLGA_WORKER_PORT");
        let transport_port = parse_env_u16("VOLGA_WORKER_TRANSPORT_PORT");
        let mut worker_nodes = HashMap::new();
        for i in 0..worker_count {
            let ordinal = i;
            let worker_id = format!("{}{}", worker_host_prefix, ordinal);
            worker_nodes.insert(
                worker_id.clone(),
                WorkerNode::new(
                    worker_id.clone(),
                    worker_id,
                    worker_port,
                    transport_port,
                ),
            );
        }
        let spec = parse_pipeline_spec_from_env()?;
        Ok(Self {
            worker_nodes,
            pipeline_id,
            spec,
        })
    }
}

impl DockerWorkerOrchestrator {
    pub fn from_env() -> Result<Self> {
        let master_service_addr = env::var("VOLGA_MASTER_SERVICE_ADDR")
            .unwrap_or_else(|_| panic!("VOLGA_MASTER_SERVICE_ADDR is required for docker orchestrator"));
        let worker_host_prefix = env::var("VOLGA_WORKER_HOST_PREFIX")
            .unwrap_or_else(|_| panic!("VOLGA_WORKER_HOST_PREFIX is required for docker orchestrator"));
        Ok(Self {
            master_service_addr,
            worker_host_prefix,
        })
    }
}

#[async_trait]
impl MasterOrchestrator for DockerMasterOrchestrator {
    async fn get_worker_nodes(&self) -> HashMap<String, WorkerNode> {
        self.worker_nodes.clone()
    }

    async fn get_pipeline_id(&self) -> String {
        self.pipeline_id.clone()
    }

    async fn get_spec(&self) -> PipelineSpec {
        self.spec.clone()
    }

    async fn get_num_expected_workers(&self) -> usize {
        self.worker_nodes.len()
    }
}

#[async_trait]
impl WorkerOrchestrator for DockerWorkerOrchestrator {
    async fn get_master_service_addr(&self) -> String {
        self.master_service_addr.clone()
    }

    async fn resolve_worker_id(&self) -> Result<String> {
        let worker_index = env::var("VOLGA_WORKER_INDEX")
            .context("VOLGA_WORKER_INDEX is required for docker worker orchestrator")?;
        let index = worker_index
            .parse::<usize>()
            .with_context(|| format!("failed to parse VOLGA_WORKER_INDEX='{}' as usize", worker_index))?;
        Ok(format!("{}{}", self.worker_host_prefix, index))
    }
}
