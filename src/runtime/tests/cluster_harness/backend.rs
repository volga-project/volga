use anyhow::{bail, Result};
use async_trait::async_trait;

use super::{FaultAction, PipelineLaunchSpec};
use crate::runtime::master::LifecycleEventRecord;
use crate::runtime::observability::PipelineSnapshot;
use crate::storage::InMemoryStorageSnapshot;

#[async_trait]
pub trait ClusterBackend: Send {
    async fn launch(&mut self, launch: PipelineLaunchSpec) -> Result<Vec<String>>;
    async fn start_execution(&mut self) -> Result<()>;
    async fn wait_for_completion(&mut self) -> Result<()>;
    async fn shutdown(&mut self) -> Result<()>;
    async fn storage_snapshot(&mut self) -> Result<InMemoryStorageSnapshot>;
    async fn lifecycle_events_since(&mut self, sequence: u64) -> Result<Vec<LifecycleEventRecord>>;
    async fn latest_pipeline_snapshot(&mut self) -> Result<Option<PipelineSnapshot>>;
    async fn stop_sources(&mut self) -> Result<()>;

    async fn apply_fault(&mut self, fault: FaultAction) -> Result<()> {
        bail!("fault action {:?} is not supported by this environment", fault)
    }
}
