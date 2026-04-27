use anyhow::{bail, Result};
use async_trait::async_trait;

use crate::executor::placement::TaskPlacementStrategyName;
use crate::executor::runtime_adapter::{AttemptHandle, RuntimeAdapter, StartAttemptRequest};

#[derive(Clone, Debug)]
pub struct K8sRuntimeConfig {
    pub namespace: String,
    pub master_image: String,
    pub worker_image: String,
}

impl Default for K8sRuntimeConfig {
    fn default() -> Self {
        Self {
            namespace: "default".to_string(),
            master_image: "volga-master:latest".to_string(),
            worker_image: "volga-worker:latest".to_string(),
        }
    }
}

pub struct K8sRuntimeAdapter {
    pub config: K8sRuntimeConfig,
}

impl K8sRuntimeAdapter {
    pub fn new(config: K8sRuntimeConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl RuntimeAdapter for K8sRuntimeAdapter {
    async fn start_attempt(&self, _req: StartAttemptRequest) -> Result<AttemptHandle> {
        bail!("K8s runtime adapter start lifecycle is not implemented yet")
    }

    async fn stop_attempt(&self, _handle: &AttemptHandle) -> Result<()> {
        Ok(())
    }

    fn supported_task_placement_strategies(&self) -> &'static [TaskPlacementStrategyName] {
        &[TaskPlacementStrategyName::SingleWorker]
    }
}
