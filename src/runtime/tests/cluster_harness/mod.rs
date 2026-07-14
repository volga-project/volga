#[path = "resources/docker.rs"]
mod docker;
#[path = "resources/kube.rs"]
mod kube;
#[path = "resources/local/mod.rs"]
mod local;

pub mod backend;
pub mod cluster;
pub mod handles;
pub mod oracle;

pub use cluster::TestCluster;
pub use handles::{MasterHandle, StorageHandle, WorkerHandle};
pub use oracle::{LifecycleOracle, OutputOracle};

pub(crate) use docker::DockerCluster;
pub(crate) use kube::KubeCluster;
pub(crate) use local::LocalCluster;

use crate::api::PipelineSpec;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeEnv {
    Local,
    Docker,
    Kube,
}

/// How a local worker kill is simulated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WorkerKillMode {
    /// Tear down without reporting Panic (master sees HeartbeatUnavailable).
    #[default]
    Abrupt,
    /// Report `WorkerFatalReason::Panic` before teardown (master sees WorkerPanic).
    Panic,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultAction {
    KillWorker {
        worker_id: String,
        mode: WorkerKillMode,
    },
    RestartWorker { worker_id: String },
    KillMaster,
    RestartMaster,
}

#[derive(Debug, Clone)]
pub struct PipelineLaunchSpec {
    pub pipeline: PipelineSpec,
    pub worker_count: usize,
    pub expected_output_rows: usize,
}

impl PipelineLaunchSpec {
    pub fn new(pipeline: PipelineSpec, worker_count: usize, expected_output_rows: usize) -> Self {
        Self {
            pipeline,
            worker_count,
            expected_output_rows,
        }
    }
}
