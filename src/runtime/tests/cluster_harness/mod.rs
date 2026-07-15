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
pub use oracle::{LifecycleOracle, OutputOracle, RecoveryAttemptReport, RecoveryReport};

pub(crate) use docker::DockerCluster;
pub(crate) use kube::KubeCluster;
pub(crate) use local::LocalCluster;

use crate::api::PipelineSpec;
use crate::runtime::consts::RuntimeConstsProfile;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeEnv {
    Local,
    Docker,
    Kube,
}

/// How a worker kill is simulated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WorkerKillMode {
    /// Tear down without reporting Panic (master sees HeartbeatUnavailable / StatePoll).
    /// Local: abort worker server. Kube: `kubectl delete pod`.
    #[default]
    Abrupt,
    /// Report `WorkerFatalReason::Panic` before teardown (master sees WorkerPanic).
    /// Local-only; kube ignores and falls back to Abrupt pod delete.
    Panic,
    /// Local-only: kill the worker process and start a new one on the **same listen
    /// address** without configuring it for the current attempt (kube IP-reuse analogue).
    /// Exercises attempt fencing: master must reject the unbound peer (not only treat
    /// a dead dial as failure). Kube: same as Abrupt pod delete.
    SameAddrRestart,
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
    /// Kube only: sets `volga.io/kube-worker-health-poll` on the pipeline CR.
    /// Master reads it at poll start (env overrides). Default `true`.
    pub kube_worker_health_poll: bool,
    /// Kube only: sets `volga.io/runtime-consts-profile`. Default [`RuntimeConstsProfile::KubeTest`].
    /// In-process local masters already pick `local_test` via `cfg!(test)`.
    pub runtime_consts_profile: RuntimeConstsProfile,
}

impl PipelineLaunchSpec {
    pub fn new(pipeline: PipelineSpec, worker_count: usize, expected_output_rows: usize) -> Self {
        Self {
            pipeline,
            worker_count,
            expected_output_rows,
            kube_worker_health_poll: true,
            runtime_consts_profile: RuntimeConstsProfile::KubeTest,
        }
    }

    pub fn with_kube_worker_health_poll(mut self, enabled: bool) -> Self {
        self.kube_worker_health_poll = enabled;
        self
    }

    pub fn with_runtime_consts_profile(mut self, profile: RuntimeConstsProfile) -> Self {
        self.runtime_consts_profile = profile;
        self
    }
}
