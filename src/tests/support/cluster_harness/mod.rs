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

use crate::api::spec::connectors::SinkSpec;
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
    RestartWorker {
        worker_id: String,
    },
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
        let mut pipeline = pipeline;
        // Cluster e2e (local/docker/kube) always run store maintenance by default.
        pipeline.state.maintenance_enabled = true;
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

    /// Pipeline `state.maintenance_*` (flows into kube `pipelineSpec` / docker spec JSON).
    pub fn with_state_maintenance(mut self, enabled: bool, interval_ms: u64) -> Self {
        self.pipeline.state.maintenance_enabled = enabled;
        self.pipeline.state.maintenance_interval_ms = interval_ms.max(1);
        self
    }
}

/// Install/replace the in-memory gRPC sink address, preserving any upsert keys already on the pipeline.
/// Non-InMemory sinks (Count, Parquet, Request) are left unchanged.
pub(crate) fn install_in_memory_sink(pipeline: &mut PipelineSpec, server_addr: impl Into<String>) {
    let server_addr = server_addr.into();
    pipeline.sink = Some(match pipeline.sink.take() {
        Some(sink @ SinkSpec::InMemoryStorageGrpc { .. }) => sink.with_server_addr(server_addr),
        Some(other) => other,
        None => SinkSpec::in_memory_grpc(server_addr),
    });
}

pub(crate) fn pipeline_needs_in_memory_store(pipeline: &PipelineSpec) -> bool {
    pipeline
        .sink
        .as_ref()
        .map(SinkSpec::needs_in_memory_store)
        .unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::spec::event_time::EventTimeSpec;
    use crate::api::spec::operators::OperatorOverrides;
    use crate::api::spec::pipeline::ExecutionMode;
    use crate::api::spec::state::StateSpec;
    use crate::api::spec::worker_runtime::WorkerRuntimeSpec;

    fn pipeline_with_sink(sink: Option<SinkSpec>) -> PipelineSpec {
        PipelineSpec {
            execution_profile: None,
            execution_mode: ExecutionMode::Streaming,
            parallelism: 1,
            worker_runtime: WorkerRuntimeSpec::default(),
            state: StateSpec::default(),
            operator_overrides: OperatorOverrides::default(),
            event_time: EventTimeSpec::default(),
            sources: Vec::new(),
            request_source_sink: None,
            sink,
            sql: None,
            task_assignment_strategy: None,
        }
    }

    #[test]
    fn install_in_memory_sink_preserves_count() {
        let mut pipeline = pipeline_with_sink(Some(SinkSpec::Count));
        assert!(!pipeline_needs_in_memory_store(&pipeline));
        install_in_memory_sink(&mut pipeline, "http://127.0.0.1:1");
        assert!(matches!(pipeline.sink, Some(SinkSpec::Count)));
    }

    #[test]
    fn install_in_memory_sink_fills_default() {
        let mut pipeline = pipeline_with_sink(None);
        assert!(pipeline_needs_in_memory_store(&pipeline));
        install_in_memory_sink(&mut pipeline, "http://127.0.0.1:1");
        match pipeline.sink {
            Some(SinkSpec::InMemoryStorageGrpc { server_addr, .. }) => {
                assert_eq!(server_addr, "http://127.0.0.1:1");
            }
            other => panic!("expected InMemoryStorageGrpc, got {other:?}"),
        }
    }
}
