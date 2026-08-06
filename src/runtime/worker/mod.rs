//! Worker actor: owns task/transport runtimes for one process.

mod config;
mod handlers;
mod lifecycle;
mod messages;
mod tasks;

pub use config::{WorkerConfig, WorkerIdentity};
pub use messages::{
    Close, CloseTasks, Configure, GetIdentity, GetState, Reset, RunTasks, RunTestLifecycle,
    Shutdown, Start, StopSources, TriggerBarrier,
};

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use kameo::prelude::{spawn, ActorRef};
use kameo::Actor;
use tokio::runtime::Runtime;

use crate::common::types::PipelineId;
use crate::runtime::functions::source::request_source::RequestSourceProcessor;
use crate::runtime::health::WorkerHealth;
use crate::runtime::observability::snapshot_types::WorkerSnapshot;
use crate::runtime::operators::source::SourceHandles;
use crate::runtime::state::OperatorStates;
use crate::runtime::stream_task_actor::StreamTaskActor;
use crate::runtime::VertexId;
use crate::transport::transport_backend_actor::TransportBackendActor;

#[derive(Actor)]
pub struct Worker {
    pub(crate) worker_id: String,
    pub(crate) health: Arc<WorkerHealth>,
    pub(crate) config: Option<WorkerConfig>,
    pub(crate) task_actors: HashMap<VertexId, ActorRef<StreamTaskActor>>,
    pub(crate) backend_actor: Option<ActorRef<TransportBackendActor>>,
    pub(crate) task_runtimes: HashMap<VertexId, Runtime>,
    pub(crate) transport_backend_runtime: Option<Runtime>,
    pub(crate) worker_state: Arc<tokio::sync::Mutex<WorkerSnapshot>>,
    pub(crate) operator_states: Arc<OperatorStates>,
    pub(crate) running: Arc<AtomicBool>,
    pub(crate) tasks_state_polling_handle: Option<tokio::task::JoinHandle<()>>,
    /// Watches the process health bus; on a fatal event it quiesces tasks so a broken
    /// worker stops producing while the master drives a recovery reset.
    pub(crate) fatal_watcher_handle: Option<tokio::task::JoinHandle<()>>,
    pub(crate) request_source_processor: Option<RequestSourceProcessor>,
    pub(crate) request_source_processor_runtime: Option<Runtime>,
    pub(crate) source_handles: Arc<SourceHandles>,
}

impl Worker {
    pub fn new(worker_id: String) -> Self {
        Self {
            worker_id: worker_id.clone(),
            health: Arc::new(WorkerHealth::new()),
            config: None,
            task_actors: HashMap::new(),
            backend_actor: None,
            task_runtimes: HashMap::new(),
            transport_backend_runtime: None,
            worker_state: Arc::new(tokio::sync::Mutex::new(WorkerSnapshot::new(
                worker_id,
                PipelineId(String::new()),
            ))),
            operator_states: Arc::new(OperatorStates::new()),
            running: Arc::new(AtomicBool::new(false)),
            tasks_state_polling_handle: None,
            fatal_watcher_handle: None,
            request_source_processor: None,
            request_source_processor_runtime: None,
            source_handles: Arc::new(SourceHandles::new()),
        }
    }

    pub fn spawn(worker_id: String) -> (ActorRef<Self>, Arc<WorkerHealth>) {
        let worker = Self::new(worker_id);
        let health = worker.health.clone();
        (spawn(worker), health)
    }

    /// Spawn and configure in one step (tests).
    pub async fn spawn_configured(config: WorkerConfig) -> ActorRef<Self> {
        let worker_id = config.worker_id.clone();
        let (actor, _) = Self::spawn(worker_id);
        actor
            .ask(Configure(config))
            .await
            .expect("configure worker actor");
        actor
    }

    pub fn health(&self) -> Arc<WorkerHealth> {
        self.health.clone()
    }

    pub fn is_configured(&self) -> bool {
        self.config.is_some()
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    pub fn worker_id(&self) -> String {
        self.worker_id.clone()
    }

    pub fn pipeline_id(&self) -> Option<String> {
        self.config
            .as_ref()
            .map(|cfg| cfg.pipeline_id.0.clone())
    }

    pub fn execution_attempt_id(&self) -> u64 {
        self.config
            .as_ref()
            .map(|cfg| cfg.execution_attempt_id)
            .unwrap_or_default()
    }

    pub fn operator_states(&self) -> Arc<OperatorStates> {
        self.operator_states.clone()
    }

    /// Tear down nested runtimes. Idempotent. Keeps `worker_id` and `health`.
    /// Prefer [`Self::close_async`] from actor handlers so transport Close is awaited
    /// on the mailbox runtime instead of `block_on` from a sync path.
    pub(crate) fn identity(&self) -> WorkerIdentity {
        WorkerIdentity {
            worker_id: self.worker_id.clone(),
            pipeline_id: self.pipeline_id(),
            execution_attempt_id: self.execution_attempt_id(),
            configured: self.is_configured(),
        }
    }

    pub(crate) fn require_attempt(&self, execution_attempt_id: u64) -> Result<(), String> {
        if execution_attempt_id != self.execution_attempt_id() {
            return Err(format!(
                "stale worker command execution attempt: got {}, current {}",
                execution_attempt_id,
                self.execution_attempt_id()
            ));
        }
        Ok(())
    }

    // control functions
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.fatal_watcher_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.tasks_state_polling_handle.take() {
            handle.abort();
        }
        self.request_source_processor.take();
        self.task_actors.clear();
        self.config = None;
        self.backend_actor = None;
        // Best-effort runtime dispose; async Close should have run via Reset/Close.
        self.close_sync_dispose_runtimes();
    }
}
