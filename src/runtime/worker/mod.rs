//! Worker actor: owns task/transport runtimes for one process.

mod config;
mod handlers;
mod inner;
mod lifecycle;
mod messages;
mod tasks;

pub use config::{WorkerConfig, WorkerIdentity};
pub use messages::{
    Close, CloseTasks, Configure, GetIdentity, GetState, Reset, RunTasks, RunTestLifecycle,
    Shutdown, Start, StopSources, TriggerBarrier,
};

use std::sync::Arc;

use kameo::prelude::{spawn, ActorRef};
use kameo::Actor;

use crate::common::types::PipelineId;
use crate::runtime::health::WorkerHealthSlot;
use crate::runtime::observability::snapshot_types::WorkerSnapshot;
use crate::runtime::state::OperatorStates;

use inner::WorkerInner;

#[derive(Actor)]
pub struct Worker {
    pub(crate) worker_id: String,
    pub(crate) health_slot: WorkerHealthSlot,
    pub(crate) inner: Option<WorkerInner>,
    pub(crate) last_snapshot: WorkerSnapshot,
}

impl Worker {
    pub fn new(worker_id: String) -> Self {
        Self {
            worker_id: worker_id.clone(),
            health_slot: WorkerHealthSlot::new(),
            inner: None,
            last_snapshot: WorkerSnapshot::new(worker_id, PipelineId(String::new())),
        }
    }

    pub fn spawn(worker_id: String) -> (ActorRef<Self>, WorkerHealthSlot) {
        let worker = Self::new(worker_id);
        let health_slot = worker.health_slot.clone();
        (spawn(worker), health_slot)
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

    pub fn health_slot(&self) -> WorkerHealthSlot {
        self.health_slot.clone()
    }

    pub fn is_configured(&self) -> bool {
        self.inner.is_some()
    }

    pub fn is_running(&self) -> bool {
        self.inner.as_ref().is_some_and(|i| i.is_running())
    }

    pub fn worker_id(&self) -> String {
        self.worker_id.clone()
    }

    pub fn pipeline_id(&self) -> Option<String> {
        self.inner
            .as_ref()
            .map(|i| i.config.pipeline_id.0.clone())
    }

    pub fn execution_attempt_id(&self) -> u64 {
        self.inner
            .as_ref()
            .map(|i| i.execution_attempt_id())
            .unwrap_or_default()
    }

    pub fn operator_states(&self) -> Arc<OperatorStates> {
        self.inner
            .as_ref()
            .map(|i| i.operator_states.clone())
            .unwrap_or_else(|| Arc::new(OperatorStates::new()))
    }

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

    pub(crate) fn require_inner(&mut self) -> Result<&mut WorkerInner, String> {
        self.inner
            .as_mut()
            .ok_or_else(|| "Worker is not configured yet".to_string())
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.inner.take();
        self.health_slot.clear();
    }
}
