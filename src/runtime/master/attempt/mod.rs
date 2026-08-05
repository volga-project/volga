use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::common::failure::FailureEvent;
use super::state::{MasterState, PipelineContext};
use super::worker_client::WorkerClient;

mod execute;
mod phase;
mod schedule;
mod teardown;

pub(super) use phase::{AttemptPhase, AtomicAttemptPhase};

pub(super) enum AttemptOutcome {
    Finished,
    Recover(HashSet<String>),
}

pub(super) enum ScheduleError {
    Terminal(String),
    Recoverable {
        replace: HashSet<String>,
        detail: String,
    },
}

pub(super) struct ExecutionAttempt {
    id: u64,
    restore_checkpoint_id: Option<u64>,
    state: Arc<MasterState>,
    pipeline: Arc<PipelineContext>,
    /// Shared with MasterState / StopSources (same `Arc`).
    phase: Arc<AtomicAttemptPhase>,
    clients: HashMap<String, WorkerClient>,
    failure_tx: mpsc::Sender<FailureEvent>,
    failure_rx: mpsc::Receiver<FailureEvent>,
}

impl ExecutionAttempt {
    pub(super) fn new(
        id: u64,
        restore_checkpoint_id: Option<u64>,
        state: Arc<MasterState>,
        pipeline: Arc<PipelineContext>,
    ) -> Self {
        let (failure_tx, failure_rx) = mpsc::channel(256);
        let phase = state.attempt_phase();
        phase.set(AttemptPhase::Running);
        Self {
            id,
            restore_checkpoint_id,
            state,
            pipeline,
            phase,
            clients: HashMap::new(),
            failure_tx,
            failure_rx,
        }
    }
}
