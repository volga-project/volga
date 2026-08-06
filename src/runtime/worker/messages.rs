use tokio::sync::mpsc;

use crate::runtime::health::WorkerFatalReason;
use crate::runtime::observability::snapshot_types::WorkerSnapshot;

use super::config::WorkerConfig;

#[derive(Debug)]
pub struct Configure(pub WorkerConfig);

/// Report a fatal into the current incarnation (no-op if unconfigured).
#[derive(Debug)]
pub struct ReportFatal {
    pub reason: WorkerFatalReason,
    pub message: String,
}

/// Fenced start for an execution attempt.
#[derive(Debug)]
pub struct Start {
    pub execution_attempt_id: u64,
}

#[derive(Debug)]
pub struct RunTasks {
    pub execution_attempt_id: u64,
}

#[derive(Debug)]
pub struct CloseTasks {
    pub execution_attempt_id: u64,
}

#[derive(Debug)]
pub struct GetState {
    pub execution_attempt_id: u64,
}

/// Heartbeat / diagnostics read (no fence).
#[derive(Debug)]
pub struct GetIdentity;

#[derive(Debug)]
pub struct StopSources {
    pub execution_attempt_id: u64,
}

#[derive(Debug)]
pub struct TriggerBarrier {
    pub checkpoint_id: u64,
    pub execution_attempt_id: u64,
}

/// Close nested runtimes and return to an empty shell (same ActorRef).
#[derive(Debug)]
pub struct Reset;

/// Fenced process teardown (gRPC shutdown).
#[derive(Debug)]
pub struct Shutdown {
    pub execution_attempt_id: u64,
}

/// Unfenced teardown (local stop / tests).
#[derive(Debug)]
pub struct Close;

/// In-process test orchestration (not used by gRPC).
#[derive(Debug)]
pub struct RunTestLifecycle {
    pub state_updates: Option<mpsc::Sender<WorkerSnapshot>>,
}
