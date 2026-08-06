use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use tokio::sync::broadcast;

#[derive(Clone, Debug)]
pub enum WorkerFatalReason {
    Panic,
    TransportDisconnect,
    TaskFailure,
}

#[derive(Clone, Debug)]
pub struct WorkerFatalEvent {
    pub reason: WorkerFatalReason,
    pub message: String,
}

#[derive(Debug)]
pub struct WorkerHealth {
    tx: broadcast::Sender<WorkerFatalEvent>,
    last_fatal: Mutex<Option<WorkerFatalEvent>>,
    /// Set on configure; `0` when unconfigured / after reset.
    execution_attempt_id: AtomicU64,
}

impl WorkerHealth {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(64);
        Self {
            tx,
            last_fatal: Mutex::new(None),
            execution_attempt_id: AtomicU64::new(0),
        }
    }

    pub fn execution_attempt_id(&self) -> u64 {
        self.execution_attempt_id.load(Ordering::Acquire)
    }

    /// Set this bus to an execution attempt and clear sticky fatal state.
    pub fn set_execution_attempt(&self, execution_attempt_id: u64) {
        self.execution_attempt_id
            .store(execution_attempt_id, Ordering::Release);
        self.clear();
    }

    pub fn subscribe(&self) -> broadcast::Receiver<WorkerFatalEvent> {
        self.tx.subscribe()
    }

    /// Record a fatal only if it belongs to the currently bound attempt.
    /// Stale tasks/transport from a prior incarnation pass their old id and are ignored.
    pub fn report_fatal(
        &self,
        execution_attempt_id: u64,
        reason: WorkerFatalReason,
        message: impl Into<String>,
    ) {
        if execution_attempt_id != self.execution_attempt_id.load(Ordering::Acquire) {
            return;
        }
        let event = WorkerFatalEvent {
            reason,
            message: message.into(),
        };
        if let Ok(mut guard) = self.last_fatal.lock() {
            if guard.is_none() {
                *guard = Some(event.clone());
            }
        }

        let _ = self.tx.send(event);
    }

    pub fn last_fatal(&self) -> Option<WorkerFatalEvent> {
        self.last_fatal.lock().ok().and_then(|g| g.clone())
    }

    /// Clear the sticky "last fatal". Prefer [`Self::set_execution_attempt`] on
    /// configure/reset; this remains for callers that only need to wipe sticky state.
    pub fn clear(&self) {
        if let Ok(mut guard) = self.last_fatal.lock() {
            *guard = None;
        }
    }
}
