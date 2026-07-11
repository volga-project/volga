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
}

impl WorkerHealth {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(64);
        Self {
            tx,
            last_fatal: Mutex::new(None),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<WorkerFatalEvent> {
        self.tx.subscribe()
    }

    pub fn report_fatal(&self, reason: WorkerFatalReason, message: impl Into<String>) {
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

    /// Clear the sticky "last fatal". Must be called when a worker is
    /// (re)configured for a new execution attempt so a stale fatal from a previous
    /// incarnation does not immediately mark the fresh worker as unhealthy.
    pub fn clear(&self) {
        if let Ok(mut guard) = self.last_fatal.lock() {
            *guard = None;
        }
    }
}
