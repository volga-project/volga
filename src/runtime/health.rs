use std::sync::{Arc, Mutex};

use parking_lot::RwLock;
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

/// Per-incarnation fatal bus. Recreated on configure; orphaned on reset/close.
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
}

/// Process-stable pointer to the current [`WorkerHealth`].
#[derive(Clone, Debug, Default)]
pub struct WorkerHealthSlot {
    current: Arc<RwLock<Option<Arc<WorkerHealth>>>>,
}

impl WorkerHealthSlot {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn install(&self, health: Arc<WorkerHealth>) {
        *self.current.write() = Some(health);
    }

    pub fn clear(&self) {
        *self.current.write() = None;
    }

    pub fn get(&self) -> Option<Arc<WorkerHealth>> {
        self.current.read().clone()
    }
}
