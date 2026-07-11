use std::sync::{Mutex, OnceLock};
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

static WORKER_FATAL_TX: OnceLock<broadcast::Sender<WorkerFatalEvent>> = OnceLock::new();
static LAST_WORKER_FATAL: OnceLock<Mutex<Option<WorkerFatalEvent>>> = OnceLock::new();

pub fn init_worker_health_bus() {
    let _ = WORKER_FATAL_TX.get_or_init(|| {
        let (tx, _rx) = broadcast::channel(64);
        tx
    });
    let _ = LAST_WORKER_FATAL.get_or_init(|| Mutex::new(None));
}

pub fn subscribe_worker_fatal_events() -> broadcast::Receiver<WorkerFatalEvent> {
    init_worker_health_bus();
    WORKER_FATAL_TX
        .get()
        .expect("health bus initialized")
        .subscribe()
}

pub fn report_worker_fatal(reason: WorkerFatalReason, message: impl Into<String>) {
    init_worker_health_bus();
    let event = WorkerFatalEvent {
        reason,
        message: message.into(),
    };
    if let Some(last) = LAST_WORKER_FATAL.get() {
        if let Ok(mut guard) = last.lock() {
            if guard.is_none() {
                *guard = Some(event.clone());
            }
        }
    }
    if let Some(tx) = WORKER_FATAL_TX.get() {
        let _ = tx.send(event);
    }
}

pub fn get_last_worker_fatal() -> Option<WorkerFatalEvent> {
    LAST_WORKER_FATAL
        .get()
        .and_then(|m| m.lock().ok().and_then(|g| g.clone()))
}

/// Clear the sticky "last fatal" for the process. Must be called when a worker is
/// (re)configured for a new execution attempt so a stale fatal from a previous incarnation
/// does not immediately mark the fresh worker as unhealthy.
pub fn clear_worker_fatal() {
    init_worker_health_bus();
    if let Some(last) = LAST_WORKER_FATAL.get() {
        if let Ok(mut guard) = last.lock() {
            *guard = None;
        }
    }
}
