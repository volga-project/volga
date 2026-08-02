//! Per-source interrupt handles for source tasks.
//!
//! [`SourceInterrupt`]: checkpointable sources use `sleep` / `race` for prompt barrier yield.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

use crate::runtime::VertexId;

/// Latched cancel + notify for interruptible source waits (checkpoint barrier yield).
#[derive(Debug, Default)]
pub struct SourceInterrupt {
    wake: Notify,
    canceled: AtomicBool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interrupted;

impl SourceInterrupt {
    pub fn new() -> Self {
        Self::default()
    }

    /// Worker/task path: ask the source to yield (latch + wake any in-flight wait).
    pub fn cancel(&self) {
        self.canceled.store(true, Ordering::SeqCst);
        self.wake.notify_waiters();
    }

    /// True if cancel arrived between fetches (nobody was in [`Self::race`]).
    pub fn take_canceled(&self) -> bool {
        self.canceled.swap(false, Ordering::SeqCst)
    }

    /// Complete `fut` or return [`Interrupted`] if [`Self::cancel`] runs first.
    pub async fn race<T>(&self, fut: impl Future<Output = T>) -> Result<T, Interrupted> {
        tokio::select! {
            result = fut => Ok(result),
            _ = self.wake.notified() => {
                self.canceled.store(false, Ordering::SeqCst);
                Err(Interrupted)
            }
        }
    }
}

/// Race a future against an optional checkpoint interrupt.
pub async fn race_interruptible<T>(
    interrupt: Option<&SourceInterrupt>,
    fut: impl Future<Output = T>,
) -> Result<T, Interrupted> {
    match interrupt {
        Some(interrupt) => interrupt.race(fut).await,
        None => Ok(fut.await),
    }
}

/// Per source-task shared interrupt + cooperative stop.
#[derive(Debug)]
pub struct SourceHandle {
    pub interrupt: Arc<SourceInterrupt>,
    /// Latched by harness/master `StopSources`; source then returns Idle and finishes.
    pub stopped: Arc<AtomicBool>,
}

impl SourceHandle {
    fn new() -> Self {
        Self {
            interrupt: Arc::new(SourceInterrupt::new()),
            stopped: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        self.interrupt.cancel();
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }
}

/// Worker-owned map of [`SourceHandle`] by vertex.
#[derive(Debug, Default, Clone)]
pub struct SourceHandles {
    inner: Arc<Mutex<HashMap<VertexId, Arc<SourceHandle>>>>,
}

impl SourceHandles {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
    }

    /// Get or create the handle for a source vertex.
    pub fn register(&self, vertex_id: VertexId) -> Arc<SourceHandle> {
        let mut guard = self.inner.lock().unwrap();
        guard
            .entry(vertex_id)
            .or_insert_with(|| Arc::new(SourceHandle::new()))
            .clone()
    }

    pub fn get(&self, vertex_id: &VertexId) -> Option<Arc<SourceHandle>> {
        self.inner.lock().unwrap().get(vertex_id).cloned()
    }

    pub fn cancel(&self, vertex_id: &VertexId) {
        if let Some(handle) = self.inner.lock().unwrap().get(vertex_id) {
            handle.interrupt.cancel();
        }
    }

    /// Cooperative stop on every registered source (Idle → drain → Finished).
    pub fn stop_all(&self) {
        for handle in self.inner.lock().unwrap().values() {
            handle.stop();
        }
    }
}
