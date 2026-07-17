//! Per-source interrupt + emit stats for source tasks.
//!
//! - [`SourceInterrupt`]: checkpointable sources use `sleep` / `race` for prompt barrier yield.
//! - [`SourceStats`]: rows emitted (updated by [`super::source_operator::SourceOperator`]);
//!   exposed as task poll metadata.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

/// Shared per-source emit counter (and labels) for snapshots / source logic.
#[derive(Debug)]
pub struct SourceStats {
    records_emitted: Arc<AtomicU64>,
    task_index: i32,
}

impl SourceStats {
    pub fn new(task_index: i32) -> Self {
        Self {
            records_emitted: Arc::new(AtomicU64::new(0)),
            task_index,
        }
    }

    pub fn task_index(&self) -> i32 {
        self.task_index
    }

    pub fn records_emitted(&self) -> u64 {
        self.records_emitted.load(Ordering::SeqCst)
    }

    pub fn records_emitted_arc(&self) -> Arc<AtomicU64> {
        self.records_emitted.clone()
    }

    pub fn add_records(&self, n: usize) {
        if n > 0 {
            self.records_emitted
                .fetch_add(n as u64, Ordering::SeqCst);
        }
    }

    pub fn set_records_emitted(&self, n: u64) {
        self.records_emitted.store(n, Ordering::SeqCst);
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

/// Per source-task shared interrupt + stats.
#[derive(Debug)]
pub struct SourceHandle {
    pub interrupt: Arc<SourceInterrupt>,
    pub stats: Arc<SourceStats>,
}

impl SourceHandle {
    fn new(task_index: i32) -> Self {
        Self {
            interrupt: Arc::new(SourceInterrupt::new()),
            stats: Arc::new(SourceStats::new(task_index)),
        }
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
    pub fn register(&self, vertex_id: VertexId, task_index: i32) -> Arc<SourceHandle> {
        let mut guard = self.inner.lock().unwrap();
        guard
            .entry(vertex_id)
            .or_insert_with(|| Arc::new(SourceHandle::new(task_index)))
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

    pub fn task_metadata(&self, vertex_id: &VertexId) -> HashMap<String, String> {
        let guard = self.inner.lock().unwrap();
        let Some(handle) = guard.get(vertex_id) else {
            return HashMap::new();
        };
        let mut meta = HashMap::new();
        meta.insert(
            crate::runtime::observability::task_meta::RECORDS_GENERATED.to_string(),
            handle.stats.records_emitted().to_string(),
        );
        meta.insert(
            crate::runtime::observability::task_meta::TASK_INDEX.to_string(),
            handle.stats.task_index().to_string(),
        );
        meta
    }
}
