//! Shared stop/stats/wake handles for checkpointable sources (datagen).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use crate::runtime::VertexId;

#[derive(Debug, Default)]
pub struct SourceTaskControl {
    pub stop: Arc<AtomicBool>,
    pub records_generated: Arc<AtomicU64>,
    /// Wakes interruptible source waits (rate-limit sleep) when a CP barrier is triggered.
    pub checkpoint_wake: Arc<Notify>,
    /// After wake, source poll returns Continue so StreamTask can inject the barrier first.
    pub prefer_control: Arc<AtomicBool>,
    pub task_index: i32,
}

impl SourceTaskControl {
    pub fn new(task_index: i32) -> Self {
        Self {
            stop: Arc::new(AtomicBool::new(false)),
            records_generated: Arc::new(AtomicU64::new(0)),
            checkpoint_wake: Arc::new(Notify::new()),
            prefer_control: Arc::new(AtomicBool::new(false)),
            task_index,
        }
    }

    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    pub fn wake_checkpoint(&self) {
        self.prefer_control.store(true, Ordering::SeqCst);
        self.checkpoint_wake.notify_waiters();
    }

    pub fn records_generated(&self) -> u64 {
        self.records_generated.load(Ordering::SeqCst)
    }
}

#[derive(Debug, Default, Clone)]
pub struct SourceControlRegistry {
    inner: Arc<Mutex<HashMap<VertexId, Arc<SourceTaskControl>>>>,
}

impl SourceControlRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, vertex_id: VertexId, control: Arc<SourceTaskControl>) {
        self.inner.lock().unwrap().insert(vertex_id, control);
    }

    pub fn clear(&self) {
        self.inner.lock().unwrap().clear();
    }

    pub fn request_stop_all(&self) {
        for control in self.inner.lock().unwrap().values() {
            control.request_stop();
        }
    }

    pub fn wake_checkpoint(&self, vertex_id: &VertexId) {
        if let Some(control) = self.inner.lock().unwrap().get(vertex_id) {
            control.wake_checkpoint();
        }
    }

    pub fn prefer_control_flag(&self, vertex_id: VertexId) -> Option<Arc<AtomicBool>> {
        self.inner
            .lock()
            .unwrap()
            .get(&vertex_id)
            .map(|control| control.prefer_control.clone())
    }

    pub fn snapshot_stats(&self) -> Vec<(VertexId, i32, u64)> {
        let guard = self.inner.lock().unwrap();
        let mut out: Vec<_> = guard
            .iter()
            .map(|(vertex_id, control)| {
                (
                    vertex_id.clone(),
                    control.task_index,
                    control.records_generated(),
                )
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }
}
