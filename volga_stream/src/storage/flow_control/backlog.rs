use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::{Mutex, Notify};

/// Bytes-only backlog budget for pipeline backpressure.
#[derive(Debug)]
pub struct BacklogBudget {
    limit_bytes: usize,
    used_bytes: AtomicUsize,
    gate: Mutex<()>,
    notify: Notify,
}

impl BacklogBudget {
    pub fn new(limit_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            limit_bytes: limit_bytes.max(1),
            used_bytes: AtomicUsize::new(0),
            gate: Mutex::new(()),
            notify: Notify::new(),
        })
    }

    pub fn limit_bytes(&self) -> usize {
        self.limit_bytes
    }

    pub fn used_bytes(&self) -> usize {
        self.used_bytes.load(Ordering::Relaxed)
    }

    pub async fn acquire(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        loop {
            let _guard = self.gate.lock().await;
            let used = self.used_bytes.load(Ordering::Relaxed);
            if used.saturating_add(bytes) <= self.limit_bytes {
                self.used_bytes.fetch_add(bytes, Ordering::Relaxed);
                return;
            }
            drop(_guard);
            self.notify.notified().await;
        }
    }

    pub fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let mut current = self.used_bytes.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_sub(bytes);
            match self.used_bytes.compare_exchange(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        self.notify.notify_waiters();
    }
}
