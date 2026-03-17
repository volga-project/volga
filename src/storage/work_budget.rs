use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::Notify;

/// A simple byte-budget admission controller for "work memory" (hydration/read working sets).
#[derive(Debug)]
pub struct WorkBudget {
    limit_bytes: usize,
    used_bytes: AtomicUsize,
    notify: Notify,
}

impl WorkBudget {
    pub fn new(limit_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            limit_bytes: limit_bytes.max(1),
            used_bytes: AtomicUsize::new(0),
            notify: Notify::new(),
        })
    }

    pub fn limit_bytes(&self) -> usize {
        self.limit_bytes
    }

    pub fn used_bytes(&self) -> usize {
        self.used_bytes.load(Ordering::Relaxed)
    }

    /// Acquire a lease for `bytes`.
    ///
    /// If `bytes` exceeds the limit, we clamp it to `limit_bytes` to guarantee progress
    /// while still preventing concurrent oversized loads from stacking up.
    pub async fn acquire(self: &Arc<Self>, bytes: usize) -> WorkLease {
        let want = bytes.min(self.limit_bytes);
        if want == 0 {
            return WorkLease::empty(self.clone());
        }

        loop {
            let used = self.used_bytes.load(Ordering::Acquire);
            if used.saturating_add(want) > self.limit_bytes {
                self.notify.notified().await;
                continue;
            }
            match self.used_bytes.compare_exchange_weak(
                used,
                used + want,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return WorkLease::new(self.clone(), want),
                Err(_) => continue,
            }
        }
    }

    fn release(&self, bytes: usize) {
        self.used_bytes.fetch_sub(bytes, Ordering::AcqRel);
        self.notify.notify_waiters();
    }
}

#[derive(Debug)]
struct WorkLeaseInner {
    budget: Arc<WorkBudget>,
    bytes: usize,
}

impl Drop for WorkLeaseInner {
    fn drop(&mut self) {
        if self.bytes != 0 {
            self.budget.release(self.bytes);
        }
    }
}

/// Cloneable lease; the budget is released when the last clone is dropped.
#[derive(Debug, Clone)]
pub struct WorkLease(Arc<WorkLeaseInner>);

impl WorkLease {
    fn new(budget: Arc<WorkBudget>, bytes: usize) -> Self {
        Self(Arc::new(WorkLeaseInner { budget, bytes }))
    }

    fn empty(budget: Arc<WorkBudget>) -> Self {
        Self::new(budget, 0)
    }

    pub fn bytes(&self) -> usize {
        self.0.bytes
    }
}

