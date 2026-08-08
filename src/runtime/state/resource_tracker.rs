//! Worker-scoped state resource accounting (quotas / pressure).
//!
//! Admission for *new* charges may signal pressure; maintenance / watermark
//! paths must not wait on this tracker (deadlock avoidance).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::runtime::operators::window::model::StateNamespace;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceKind {
    /// Live operator state (InMem maps, remote logical retention, etc.).
    RetainedState,
    /// Optional local cache (Foyer); unused by InMem.
    Cache,
}

#[derive(Debug, Default)]
pub struct StateResourceTracker {
    /// Soft total across all namespaces (0 = unlimited).
    soft_limit_bytes: AtomicU64,
    total_bytes: AtomicU64,
    per_ns: Mutex<HashMap<Vec<u8>, u64>>,
}

impl StateResourceTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_soft_limit_bytes(limit: u64) -> Self {
        let t = Self::new();
        t.soft_limit_bytes.store(limit, Ordering::Relaxed);
        t
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    pub fn soft_limit_bytes(&self) -> Option<u64> {
        match self.soft_limit_bytes.load(Ordering::Relaxed) {
            0 => None,
            n => Some(n),
        }
    }

    /// Returns false if soft limit would be exceeded (admission denied).
    /// Never blocks; callers must not wait on this for GC progress.
    pub fn try_charge(&self, ns: &StateNamespace, _kind: ResourceKind, bytes: u64) -> bool {
        if bytes == 0 {
            return true;
        }
        if let Some(limit) = self.soft_limit_bytes() {
            let cur = self.total_bytes.load(Ordering::Relaxed);
            if cur.saturating_add(bytes) > limit {
                return false;
            }
        }
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
        let mut map = self.per_ns.lock().expect("resource tracker");
        *map.entry(ns.bytes.clone()).or_default() += bytes;
        true
    }

    pub fn charge(&self, ns: &StateNamespace, kind: ResourceKind, bytes: u64) {
        let _ = self.try_charge(ns, kind, bytes);
    }

    pub fn release(&self, ns: &StateNamespace, _kind: ResourceKind, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.total_bytes.fetch_sub(bytes.min(self.total_bytes()), Ordering::Relaxed);
        let mut map = self.per_ns.lock().expect("resource tracker");
        if let Some(v) = map.get_mut(&ns.bytes) {
            *v = v.saturating_sub(bytes);
        }
    }

    pub fn over_soft_limit(&self) -> bool {
        match self.soft_limit_bytes() {
            Some(limit) => self.total_bytes() > limit,
            None => false,
        }
    }
}
