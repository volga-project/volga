use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Serialize};

/// Lightweight, backend-agnostic storage statistics for debugging/perf tuning.
///
/// This is intentionally NOT tied to any metrics backend yet; callers can snapshot and export later.
#[derive(Debug, Default)]
pub struct StorageStats {
    // In-mem batch cache gauges.
    inmem_batches: AtomicUsize,
    inmem_bytes: AtomicUsize,

    // Pressure relief.
    pressure_relief_runs: AtomicU64,
    pressure_planned_buckets: AtomicU64,
    pressure_dumped_buckets: AtomicU64,

    // Dumping to store.
    dump_calls: AtomicU64,
    dump_published: AtomicU64,
    dump_written_segments: AtomicU64,
    dump_written_bytes: AtomicU64,
    dump_seconds_nanos: AtomicU64,

    // Compaction (in-mem).
    compact_calls: AtomicU64,
    compact_published: AtomicU64,
    compact_seconds_nanos: AtomicU64,
}

impl StorageStats {
    /// A process-global instance for now (future: move into WorkerStorageContext).
    pub fn global() -> Arc<Self> {
        static STATS: OnceLock<Arc<StorageStats>> = OnceLock::new();
        STATS.get_or_init(|| Arc::new(StorageStats::default())).clone()
    }

    pub fn snapshot(&self) -> StorageStatsSnapshot {
        StorageStatsSnapshot {
            inmem_batches: self.inmem_batches.load(Ordering::Relaxed),
            inmem_bytes: self.inmem_bytes.load(Ordering::Relaxed),
            pressure_relief_runs: self.pressure_relief_runs.load(Ordering::Relaxed),
            pressure_planned_buckets: self.pressure_planned_buckets.load(Ordering::Relaxed),
            pressure_dumped_buckets: self.pressure_dumped_buckets.load(Ordering::Relaxed),
            dump_calls: self.dump_calls.load(Ordering::Relaxed),
            dump_published: self.dump_published.load(Ordering::Relaxed),
            dump_written_segments: self.dump_written_segments.load(Ordering::Relaxed),
            dump_written_bytes: self.dump_written_bytes.load(Ordering::Relaxed),
            dump_seconds: nanos_to_seconds_f64(self.dump_seconds_nanos.load(Ordering::Relaxed)),
            compact_calls: self.compact_calls.load(Ordering::Relaxed),
            compact_published: self.compact_published.load(Ordering::Relaxed),
            compact_seconds: nanos_to_seconds_f64(self.compact_seconds_nanos.load(Ordering::Relaxed)),
        }
    }

    // ----------------- update helpers -----------------

    pub fn on_inmem_put(&self, bytes: usize) {
        self.inmem_batches.fetch_add(1, Ordering::Relaxed);
        self.inmem_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn on_inmem_remove(&self, bytes: usize) {
        self.inmem_batches.fetch_sub(1, Ordering::Relaxed);
        self.inmem_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn on_inmem_clear(&self) {
        self.inmem_batches.store(0, Ordering::Relaxed);
        self.inmem_bytes.store(0, Ordering::Relaxed);
    }

    pub fn on_pressure_relief_start(&self, planned_buckets: usize) {
        self.pressure_relief_runs.fetch_add(1, Ordering::Relaxed);
        self.pressure_planned_buckets
            .fetch_add(planned_buckets as u64, Ordering::Relaxed);
    }

    pub fn on_pressure_relief_dumped(&self, dumped_buckets: usize) {
        self.pressure_dumped_buckets
            .fetch_add(dumped_buckets as u64, Ordering::Relaxed);
    }

    pub fn on_dump_start(&self) {
        self.dump_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn on_dump_written(&self, segments: usize, bytes: usize) {
        self.dump_written_segments
            .fetch_add(segments as u64, Ordering::Relaxed);
        self.dump_written_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn on_dump_finish(&self, published: bool, duration_nanos: u64) {
        if published {
            self.dump_published.fetch_add(1, Ordering::Relaxed);
        }
        self.dump_seconds_nanos
            .fetch_add(duration_nanos, Ordering::Relaxed);
    }

    pub fn on_compact_finish(&self, published: bool, duration_nanos: u64) {
        self.compact_calls.fetch_add(1, Ordering::Relaxed);
        if published {
            self.compact_published.fetch_add(1, Ordering::Relaxed);
        }
        self.compact_seconds_nanos
            .fetch_add(duration_nanos, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStatsSnapshot {
    pub inmem_batches: usize,
    pub inmem_bytes: usize,

    pub pressure_relief_runs: u64,
    pub pressure_planned_buckets: u64,
    pub pressure_dumped_buckets: u64,

    pub dump_calls: u64,
    pub dump_published: u64,
    pub dump_written_segments: u64,
    pub dump_written_bytes: u64,
    pub dump_seconds: f64,

    pub compact_calls: u64,
    pub compact_published: u64,
    pub compact_seconds: f64,
}

fn nanos_to_seconds_f64(nanos: u64) -> f64 {
    (nanos as f64) / 1_000_000_000.0
}




