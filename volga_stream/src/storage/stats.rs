use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Serialize};

/// Lightweight, backend-agnostic storage statistics for debugging/perf tuning.
///
/// This is intentionally NOT tied to any metrics backend yet; callers can snapshot and export later.
#[derive(Debug, Default)]
pub struct StorageStats {
    // Tenant gauges.
    hot_batches: AtomicUsize,
    hot_bytes: AtomicUsize,
    work_bytes: AtomicUsize,
    state_bytes: AtomicUsize,
    batch_bytes: AtomicUsize,
    control_bytes: AtomicUsize,

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
    pub fn new() -> Arc<Self> {
        Arc::new(StorageStats::default())
    }

    /// A process-global instance for now (prefer worker-owned stats when possible).
    pub fn global() -> Arc<Self> {
        static STATS: OnceLock<Arc<StorageStats>> = OnceLock::new();
        STATS.get_or_init(|| Arc::new(StorageStats::default())).clone()
    }

    pub fn snapshot(&self) -> StorageStatsSnapshot {
        StorageStatsSnapshot {
            hot_batches: self.hot_batches.load(Ordering::Relaxed),
            hot_bytes: self.hot_bytes.load(Ordering::Relaxed),
            work_bytes: self.work_bytes.load(Ordering::Relaxed),
            state_bytes: self.state_bytes.load(Ordering::Relaxed),
            batch_bytes: self.batch_bytes.load(Ordering::Relaxed),
            control_bytes: self.control_bytes.load(Ordering::Relaxed),
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

    pub fn on_hot_put(&self, bytes: usize) {
        self.hot_batches.fetch_add(1, Ordering::Relaxed);
        self.hot_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn on_hot_remove(&self, bytes: usize) {
        self.hot_batches.fetch_sub(1, Ordering::Relaxed);
        self.hot_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn on_hot_clear(&self) {
        self.hot_batches.store(0, Ordering::Relaxed);
        self.hot_bytes.store(0, Ordering::Relaxed);
    }

    pub fn on_hot_add(&self, bytes: usize) {
        self.hot_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn on_hot_sub(&self, bytes: usize) {
        self.hot_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn on_work_add(&self, bytes: usize) {
        self.work_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn on_work_sub(&self, bytes: usize) {
        self.work_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn on_state_add(&self, bytes: usize) {
        self.state_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn on_state_sub(&self, bytes: usize) {
        self.state_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn on_batch_add(&self, bytes: usize) {
        self.batch_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn on_batch_sub(&self, bytes: usize) {
        self.batch_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn on_control_add(&self, bytes: usize) {
        self.control_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn on_control_sub(&self, bytes: usize) {
        self.control_bytes.fetch_sub(bytes, Ordering::Relaxed);
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
    pub hot_batches: usize,
    pub hot_bytes: usize,
    pub work_bytes: usize,
    pub state_bytes: usize,
    pub batch_bytes: usize,
    pub control_bytes: usize,

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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StorageStatsDelta {
    pub hot_batches: i64,
    pub hot_bytes: i64,
    pub work_bytes: i64,
    pub state_bytes: i64,
    pub batch_bytes: i64,
    pub control_bytes: i64,

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStatsReport {
    pub snapshot: StorageStatsSnapshot,
    pub delta: StorageStatsDelta,
}

#[derive(Debug, Clone)]
pub struct StorageStatsPoller {
    stats: Arc<StorageStats>,
    last: StorageStatsSnapshot,
}

impl StorageStatsPoller {
    pub fn new(stats: Arc<StorageStats>) -> Self {
        let last = stats.snapshot();
        Self { stats, last }
    }

    pub fn poll(&mut self) -> StorageStatsReport {
        let cur = self.stats.snapshot();
        let delta = StorageStatsDelta {
            hot_batches: cur.hot_batches as i64 - self.last.hot_batches as i64,
            hot_bytes: cur.hot_bytes as i64 - self.last.hot_bytes as i64,
            work_bytes: cur.work_bytes as i64 - self.last.work_bytes as i64,
            state_bytes: cur.state_bytes as i64 - self.last.state_bytes as i64,
            batch_bytes: cur.batch_bytes as i64 - self.last.batch_bytes as i64,
            control_bytes: cur.control_bytes as i64 - self.last.control_bytes as i64,

            pressure_relief_runs: cur.pressure_relief_runs.saturating_sub(self.last.pressure_relief_runs),
            pressure_planned_buckets: cur
                .pressure_planned_buckets
                .saturating_sub(self.last.pressure_planned_buckets),
            pressure_dumped_buckets: cur
                .pressure_dumped_buckets
                .saturating_sub(self.last.pressure_dumped_buckets),

            dump_calls: cur.dump_calls.saturating_sub(self.last.dump_calls),
            dump_published: cur.dump_published.saturating_sub(self.last.dump_published),
            dump_written_segments: cur
                .dump_written_segments
                .saturating_sub(self.last.dump_written_segments),
            dump_written_bytes: cur
                .dump_written_bytes
                .saturating_sub(self.last.dump_written_bytes),
            dump_seconds: (cur.dump_seconds - self.last.dump_seconds).max(0.0),

            compact_calls: cur.compact_calls.saturating_sub(self.last.compact_calls),
            compact_published: cur.compact_published.saturating_sub(self.last.compact_published),
            compact_seconds: (cur.compact_seconds - self.last.compact_seconds).max(0.0),
        };
        self.last = cur.clone();
        StorageStatsReport {
            snapshot: cur,
            delta,
        }
    }
}

fn nanos_to_seconds_f64(nanos: u64) -> f64 {
    (nanos as f64) / 1_000_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poller_reports_snapshot_and_delta() {
        let stats = StorageStats::new();
        let mut poller = StorageStatsPoller::new(stats.clone());

        stats.on_hot_put(10);
        stats.on_dump_start();
        stats.on_dump_finish(true, 1_000_000_000);

        let r = poller.poll();
        assert_eq!(r.snapshot.hot_batches, 1);
        assert_eq!(r.delta.hot_batches, 1);
        assert_eq!(r.delta.dump_calls, 1);
        assert_eq!(r.delta.dump_published, 1);
        assert!(r.delta.dump_seconds >= 1.0);

        let r2 = poller.poll();
        assert_eq!(r2.delta.dump_calls, 0);
        assert_eq!(r2.delta.hot_batches, 0);
    }
}




