use std::collections::VecDeque;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::runtime::observability::snapshot_types::PipelineSnapshot;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSnapshotEntry {
    /// Wall clock milliseconds since Unix epoch.
    pub ts_ms: u64,
    /// Monotonic sequence number within one process for this history instance.
    pub seq: u64,
    pub snapshot: PipelineSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDerivedStats {
    pub snapshot_count: usize,
    pub first_ts_ms: Option<u64>,
    pub last_ts_ms: Option<u64>,
}

impl Default for PipelineDerivedStats {
    fn default() -> Self {
        Self {
            snapshot_count: 0,
            first_ts_ms: None,
            last_ts_ms: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSnapshotHistory {
    retention_ms: u64,
    next_seq: u64,
    entries: VecDeque<PipelineSnapshotEntry>,
}

impl PipelineSnapshotHistory {
    pub fn new(retention: Duration) -> Self {
        Self {
            retention_ms: retention.as_millis() as u64,
            next_seq: 1,
            entries: VecDeque::new(),
        }
    }

    pub fn retention(&self) -> Duration {
        Duration::from_millis(self.retention_ms)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn latest(&self) -> Option<&PipelineSnapshotEntry> {
        self.entries.back()
    }

    pub fn iter(&self) -> impl Iterator<Item = &PipelineSnapshotEntry> {
        self.entries.iter()
    }

    pub fn push_now(&mut self, snapshot: PipelineSnapshot) -> u64 {
        self.push_at(snapshot, SystemTime::now())
    }

    pub fn push_at(&mut self, snapshot: PipelineSnapshot, ts: SystemTime) -> u64 {
        let ts_ms = system_time_to_ms(ts);
        self.evict_older_than(ts_ms.saturating_sub(self.retention_ms));

        let seq = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);

        self.entries.push_back(PipelineSnapshotEntry { ts_ms, seq, snapshot });
        seq
    }

    pub fn evict_older_than(&mut self, min_ts_ms: u64) {
        while let Some(front) = self.entries.front() {
            if front.ts_ms >= min_ts_ms {
                break;
            }
            self.entries.pop_front();
        }
    }

    pub fn derived_stats(&self) -> PipelineDerivedStats {
        let first_ts_ms = self.entries.front().map(|e| e.ts_ms);
        let last_ts_ms = self.entries.back().map(|e| e.ts_ms);
        PipelineDerivedStats {
            snapshot_count: self.entries.len(),
            first_ts_ms,
            last_ts_ms,
        }
    }
}

impl Default for PipelineSnapshotHistory {
    fn default() -> Self {
        // Reasonable default for in-proc use; call sites should override from spec.
        Self::new(Duration::from_secs(10 * 60))
    }
}

fn system_time_to_ms(ts: SystemTime) -> u64 {
    ts.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

