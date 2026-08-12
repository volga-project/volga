//! Window-operator metrics: write helpers + registry-backed API snapshot.
//!
//! Hot path / maintain writes go through shared task helpers into the metrics
//! fanout (Prometheus + in-process registry). API snapshots are built by
//! visiting that registry on poll.

use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::TimestampMillisecondArray;

use crate::runtime::metrics::{
    collect_task_metric_values, increment_task_counter, record_task_histogram, MetricsLabels,
};
use serde::{Deserialize, Serialize};

pub const METRIC_WO_STATE_RAW_COUNT: &str = "volga_wo_state_raw_count";
pub const METRIC_WO_STATE_RAW_BYTES: &str = "volga_wo_state_raw_bytes";
pub const METRIC_WO_STATE_TILES_COUNT: &str = "volga_wo_state_tiles_count";
pub const METRIC_WO_STATE_TILES_BYTES: &str = "volga_wo_state_tiles_bytes";
pub const METRIC_WO_STATE_TRIGGERS_COUNT: &str = "volga_wo_state_triggers_count";
pub const METRIC_WO_STATE_TRIGGERS_BYTES: &str = "volga_wo_state_triggers_bytes";
pub const METRIC_WO_STATE_KEY_STATES_COUNT: &str = "volga_wo_state_key_states_count";
pub const METRIC_WO_STATE_KEY_STATES_BYTES: &str = "volga_wo_state_key_states_bytes";

pub const METRIC_WO_WM_PROCESS_MS: &str = "volga_wo_wm_process_ms";
pub const METRIC_WO_RESULT_FRESHNESS_MS: &str = "volga_wo_result_freshness_ms";
pub const METRIC_WO_MAINTAIN_MS: &str = "volga_wo_maintain_ms";
pub const METRIC_WO_MAINTAIN_PRUNED_ROWS: &str = "volga_wo_maintain_pruned_rows";
pub const METRIC_WO_LATE_DROPPED_ROWS: &str = "volga_wo_late_dropped_rows";

const WO_SNAPSHOT_METRIC_NAMES: &[&'static str] = &[
    METRIC_WO_STATE_RAW_COUNT,
    METRIC_WO_STATE_RAW_BYTES,
    METRIC_WO_STATE_TILES_COUNT,
    METRIC_WO_STATE_TILES_BYTES,
    METRIC_WO_STATE_TRIGGERS_COUNT,
    METRIC_WO_STATE_TRIGGERS_BYTES,
    METRIC_WO_STATE_KEY_STATES_COUNT,
    METRIC_WO_STATE_KEY_STATES_BYTES,
    METRIC_WO_LATE_DROPPED_ROWS,
    METRIC_WO_MAINTAIN_PRUNED_ROWS,
];

/// API / `WorkerSnapshot` payload (from registry visit on poll).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowOperatorMetricsSnapshot {
    pub raw_count: u64,
    pub raw_bytes: u64,
    pub tiles_count: u64,
    pub tiles_bytes: u64,
    pub triggers_count: u64,
    pub triggers_bytes: u64,
    pub key_states_count: u64,
    pub key_states_bytes: u64,
    pub late_dropped_rows: u64,
    pub pruned_rows: u64,
}

pub fn add_late_dropped(task_id: &str, labels: &MetricsLabels, rows: u64) {
    increment_task_counter(METRIC_WO_LATE_DROPPED_ROWS, rows, task_id, Some(labels));
}

pub fn add_pruned(task_id: &str, labels: &MetricsLabels, rows: u64) {
    increment_task_counter(METRIC_WO_MAINTAIN_PRUNED_ROWS, rows, task_id, Some(labels));
}

pub fn record_wm_process_ms(task_id: &str, labels: &MetricsLabels, ms: f64) {
    record_task_histogram(METRIC_WO_WM_PROCESS_MS, ms, task_id, Some(labels));
}

pub fn record_maintain_ms(task_id: &str, labels: &MetricsLabels, ms: f64) {
    record_task_histogram(METRIC_WO_MAINTAIN_MS, ms, task_id, Some(labels));
}

/// Max hist samples per emit batch (stride across rows; all rows if shorter).
const RESULT_FRESHNESS_SAMPLE_COUNT: usize = 16;

/// Record result freshness from a stride sample of the batch (≤16 hist samples).
pub fn record_result_freshness(task_id: &str, labels: &MetricsLabels, ts: &TimestampMillisecondArray) {
    let len = ts.len();
    if len == 0 {
        return;
    }
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let samples = RESULT_FRESHNESS_SAMPLE_COUNT.min(len);
    let labels = Some(labels);
    let values = ts.values();
    for k in 0..samples {
        let i = k * len / samples;
        let freshness = now_ms.saturating_sub(values[i]).max(0) as f64;
        record_task_histogram(METRIC_WO_RESULT_FRESHNESS_MS, freshness, task_id, labels);
    }
}

fn gauge_u64(values: &crate::runtime::metrics::TaskMetricValues, name: &str) -> u64 {
    values.gauges.get(name).copied().unwrap_or(0.0) as u64
}

fn counter_u64(values: &crate::runtime::metrics::TaskMetricValues, name: &str) -> u64 {
    values.counters.get(name).copied().unwrap_or(0)
}

/// Build API snapshot for a WO task from the in-process metrics registry.
pub fn collect_window_operator_snapshot(
    task_id: &str,
    labels: Option<&MetricsLabels>,
) -> WindowOperatorMetricsSnapshot {
    let values = collect_task_metric_values(task_id, labels, WO_SNAPSHOT_METRIC_NAMES);
    WindowOperatorMetricsSnapshot {
        raw_count: gauge_u64(&values, METRIC_WO_STATE_RAW_COUNT),
        raw_bytes: gauge_u64(&values, METRIC_WO_STATE_RAW_BYTES),
        tiles_count: gauge_u64(&values, METRIC_WO_STATE_TILES_COUNT),
        tiles_bytes: gauge_u64(&values, METRIC_WO_STATE_TILES_BYTES),
        triggers_count: gauge_u64(&values, METRIC_WO_STATE_TRIGGERS_COUNT),
        triggers_bytes: gauge_u64(&values, METRIC_WO_STATE_TRIGGERS_BYTES),
        key_states_count: gauge_u64(&values, METRIC_WO_STATE_KEY_STATES_COUNT),
        key_states_bytes: gauge_u64(&values, METRIC_WO_STATE_KEY_STATES_BYTES),
        late_dropped_rows: counter_u64(&values, METRIC_WO_LATE_DROPPED_ROWS),
        pruned_rows: counter_u64(&values, METRIC_WO_MAINTAIN_PRUNED_ROWS),
    }
}
