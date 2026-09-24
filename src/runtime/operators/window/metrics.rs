//! Window-operator metrics: write helpers + registry-backed API snapshot.
//!
//! Hot path / maintain writes go through shared task helpers into the metrics
//! fanout (Prometheus + in-process registry). API snapshots are built by
//! visiting that registry on poll.

use metrics::{counter, gauge, histogram};

use crate::runtime::metrics::{
    collect_task_metric_values, increment_task_counter, record_task_histogram, MetricsLabels,
    LABEL_PIPELINE_ID, LABEL_TASK_ID, LABEL_WORKER_ID,
};
use serde::{Deserialize, Serialize};

pub const METRIC_WO_INMEM_STATE_RAW_COUNT: &str = "volga_wo_inmem_state_raw_count";
pub const METRIC_WO_INMEM_STATE_RAW_BYTES: &str = "volga_wo_inmem_state_raw_bytes";
pub const METRIC_WO_INMEM_STATE_TILES_COUNT: &str = "volga_wo_inmem_state_tiles_count";
pub const METRIC_WO_INMEM_STATE_TILES_BYTES: &str = "volga_wo_inmem_state_tiles_bytes";
pub const METRIC_WO_INMEM_STATE_TRIGGERS_COUNT: &str = "volga_wo_inmem_state_triggers_count";
pub const METRIC_WO_INMEM_STATE_TRIGGERS_BYTES: &str = "volga_wo_inmem_state_triggers_bytes";
pub const METRIC_WO_INMEM_STATE_KEY_STATES_COUNT: &str = "volga_wo_inmem_state_key_states_count";
pub const METRIC_WO_INMEM_STATE_KEY_STATES_BYTES: &str = "volga_wo_inmem_state_key_states_bytes";

pub const METRIC_WO_INGEST_MS: &str = "volga_wo_ingest_ms";
pub const METRIC_WO_WM_PROCESS_MS: &str = "volga_wo_wm_process_ms";
pub const METRIC_WO_INMEM_PRUNED_ROWS: &str = "volga_wo_inmem_pruned_rows";
pub const METRIC_WO_LATE_DROPPED_ROWS: &str = "volga_wo_late_dropped_rows";

pub const METRIC_WO_SCYLLA_CALLS: &str = "volga_wo_scylla_calls_total";
pub const METRIC_WO_SCYLLA_FAILURES: &str = "volga_wo_scylla_failures_total";
pub const METRIC_WO_SCYLLA_STATEMENTS: &str = "volga_wo_scylla_statements_total";
pub const METRIC_WO_SCYLLA_LATENCY_MS: &str = "volga_wo_scylla_latency_ms";
pub const METRIC_WO_SCYLLA_ROWS: &str = "volga_wo_scylla_rows_total";
pub const METRIC_WO_SCYLLA_PAYLOAD_BYTES: &str = "volga_wo_scylla_payload_bytes_total";
pub const METRIC_WO_SCYLLA_MAINTAIN_PHASE_MS: &str = "volga_wo_scylla_maintain_phase_ms";

pub const METRIC_WO_SCYLLA_KG_BUCKETS_LIVE: &str = "volga_wo_scylla_kg_buckets_live";
pub const METRIC_WO_SCYLLA_KEYS: &str = "volga_wo_scylla_keys";
pub const METRIC_WO_SCYLLA_KG_BUCKETS_EXPIRED: &str = "volga_wo_scylla_kg_buckets_expired";
pub const METRIC_WO_SCYLLA_TILE_VERSION_ROWS: &str = "volga_wo_scylla_tile_version_rows";
pub const METRIC_WO_SCYLLA_KEY_STATE_ROWS: &str = "volga_wo_scylla_key_state_rows";

pub const METRIC_WO_SCYLLA_RAW_PARTITIONS_DELETED: &str = "volga_wo_scylla_raw_partitions_deleted";
pub const METRIC_WO_SCYLLA_TILE_RANGE_DELETES: &str = "volga_wo_scylla_tile_range_deletes";
pub const METRIC_WO_SCYLLA_TRIGGER_SHARD_DELETES: &str = "volga_wo_scylla_trigger_shard_deletes";
pub const METRIC_WO_SCYLLA_VERSION_ROWS_DELETED: &str = "volga_wo_scylla_version_rows_deleted";

const LABEL_OP: &str = "op";
const LABEL_PHASE: &str = "phase";
const LABEL_TABLE: &str = "table";

const WO_SNAPSHOT_METRIC_NAMES: &[&'static str] = &[
    METRIC_WO_INMEM_STATE_RAW_COUNT,
    METRIC_WO_INMEM_STATE_RAW_BYTES,
    METRIC_WO_INMEM_STATE_TILES_COUNT,
    METRIC_WO_INMEM_STATE_TILES_BYTES,
    METRIC_WO_INMEM_STATE_TRIGGERS_COUNT,
    METRIC_WO_INMEM_STATE_TRIGGERS_BYTES,
    METRIC_WO_INMEM_STATE_KEY_STATES_COUNT,
    METRIC_WO_INMEM_STATE_KEY_STATES_BYTES,
    METRIC_WO_LATE_DROPPED_ROWS,
    METRIC_WO_INMEM_PRUNED_ROWS,
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
    increment_task_counter(METRIC_WO_INMEM_PRUNED_ROWS, rows, task_id, Some(labels));
}

pub fn record_ingest_ms(task_id: &str, labels: &MetricsLabels, ms: f64) {
    record_task_histogram(METRIC_WO_INGEST_MS, ms, task_id, Some(labels));
}

pub fn record_wm_process_ms(task_id: &str, labels: &MetricsLabels, ms: f64) {
    record_task_histogram(METRIC_WO_WM_PROCESS_MS, ms, task_id, Some(labels));
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
        raw_count: gauge_u64(&values, METRIC_WO_INMEM_STATE_RAW_COUNT),
        raw_bytes: gauge_u64(&values, METRIC_WO_INMEM_STATE_RAW_BYTES),
        tiles_count: gauge_u64(&values, METRIC_WO_INMEM_STATE_TILES_COUNT),
        tiles_bytes: gauge_u64(&values, METRIC_WO_INMEM_STATE_TILES_BYTES),
        triggers_count: gauge_u64(&values, METRIC_WO_INMEM_STATE_TRIGGERS_COUNT),
        triggers_bytes: gauge_u64(&values, METRIC_WO_INMEM_STATE_TRIGGERS_BYTES),
        key_states_count: gauge_u64(&values, METRIC_WO_INMEM_STATE_KEY_STATES_COUNT),
        key_states_bytes: gauge_u64(&values, METRIC_WO_INMEM_STATE_KEY_STATES_BYTES),
        late_dropped_rows: counter_u64(&values, METRIC_WO_LATE_DROPPED_ROWS),
        pruned_rows: counter_u64(&values, METRIC_WO_INMEM_PRUNED_ROWS),
    }
}

fn task_label_values(task_id: &str, labels: &MetricsLabels) -> (String, String, String) {
    (
        task_id.to_string(),
        labels.pipeline_id.clone(),
        labels.worker_id.clone(),
    )
}

/// One store-trait call. `ok` increments `calls_total`; a failure increments
/// `failures_total` and does not increment `calls_total`.
pub fn record_scylla_call(
    task_id: &str,
    labels: &MetricsLabels,
    op: &'static str,
    ok: bool,
    stmts: u64,
    rows: u64,
    bytes: u64,
    ms: f64,
) {
    let (task, pipeline, worker) = task_label_values(task_id, labels);
    if ok {
        counter!(
            METRIC_WO_SCYLLA_CALLS,
            LABEL_TASK_ID => task.clone(),
            LABEL_PIPELINE_ID => pipeline.clone(),
            LABEL_WORKER_ID => worker.clone(),
            LABEL_OP => op,
        )
        .increment(1);
    } else {
        counter!(
            METRIC_WO_SCYLLA_FAILURES,
            LABEL_TASK_ID => task.clone(),
            LABEL_PIPELINE_ID => pipeline.clone(),
            LABEL_WORKER_ID => worker.clone(),
            LABEL_OP => op,
        )
        .increment(1);
    }
    if stmts > 0 {
        counter!(
            METRIC_WO_SCYLLA_STATEMENTS,
            LABEL_TASK_ID => task.clone(),
            LABEL_PIPELINE_ID => pipeline.clone(),
            LABEL_WORKER_ID => worker.clone(),
            LABEL_OP => op,
        )
        .increment(stmts);
    }
    if rows > 0 {
        counter!(
            METRIC_WO_SCYLLA_ROWS,
            LABEL_TASK_ID => task.clone(),
            LABEL_PIPELINE_ID => pipeline.clone(),
            LABEL_WORKER_ID => worker.clone(),
            LABEL_OP => op,
        )
        .increment(rows);
    }
    if bytes > 0 {
        counter!(
            METRIC_WO_SCYLLA_PAYLOAD_BYTES,
            LABEL_TASK_ID => task.clone(),
            LABEL_PIPELINE_ID => pipeline.clone(),
            LABEL_WORKER_ID => worker.clone(),
            LABEL_OP => op,
        )
        .increment(bytes);
    }
    histogram!(
        METRIC_WO_SCYLLA_LATENCY_MS,
        LABEL_TASK_ID => task,
        LABEL_PIPELINE_ID => pipeline,
        LABEL_WORKER_ID => worker,
        LABEL_OP => op,
    )
    .record(ms);
}

pub fn record_scylla_maintain_phase(
    task_id: &str,
    labels: &MetricsLabels,
    phase: &'static str,
    ms: f64,
) {
    let (task, pipeline, worker) = task_label_values(task_id, labels);
    histogram!(
        METRIC_WO_SCYLLA_MAINTAIN_PHASE_MS,
        LABEL_TASK_ID => task,
        LABEL_PIPELINE_ID => pipeline,
        LABEL_WORKER_ID => worker,
        LABEL_PHASE => phase,
    )
    .record(ms);
}

pub fn set_scylla_gauge(task_id: &str, labels: &MetricsLabels, name: &'static str, value: f64) {
    let (task, pipeline, worker) = task_label_values(task_id, labels);
    gauge!(
        name,
        LABEL_TASK_ID => task,
        LABEL_PIPELINE_ID => pipeline,
        LABEL_WORKER_ID => worker,
    )
    .set(value);
}

/// Delete counters. A zero delta still registers the series so an idle tick
/// stays flat at zero.
pub fn add_scylla_counter(task_id: &str, labels: &MetricsLabels, name: &'static str, delta: u64) {
    let (task, pipeline, worker) = task_label_values(task_id, labels);
    counter!(
        name,
        LABEL_TASK_ID => task,
        LABEL_PIPELINE_ID => pipeline,
        LABEL_WORKER_ID => worker,
    )
    .increment(delta);
}

pub fn add_scylla_version_rows_deleted(
    task_id: &str,
    labels: &MetricsLabels,
    table: &'static str,
    delta: u64,
) {
    let (task, pipeline, worker) = task_label_values(task_id, labels);
    counter!(
        METRIC_WO_SCYLLA_VERSION_ROWS_DELETED,
        LABEL_TASK_ID => task,
        LABEL_PIPELINE_ID => pipeline,
        LABEL_WORKER_ID => worker,
        LABEL_TABLE => table,
    )
    .increment(delta);
}
