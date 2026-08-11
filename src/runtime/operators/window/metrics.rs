//! Window-operator metrics: API snapshot + Prom recording helpers.
//!
//! Prom counters/histograms are recorded on the operator lifecycle path.
//! [`WindowOperatorMetrics`] is the poll/API snapshot (store sizes); size gauges
//! are published when that snapshot is taken (operator-owned, not worker `emit`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::TimestampMillisecondArray;
use metrics::gauge;
use serde::{Deserialize, Serialize};

use crate::runtime::metrics::{
    increment_vertex_counter, record_vertex_histogram, MetricsLabels, LABEL_PIPELINE_ID,
    LABEL_VERTEX_ID, LABEL_WORKER_ID,
};
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::VertexId;

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

/// Sampled WO store sizes for API snapshots (one task namespace).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowOperatorMetrics {
    pub raw_count: u64,
    pub raw_bytes: u64,
    pub tiles_count: u64,
    pub tiles_bytes: u64,
    pub triggers_count: u64,
    pub triggers_bytes: u64,
    pub key_states_count: u64,
    pub key_states_bytes: u64,
    /// Cumulative late-dropped rows since task open (API; Prom increments on ingest).
    pub late_dropped_rows: u64,
    /// Cumulative maintain-pruned rows since task open (API; Prom increments on maintain).
    pub pruned_rows: u64,
}

/// Per-task WO metrics handle: labels + hot-path Prom recording.
#[derive(Debug)]
pub struct WindowMetrics {
    vertex_id: VertexId,
    labels: MetricsLabels,
    late_dropped_rows: AtomicU64,
    pruned_rows: AtomicU64,
}

impl WindowMetrics {
    pub fn from_context(context: &RuntimeContext) -> Option<Arc<Self>> {
        let labels = context.metrics_labels()?;
        Some(Arc::new(Self {
            vertex_id: context.vertex_id_arc(),
            labels,
            late_dropped_rows: AtomicU64::new(0),
            pruned_rows: AtomicU64::new(0),
        }))
    }

    pub fn vertex_id(&self) -> &str {
        self.vertex_id.as_ref()
    }

    pub fn labels(&self) -> &MetricsLabels {
        &self.labels
    }

    pub fn add_late_dropped(&self, rows: u64) {
        if rows == 0 {
            return;
        }
        self.late_dropped_rows.fetch_add(rows, Ordering::Relaxed);
        increment_vertex_counter(
            METRIC_WO_LATE_DROPPED_ROWS,
            rows,
            self.vertex_id.as_ref(),
            Some(&self.labels),
        );
    }

    pub fn add_pruned(&self, rows: u64) {
        if rows == 0 {
            return;
        }
        self.pruned_rows.fetch_add(rows, Ordering::Relaxed);
        increment_vertex_counter(
            METRIC_WO_MAINTAIN_PRUNED_ROWS,
            rows,
            self.vertex_id.as_ref(),
            Some(&self.labels),
        );
    }

    pub fn record_wm_process_ms(&self, ms: f64) {
        record_vertex_histogram(
            METRIC_WO_WM_PROCESS_MS,
            ms,
            self.vertex_id.as_ref(),
            Some(&self.labels),
        );
    }

    pub fn record_maintain_ms(&self, ms: f64) {
        record_vertex_histogram(
            METRIC_WO_MAINTAIN_MS,
            ms,
            self.vertex_id.as_ref(),
            Some(&self.labels),
        );
    }

    /// Record result freshness as min/mean/max over the batch (3 hist samples).
    pub fn record_result_freshness(&self, ts: &TimestampMillisecondArray) {
        let n = ts.len();
        if n == 0 {
            return;
        }
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let mut min_f = i64::MAX;
        let mut max_f = 0i64;
        let mut sum = 0i64;
        for i in 0..n {
            let f = now_ms.saturating_sub(ts.value(i)).max(0);
            min_f = min_f.min(f);
            max_f = max_f.max(f);
            sum = sum.saturating_add(f);
        }
        let mean = sum / n as i64;
        let vertex = self.vertex_id.as_ref();
        let labels = Some(&self.labels);
        for sample in [min_f, mean, max_f] {
            record_vertex_histogram(
                METRIC_WO_RESULT_FRESHNESS_MS,
                sample as f64,
                vertex,
                labels,
            );
        }
    }

    /// Publish store-size gauges from a snapshot (called when the operator samples for API).
    pub fn publish_state_sizes(&self, state: &WindowOperatorMetrics) {
        let vertex = self.vertex_id.as_ref().to_string();
        let worker = self.labels.worker_id.clone();
        let pipeline = self.labels.pipeline_id.clone();
        gauge!(
            METRIC_WO_STATE_RAW_COUNT,
            LABEL_VERTEX_ID => vertex.clone(),
            LABEL_WORKER_ID => worker.clone(),
            LABEL_PIPELINE_ID => pipeline.clone(),
        )
        .set(state.raw_count as f64);
        gauge!(
            METRIC_WO_STATE_RAW_BYTES,
            LABEL_VERTEX_ID => vertex.clone(),
            LABEL_WORKER_ID => worker.clone(),
            LABEL_PIPELINE_ID => pipeline.clone(),
        )
        .set(state.raw_bytes as f64);
        gauge!(
            METRIC_WO_STATE_TILES_COUNT,
            LABEL_VERTEX_ID => vertex.clone(),
            LABEL_WORKER_ID => worker.clone(),
            LABEL_PIPELINE_ID => pipeline.clone(),
        )
        .set(state.tiles_count as f64);
        gauge!(
            METRIC_WO_STATE_TILES_BYTES,
            LABEL_VERTEX_ID => vertex.clone(),
            LABEL_WORKER_ID => worker.clone(),
            LABEL_PIPELINE_ID => pipeline.clone(),
        )
        .set(state.tiles_bytes as f64);
        gauge!(
            METRIC_WO_STATE_TRIGGERS_COUNT,
            LABEL_VERTEX_ID => vertex.clone(),
            LABEL_WORKER_ID => worker.clone(),
            LABEL_PIPELINE_ID => pipeline.clone(),
        )
        .set(state.triggers_count as f64);
        gauge!(
            METRIC_WO_STATE_TRIGGERS_BYTES,
            LABEL_VERTEX_ID => vertex.clone(),
            LABEL_WORKER_ID => worker.clone(),
            LABEL_PIPELINE_ID => pipeline.clone(),
        )
        .set(state.triggers_bytes as f64);
        gauge!(
            METRIC_WO_STATE_KEY_STATES_COUNT,
            LABEL_VERTEX_ID => vertex.clone(),
            LABEL_WORKER_ID => worker.clone(),
            LABEL_PIPELINE_ID => pipeline.clone(),
        )
        .set(state.key_states_count as f64);
        gauge!(
            METRIC_WO_STATE_KEY_STATES_BYTES,
            LABEL_VERTEX_ID => vertex,
            LABEL_WORKER_ID => worker,
            LABEL_PIPELINE_ID => pipeline,
        )
        .set(state.key_states_bytes as f64);
    }

    pub fn counter_totals(&self) -> (u64, u64) {
        (
            self.late_dropped_rows.load(Ordering::Relaxed),
            self.pruned_rows.load(Ordering::Relaxed),
        )
    }
}
