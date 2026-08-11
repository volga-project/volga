//! Window-operator metrics: sampled snapshot type, Prom names, and emit helpers.

use metrics::gauge;
use serde::{Deserialize, Serialize};

use crate::runtime::metrics::{
    MetricsLabels, LABEL_PIPELINE_ID, LABEL_VERTEX_ID, LABEL_WORKER_ID,
};

pub const METRIC_WO_STATE_RAW_COUNT: &str = "volga_wo_state_raw_count";
pub const METRIC_WO_STATE_RAW_BYTES: &str = "volga_wo_state_raw_bytes";
pub const METRIC_WO_STATE_TILES_COUNT: &str = "volga_wo_state_tiles_count";
pub const METRIC_WO_STATE_TILES_BYTES: &str = "volga_wo_state_tiles_bytes";
pub const METRIC_WO_STATE_TRIGGERS_COUNT: &str = "volga_wo_state_triggers_count";
pub const METRIC_WO_STATE_TRIGGERS_BYTES: &str = "volga_wo_state_triggers_bytes";
pub const METRIC_WO_STATE_KEY_STATES_COUNT: &str = "volga_wo_state_key_states_count";
pub const METRIC_WO_STATE_KEY_STATES_BYTES: &str = "volga_wo_state_key_states_bytes";

/// Sampled WO metrics for API snapshots and Prom gauges (one task namespace).
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
}

impl WindowOperatorMetrics {
    /// Emit Prom gauges (poll-time, set-to-current).
    pub fn emit(&self, vertex_id: &str, labels: &MetricsLabels) {
        let vertex = vertex_id.to_string();
        gauge!(
            METRIC_WO_STATE_RAW_COUNT,
            LABEL_VERTEX_ID => vertex.clone(),
            LABEL_WORKER_ID => labels.worker_id.clone(),
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
        )
        .set(self.raw_count as f64);
        gauge!(
            METRIC_WO_STATE_RAW_BYTES,
            LABEL_VERTEX_ID => vertex.clone(),
            LABEL_WORKER_ID => labels.worker_id.clone(),
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
        )
        .set(self.raw_bytes as f64);
        gauge!(
            METRIC_WO_STATE_TILES_COUNT,
            LABEL_VERTEX_ID => vertex.clone(),
            LABEL_WORKER_ID => labels.worker_id.clone(),
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
        )
        .set(self.tiles_count as f64);
        gauge!(
            METRIC_WO_STATE_TILES_BYTES,
            LABEL_VERTEX_ID => vertex.clone(),
            LABEL_WORKER_ID => labels.worker_id.clone(),
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
        )
        .set(self.tiles_bytes as f64);
        gauge!(
            METRIC_WO_STATE_TRIGGERS_COUNT,
            LABEL_VERTEX_ID => vertex.clone(),
            LABEL_WORKER_ID => labels.worker_id.clone(),
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
        )
        .set(self.triggers_count as f64);
        gauge!(
            METRIC_WO_STATE_TRIGGERS_BYTES,
            LABEL_VERTEX_ID => vertex.clone(),
            LABEL_WORKER_ID => labels.worker_id.clone(),
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
        )
        .set(self.triggers_bytes as f64);
        gauge!(
            METRIC_WO_STATE_KEY_STATES_COUNT,
            LABEL_VERTEX_ID => vertex.clone(),
            LABEL_WORKER_ID => labels.worker_id.clone(),
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
        )
        .set(self.key_states_count as f64);
        gauge!(
            METRIC_WO_STATE_KEY_STATES_BYTES,
            LABEL_VERTEX_ID => vertex,
            LABEL_WORKER_ID => labels.worker_id.clone(),
            LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
        )
        .set(self.key_states_bytes as f64);
    }
}
