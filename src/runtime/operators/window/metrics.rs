//! Window-operator Prom metric names and poll-time gauge emission.

use metrics::gauge;

use crate::runtime::metrics::{
    MetricsLabels, LABEL_PIPELINE_ID, LABEL_VERTEX_ID, LABEL_WORKER_ID,
};
use crate::runtime::observability::snapshot_types::WindowOperatorMetrics;

pub const METRIC_WO_STATE_RAW_COUNT: &str = "volga_wo_state_raw_count";
pub const METRIC_WO_STATE_RAW_BYTES: &str = "volga_wo_state_raw_bytes";
pub const METRIC_WO_STATE_TILES_COUNT: &str = "volga_wo_state_tiles_count";
pub const METRIC_WO_STATE_TILES_BYTES: &str = "volga_wo_state_tiles_bytes";
pub const METRIC_WO_STATE_TRIGGERS_COUNT: &str = "volga_wo_state_triggers_count";
pub const METRIC_WO_STATE_TRIGGERS_BYTES: &str = "volga_wo_state_triggers_bytes";

/// Emit Prom gauges for sampled [`WindowOperatorMetrics`] (poll-time, set-to-current).
pub fn emit_window_operator_metrics(
    vertex_id: &str,
    state: &WindowOperatorMetrics,
    labels: &MetricsLabels,
) {
    let vertex = vertex_id.to_string();
    gauge!(
        METRIC_WO_STATE_RAW_COUNT,
        LABEL_VERTEX_ID => vertex.clone(),
        LABEL_WORKER_ID => labels.worker_id.clone(),
        LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
    )
    .set(state.raw_count as f64);
    gauge!(
        METRIC_WO_STATE_RAW_BYTES,
        LABEL_VERTEX_ID => vertex.clone(),
        LABEL_WORKER_ID => labels.worker_id.clone(),
        LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
    )
    .set(state.raw_bytes as f64);
    gauge!(
        METRIC_WO_STATE_TILES_COUNT,
        LABEL_VERTEX_ID => vertex.clone(),
        LABEL_WORKER_ID => labels.worker_id.clone(),
        LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
    )
    .set(state.tiles_count as f64);
    gauge!(
        METRIC_WO_STATE_TILES_BYTES,
        LABEL_VERTEX_ID => vertex.clone(),
        LABEL_WORKER_ID => labels.worker_id.clone(),
        LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
    )
    .set(state.tiles_bytes as f64);
    gauge!(
        METRIC_WO_STATE_TRIGGERS_COUNT,
        LABEL_VERTEX_ID => vertex.clone(),
        LABEL_WORKER_ID => labels.worker_id.clone(),
        LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
    )
    .set(state.triggers_count as f64);
    gauge!(
        METRIC_WO_STATE_TRIGGERS_BYTES,
        LABEL_VERTEX_ID => vertex,
        LABEL_WORKER_ID => labels.worker_id.clone(),
        LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
    )
    .set(state.triggers_bytes as f64);
}
