//! Metric and label name constants.

use serde::{Deserialize, Serialize};

// Stream task metrics
pub const METRIC_STREAM_TASK_MESSAGES_SENT: &str = "volga_stream_task_messages_sent";
pub const METRIC_STREAM_TASK_MESSAGES_RECV: &str = "volga_stream_task_messages_recv";
pub const METRIC_STREAM_TASK_RECORDS_SENT: &str = "volga_stream_task_records_sent";
pub const METRIC_STREAM_TASK_RECORDS_RECV: &str = "volga_stream_task_records_recv";
pub const METRIC_STREAM_TASK_BYTES_SENT: &str = "volga_stream_task_bytes_sent";
pub const METRIC_STREAM_TASK_BYTES_RECV: &str = "volga_stream_task_bytes_recv";
/// Path latency histogram: `recv − ingest` (ms). Omit when ingest is absent.
pub const METRIC_STREAM_TASK_PATH_LATENCY: &str = "volga_stream_task_path_latency";
pub const METRIC_STREAM_TASK_PATH_LATENCY_99: &str = "volga_stream_task_path_latency_99";
pub const METRIC_STREAM_TASK_PATH_LATENCY_95: &str = "volga_stream_task_path_latency_95";
pub const METRIC_STREAM_TASK_PATH_LATENCY_50: &str = "volga_stream_task_path_latency_50";
pub const METRIC_STREAM_TASK_PATH_LATENCY_AVG: &str = "volga_stream_task_path_latency_avg";
pub const METRIC_STREAM_TASK_TX_QUEUE_SIZE: &str = "volga_stream_task_tx_queue_size";
pub const METRIC_STREAM_TASK_TX_QUEUE_REM: &str = "volga_stream_task_tx_queue_rem";
pub const METRIC_STREAM_TASK_BACKPRESSURE_RATIO: &str = "volga_stream_task_backpressure_ratio";
pub const METRIC_STREAM_TASK_BACKPRESSURE_MAX: &str = "volga_stream_task_backpressure_max";
/// Event-time lag: `wall_now_ms − current_watermark` (omit for 0 / MAX).
pub const METRIC_STREAM_TASK_WATERMARK_LAG_MS: &str = "volga_stream_task_watermark_lag_ms";
/// Hop delay for watermarks: `recv − create_stamp`.
pub const METRIC_STREAM_TASK_WM_PROPAGATION_MS: &str = "volga_stream_task_wm_propagation_ms";
/// Hop delay for checkpoint barriers: `recv − create_stamp`.
pub const METRIC_STREAM_TASK_BARRIER_PROPAGATION_MS: &str =
    "volga_stream_task_barrier_propagation_ms";
/// Wait from first barrier seen to full upstream alignment (non-sources).
pub const METRIC_STREAM_TASK_CHECKPOINT_ALIGN_WAIT_MS: &str =
    "volga_stream_task_checkpoint_align_wait_ms";
/// Local snapshot + report RPC wall time.
pub const METRIC_STREAM_TASK_CHECKPOINT_DURATION_MS: &str =
    "volga_stream_task_checkpoint_duration_ms";
pub const METRIC_STREAM_TASK_CHECKPOINT_PAYLOAD_BYTES: &str =
    "volga_stream_task_checkpoint_payload_bytes";
pub const METRIC_STREAM_TASK_CHECKPOINT_SUCCESS: &str = "volga_stream_task_checkpoint_success";
pub const METRIC_STREAM_TASK_CHECKPOINT_FAIL: &str = "volga_stream_task_checkpoint_fail";
/// Flink-style task time budget (≈ ms in the last second; sum ≈ 1000).
pub const METRIC_STREAM_TASK_BUSY_TIME_MS_PER_SECOND: &str =
    "volga_stream_task_busy_time_ms_per_second";
pub const METRIC_STREAM_TASK_IDLE_TIME_MS_PER_SECOND: &str =
    "volga_stream_task_idle_time_ms_per_second";
pub const METRIC_STREAM_TASK_BACKPRESSURED_TIME_MS_PER_SECOND: &str =
    "volga_stream_task_backpressured_time_ms_per_second";

/// Job-level checkpoint wall time (master `started_at` → complete/fail).
pub const METRIC_CHECKPOINT_DURATION_MS: &str = "volga_checkpoint_duration_ms";
pub const METRIC_CHECKPOINT_COMPLETED: &str = "volga_checkpoint_completed";
pub const METRIC_CHECKPOINT_FAILED: &str = "volga_checkpoint_failed";

// Worker-derived (poll-time) metrics
pub const METRIC_WORKER_BACKPRESSURE_MAX: &str = "volga_worker_backpressure_max";
pub const METRIC_WORKER_RSS_BYTES: &str = "volga_worker_rss_bytes";
pub const METRIC_WORKER_VIRTUAL_BYTES: &str = "volga_worker_virtual_bytes";

// Operator metrics
pub const METRIC_OPERATOR_MESSAGES_SENT: &str = "volga_operator_messages_sent";
pub const METRIC_OPERATOR_MESSAGES_RECV: &str = "volga_operator_messages_recv";
pub const METRIC_OPERATOR_RECORDS_SENT: &str = "volga_operator_records_sent";
pub const METRIC_OPERATOR_RECORDS_RECV: &str = "volga_operator_records_recv";
pub const METRIC_OPERATOR_BYTES_SENT: &str = "volga_operator_bytes_sent";
pub const METRIC_OPERATOR_BYTES_RECV: &str = "volga_operator_bytes_recv";
pub const METRIC_OPERATOR_PATH_LATENCY_99: &str = "volga_operator_path_latency_99";
pub const METRIC_OPERATOR_PATH_LATENCY_95: &str = "volga_operator_path_latency_95";
pub const METRIC_OPERATOR_PATH_LATENCY_50: &str = "volga_operator_path_latency_50";
pub const METRIC_OPERATOR_PATH_LATENCY_AVG: &str = "volga_operator_path_latency_avg";

// Label constants
pub const LABEL_TASK_ID: &str = "task_id";
pub const LABEL_TARGET_TASK_ID: &str = "target_task_id";
pub const LABEL_WORKER_ID: &str = "worker_id";
pub const LABEL_OPERATOR_ID: &str = "operator_id";
pub const LABEL_PIPELINE_ID: &str = "pipeline_id";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsLabels {
    pub pipeline_id: String,
    pub worker_id: String,
}


// Histogram bucket boundaries, milliseconds (Prometheus also emits a trailing +Inf bucket).
pub const LATENCY_BUCKET_BOUNDARIES: [f64; 12] = [1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0];
/// Finite Prom buckets plus the trailing `+Inf` cumulative count.
pub const LATENCY_HISTOGRAM_LEN: usize = LATENCY_BUCKET_BOUNDARIES.len() + 1;

