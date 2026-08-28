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
/// Records waiting on one input channel (`source_task_id` → this task).
pub const METRIC_STREAM_TASK_RX_QUEUED_RECORDS: &str = "volga_stream_task_rx_queued_records";
/// Event-time lag: `wall_now_ms − current_watermark` (omit for 0 / MAX).
/// Refreshed on watermark advance and on the task metrics tick so idle lag grows.
pub const METRIC_STREAM_TASK_WATERMARK_LAG_MS: &str = "volga_stream_task_watermark_lag_ms";
/// Control-path delay for watermarks: `recv − source/inject stamp`.
pub const METRIC_STREAM_TASK_WM_PROPAGATION_MS: &str = "volga_stream_task_wm_propagation_ms";
/// Control-path delay for checkpoint barriers: `recv − source/inject stamp`.
pub const METRIC_STREAM_TASK_CHECKPOINT_BARRIER_PROPAGATION_MS: &str =
    "volga_stream_task_checkpoint_barrier_propagation_ms";
/// Wait from first barrier seen to full upstream alignment (non-sources).
pub const METRIC_STREAM_TASK_CHECKPOINT_ALIGN_WAIT_MS: &str =
    "volga_stream_task_checkpoint_align_wait_ms";
/// Local snapshot + report RPC wall time.
pub const METRIC_STREAM_TASK_CHECKPOINT_DURATION_MS: &str =
    "volga_stream_task_checkpoint_duration_ms";
/// Per-checkpoint snapshot payload size (bytes).
pub const METRIC_STREAM_TASK_CHECKPOINT_PAYLOAD_BYTES: &str =
    "volga_stream_task_checkpoint_payload_bytes";
pub const METRIC_STREAM_TASK_CHECKPOINT_SUCCESS: &str = "volga_stream_task_checkpoint_success";
pub const METRIC_STREAM_TASK_CHECKPOINT_FAILED: &str = "volga_stream_task_checkpoint_failed";
/// Flink-style task time metrics (ms in the last second; busy+idle+bp ≈ 1000).
/// Busy is residual wall time; idle is input-stream wait (not operator compute);
/// bp is tx-queue block wait.
pub const METRIC_STREAM_TASK_BUSY_TIME_MS_PER_SECOND: &str =
    "volga_stream_task_busy_time_ms_per_second";
pub const METRIC_STREAM_TASK_IDLE_TIME_MS_PER_SECOND: &str =
    "volga_stream_task_idle_time_ms_per_second";
pub const METRIC_STREAM_TASK_BACKPRESSURED_TIME_MS_PER_SECOND: &str =
    "volga_stream_task_backpressured_time_ms_per_second";
/// Per-edge TCP pump time spent in `write` (window / peer not reading), ms in the last second.
pub const METRIC_STREAM_TASK_TRANSPORT_WRITE_BLOCK_MS_PER_SECOND: &str =
    "volga_stream_task_transport_write_block_ms_per_second";
/// Per-edge TCP disconnects. Close is fatal; this counts drops for Grafana.
pub const METRIC_STREAM_TASK_TRANSPORT_DISCONNECTS: &str =
    "volga_stream_task_transport_disconnects";

/// Job-level checkpoint wall time (master `started_at` → complete only).
pub const METRIC_CHECKPOINT_DURATION_MS: &str = "volga_checkpoint_duration_ms";
pub const METRIC_CHECKPOINT_COMPLETED: &str = "volga_checkpoint_completed";
pub const METRIC_CHECKPOINT_FAILED: &str = "volga_checkpoint_failed";

// Worker-derived (poll-time) metrics
pub const METRIC_WORKER_BACKPRESSURE_MAX: &str = "volga_worker_backpressure_max";
pub const METRIC_WORKER_RSS_BYTES: &str = "volga_worker_rss_bytes";
pub const METRIC_WORKER_VIRTUAL_BYTES: &str = "volga_worker_virtual_bytes";

/// Records accepted by a Count sink (payload discarded).
pub const METRIC_SINK_RECORDS_WRITTEN: &str = "volga_sink_records_written";

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
pub const LABEL_SOURCE_TASK_ID: &str = "source_task_id";
pub const LABEL_WORKER_ID: &str = "worker_id";
pub const LABEL_OPERATOR_ID: &str = "operator_id";
pub const LABEL_PIPELINE_ID: &str = "pipeline_id";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsLabels {
    pub pipeline_id: String,
    pub worker_id: String,
}

// Histogram bucket boundaries, milliseconds (Prometheus also emits a trailing +Inf bucket).
pub const LATENCY_BUCKET_BOUNDARIES: [f64; 18] = [
    1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
    10_000.0, 30_000.0, 60_000.0, 120_000.0, 180_000.0, 300_000.0,
];
/// Finite Prom buckets plus the trailing `+Inf` cumulative count.
pub const LATENCY_HISTOGRAM_LEN: usize = LATENCY_BUCKET_BOUNDARIES.len() + 1;
/// Prom buckets for checkpoint payload size (bytes). 256 B … 1 GiB.
pub const PAYLOAD_BUCKET_BOUNDARIES: [f64; 12] = [
    256.0,
    1024.0,
    4096.0,
    16_384.0,
    65_536.0,
    262_144.0,
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
    67_108_864.0,
    268_435_456.0,
    1_073_741_824.0,
];
