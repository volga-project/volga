use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use metrics::counter;

use crate::common::message::Message;
use crate::common::MAX_WATERMARK_VALUE;
use crate::runtime::metrics::{
    record_task_histogram, set_task_gauge, MetricsLabels, LABEL_PIPELINE_ID, LABEL_TASK_ID,
    LABEL_WORKER_ID, METRIC_STREAM_TASK_BACKPRESSURED_TIME_MS_PER_SECOND,
    METRIC_STREAM_TASK_BUSY_TIME_MS_PER_SECOND, METRIC_STREAM_TASK_BYTES_RECV,
    METRIC_STREAM_TASK_BYTES_SENT, METRIC_STREAM_TASK_CHECKPOINT_BARRIER_PROPAGATION_MS,
    METRIC_STREAM_TASK_IDLE_TIME_MS_PER_SECOND, METRIC_STREAM_TASK_MESSAGES_RECV,
    METRIC_STREAM_TASK_MESSAGES_SENT, METRIC_STREAM_TASK_PATH_LATENCY,
    METRIC_STREAM_TASK_RECORDS_RECV, METRIC_STREAM_TASK_RECORDS_SENT,
    METRIC_STREAM_TASK_WATERMARK_LAG_MS, METRIC_STREAM_TASK_WM_PROPAGATION_MS,
};
use crate::runtime::operators::operator::MessageStream;
use crate::runtime::VertexId;

use super::task::StreamTask;

/// Accumulates Flink-style task time metrics within a report window.
///
/// Idle = waiting on the input stream (`next()`), not operator compute such as
/// Window `insert_batch` / `process_due`. Backpressured = exclusive tx-queue
/// wait via [`BackpressureTracker`]; busy = residual wall time so the three
/// ms/s gauges sum to ~1000.
#[derive(Debug, Default)]
pub(super) struct TaskTimeMetrics {
    idle_ns: AtomicU64,
    backpressure_tracker: Arc<crate::transport::batch_channel::BackpressureTracker>,
}

impl TaskTimeMetrics {
    pub(super) fn add_idle_ns(&self, ns: u64) {
        self.idle_ns.fetch_add(ns, Ordering::Relaxed);
    }

    pub(super) fn backpressure_tracker(
        &self,
    ) -> Arc<crate::transport::batch_channel::BackpressureTracker> {
        self.backpressure_tracker.clone()
    }

    /// Scale to ms-per-second and reset. Returns `(busy, idle, backpressured)`.
    pub(super) fn take_ms_per_second(&self, window: Duration) -> (f64, f64, f64) {
        let idle = self.idle_ns.swap(0, Ordering::Relaxed);
        let bp = self.backpressure_tracker.take_ns();
        let window_ms = window.as_secs_f64() * 1000.0;
        if window_ms <= 0.0 {
            return (0.0, 0.0, 0.0);
        }
        let idle_ms = (idle as f64 / 1_000_000.0).min(window_ms);
        let mut bp_ms = bp as f64 / 1_000_000.0;
        if idle_ms + bp_ms > window_ms {
            bp_ms = (window_ms - idle_ms).max(0.0);
        }
        let busy_ms = (window_ms - idle_ms - bp_ms).max(0.0);
        let scale = 1000.0 / window_ms;
        (busy_ms * scale, idle_ms * scale, bp_ms * scale)
    }
}

/// Idle = time spent in `input.next()`, which is wait-for-mailbox (plus cheap
/// preprocess). Operator compute after the mailbox wait returns is not included.
pub(super) fn input_with_idle_tracking(
    mut input: MessageStream,
    metrics: Arc<TaskTimeMetrics>,
) -> MessageStream {
    Box::pin(async_stream::stream! {
        loop {
            let start = Instant::now();
            let item = input.next().await;
            metrics.add_idle_ns(start.elapsed().as_nanos() as u64);
            match item {
                Some(msg) => yield msg,
                None => break,
            }
        }
    })
}

impl StreamTask {
    pub(super) fn record_histogram(
        name: &'static str,
        value_ms: f64,
        vertex_id: &VertexId,
        labels: Option<&MetricsLabels>,
    ) {
        record_task_histogram(name, value_ms, vertex_id.as_ref(), labels);
    }

    pub(super) fn record_control_propagation(
        vertex_id: &VertexId,
        message: &Message,
        labels: Option<&MetricsLabels>,
    ) {
        let Some(stamp) = message.ingest_timestamp() else {
            return;
        };
        let delay_ms = Self::now_ms().saturating_sub(stamp) as f64;
        match message {
            Message::Watermark(_) => {
                Self::record_histogram(
                    METRIC_STREAM_TASK_WM_PROPAGATION_MS,
                    delay_ms,
                    vertex_id,
                    labels,
                );
            }
            Message::CheckpointBarrier(_) => {
                Self::record_histogram(
                    METRIC_STREAM_TASK_CHECKPOINT_BARRIER_PROPAGATION_MS,
                    delay_ms,
                    vertex_id,
                    labels,
                );
            }
            _ => {}
        }
    }

    pub(super) fn set_watermark_lag_gauge(
        vertex_id: &VertexId,
        watermark_value: u64,
        labels: Option<&MetricsLabels>,
    ) {
        if watermark_value == 0 || watermark_value == MAX_WATERMARK_VALUE {
            return;
        }
        let lag_ms = Self::now_ms().saturating_sub(watermark_value) as f64;
        set_task_gauge(
            METRIC_STREAM_TASK_WATERMARK_LAG_MS,
            lag_ms,
            vertex_id.as_ref(),
            labels,
        );
    }

    pub(super) fn record_metrics(
        vertex_id: VertexId,
        message: &Message,
        recv_or_send: bool,
        labels: Option<&MetricsLabels>,
    ) {
        let is_control_message =
            matches!(message, Message::Watermark(_) | Message::CheckpointBarrier(_));

        if is_control_message {
            return;
        }

        if recv_or_send {
            if let Some(labels) = labels {
                counter!(
                    METRIC_STREAM_TASK_MESSAGES_RECV,
                    LABEL_TASK_ID => vertex_id.clone(),
                    LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
                    LABEL_WORKER_ID => labels.worker_id.clone()
                )
                .increment(1);
                counter!(
                    METRIC_STREAM_TASK_RECORDS_RECV,
                    LABEL_TASK_ID => vertex_id.clone(),
                    LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
                    LABEL_WORKER_ID => labels.worker_id.clone()
                )
                .increment(message.num_records() as u64);
                counter!(
                    METRIC_STREAM_TASK_BYTES_RECV,
                    LABEL_TASK_ID => vertex_id.clone(),
                    LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
                    LABEL_WORKER_ID => labels.worker_id.clone()
                )
                .increment(message.get_memory_size() as u64);
            } else {
                counter!(METRIC_STREAM_TASK_MESSAGES_RECV, LABEL_TASK_ID => vertex_id.clone())
                    .increment(1);
                counter!(METRIC_STREAM_TASK_RECORDS_RECV, LABEL_TASK_ID => vertex_id.clone())
                    .increment(message.num_records() as u64);
                counter!(METRIC_STREAM_TASK_BYTES_RECV, LABEL_TASK_ID => vertex_id.clone())
                    .increment(message.get_memory_size() as u64);
            }

            if let Some(ingest_timestamp) = message.ingest_timestamp() {
                let path_latency = Self::now_ms().saturating_sub(ingest_timestamp);
                Self::record_histogram(
                    METRIC_STREAM_TASK_PATH_LATENCY,
                    path_latency as f64,
                    &vertex_id,
                    labels,
                );
            }
        } else if let Some(labels) = labels {
            counter!(
                METRIC_STREAM_TASK_MESSAGES_SENT,
                LABEL_TASK_ID => vertex_id.clone(),
                LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
                LABEL_WORKER_ID => labels.worker_id.clone()
            )
            .increment(1);
            counter!(
                METRIC_STREAM_TASK_RECORDS_SENT,
                LABEL_TASK_ID => vertex_id.clone(),
                LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
                LABEL_WORKER_ID => labels.worker_id.clone()
            )
            .increment(message.num_records() as u64);
            counter!(
                METRIC_STREAM_TASK_BYTES_SENT,
                LABEL_TASK_ID => vertex_id.clone(),
                LABEL_PIPELINE_ID => labels.pipeline_id.clone(),
                LABEL_WORKER_ID => labels.worker_id.clone()
            )
            .increment(message.get_memory_size() as u64);
        } else {
            counter!(METRIC_STREAM_TASK_MESSAGES_SENT, LABEL_TASK_ID => vertex_id.clone())
                .increment(1);
            counter!(METRIC_STREAM_TASK_RECORDS_SENT, LABEL_TASK_ID => vertex_id.clone())
                .increment(message.num_records() as u64);
            counter!(METRIC_STREAM_TASK_BYTES_SENT, LABEL_TASK_ID => vertex_id.clone())
                .increment(message.get_memory_size() as u64);
        }
    }

    pub(super) fn report_task_time_metrics(
        metrics: &TaskTimeMetrics,
        window_start: &mut Instant,
        vertex_id: &VertexId,
        labels: Option<&MetricsLabels>,
        current_watermark: &AtomicU64,
    ) {
        let elapsed = window_start.elapsed();
        if elapsed < Duration::from_secs(1) {
            return;
        }
        let (busy, idle, bp) = metrics.take_ms_per_second(elapsed);
        set_task_gauge(
            METRIC_STREAM_TASK_BUSY_TIME_MS_PER_SECOND,
            busy,
            vertex_id.as_ref(),
            labels,
        );
        set_task_gauge(
            METRIC_STREAM_TASK_IDLE_TIME_MS_PER_SECOND,
            idle,
            vertex_id.as_ref(),
            labels,
        );
        set_task_gauge(
            METRIC_STREAM_TASK_BACKPRESSURED_TIME_MS_PER_SECOND,
            bp,
            vertex_id.as_ref(),
            labels,
        );
        Self::set_watermark_lag_gauge(
            vertex_id,
            current_watermark.load(Ordering::Relaxed),
            labels,
        );
        *window_start = Instant::now();
    }
}
