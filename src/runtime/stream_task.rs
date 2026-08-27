use crate::{
    common::MAX_WATERMARK_VALUE,
    runtime::{
        checkpoint::{SerializedCheckpoint, SerializedRestore},
        collector::Collector,
        execution_graph::ExecutionGraph,
        health::{WorkerFatalReason, WorkerHealth},
        metrics::{
            increment_task_counter, init_metrics, record_task_histogram, set_task_gauge,
            MetricsLabels, LABEL_PIPELINE_ID, LABEL_TASK_ID, LABEL_WORKER_ID,
            METRIC_STREAM_TASK_BACKPRESSURED_TIME_MS_PER_SECOND,
            METRIC_STREAM_TASK_BUSY_TIME_MS_PER_SECOND, METRIC_STREAM_TASK_BYTES_RECV,
            METRIC_STREAM_TASK_BYTES_SENT, METRIC_STREAM_TASK_CHECKPOINT_ALIGN_WAIT_MS,
            METRIC_STREAM_TASK_CHECKPOINT_BARRIER_PROPAGATION_MS,
            METRIC_STREAM_TASK_CHECKPOINT_DURATION_MS, METRIC_STREAM_TASK_CHECKPOINT_FAILED,
            METRIC_STREAM_TASK_CHECKPOINT_PAYLOAD_BYTES, METRIC_STREAM_TASK_CHECKPOINT_SUCCESS,
            METRIC_STREAM_TASK_IDLE_TIME_MS_PER_SECOND,
            METRIC_STREAM_TASK_MESSAGES_RECV, METRIC_STREAM_TASK_MESSAGES_SENT,
            METRIC_STREAM_TASK_PATH_LATENCY, METRIC_STREAM_TASK_RECORDS_RECV,
            METRIC_STREAM_TASK_RECORDS_SENT, METRIC_STREAM_TASK_WATERMARK_LAG_MS,
            METRIC_STREAM_TASK_WM_PROPAGATION_MS,
        },
        observability::{TaskMetadata, TaskSnapshot, StreamTaskStatus},
        operators::operator::{
            create_operator, MessageStream, OperatorConfig, OperatorPollResult, OperatorTrait,
            OperatorType,
        },
        runtime_context::RuntimeContext,
    },
    transport::transport_client::TransportClientConfig,
};
use anyhow::Result;
use futures::{FutureExt, Stream, StreamExt};
use async_stream::stream;
use metrics::counter;
use tokio::{task::JoinHandle, sync::Mutex, sync::watch, sync::mpsc};
use crate::transport::transport_client::DataReaderControl;
use std::collections::HashSet;
use crate::transport::transport_client::TransportClient;
use crate::common::message::{Message, WatermarkMessage};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU8, AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Accumulates Flink-style task time metrics within a report window.
///
/// Idle = waiting on input (or source fetch `Pending` / empty poll), not operator
/// compute such as Window `insert_batch` / `process_due`. Backpressured = exclusive
/// tx-queue wait via [`BackpressureTracker`] (same write path as BP ratio
/// gauges); busy = residual wall time so the three ms/s gauges sum to ~1000.
#[derive(Debug, Default)]
pub(crate) struct TaskTimeMetrics {
    idle_ns: AtomicU64,
    idle_wait: std::sync::Mutex<IdleWaitState>,
    backpressure_tracker: Arc<crate::transport::batch_channel::BackpressureTracker>,
}

#[derive(Debug, Default)]
struct IdleWaitState {
    waiters: u32,
    started: Option<Instant>,
}

struct IdleWaitGuard(Arc<TaskTimeMetrics>);

impl Drop for IdleWaitGuard {
    fn drop(&mut self) {
        let mut g = self.0.idle_wait.lock().expect("idle wait");
        g.waiters = g.waiters.saturating_sub(1);
        if g.waiters == 0 {
            if let Some(started) = g.started.take() {
                self.0
                    .idle_ns
                    .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
        }
    }
}

impl TaskTimeMetrics {
    fn begin_idle(self: &Arc<Self>) -> IdleWaitGuard {
        {
            let mut g = self.idle_wait.lock().expect("idle wait");
            if g.waiters == 0 {
                g.started = Some(Instant::now());
            }
            g.waiters = g.waiters.saturating_add(1);
        }
        IdleWaitGuard(Arc::clone(self))
    }

    /// Count time the future is `Pending` (waiting), not CPU work in `Ready` polls.
    async fn while_pending<T>(self: &Arc<Self>, fut: impl Future<Output = T>) -> T {
        tokio::pin!(fut);
        let mut wait = None;
        std::future::poll_fn(|cx| match fut.as_mut().poll(cx) {
            Poll::Pending => {
                if wait.is_none() {
                    wait = Some(self.begin_idle());
                }
                Poll::Pending
            }
            Poll::Ready(v) => {
                wait = None;
                Poll::Ready(v)
            }
        })
        .await
    }

    fn backpressure_tracker(&self) -> Arc<crate::transport::batch_channel::BackpressureTracker> {
        self.backpressure_tracker.clone()
    }

    /// Take accumulated idle nanos (includes an in-flight wait slice).
    fn take_idle_ns(&self) -> u64 {
        let mut g = self.idle_wait.lock().expect("idle wait");
        let mut total = self.idle_ns.swap(0, Ordering::Relaxed);
        if g.waiters > 0 {
            if let Some(started) = g.started.replace(Instant::now()) {
                total = total.saturating_add(started.elapsed().as_nanos() as u64);
            }
        }
        total
    }

    /// Scale to ms-per-second and reset. Returns `(busy, idle, backpressured)`.
    fn take_ms_per_second(&self, window: Duration) -> (f64, f64, f64) {
        let idle = self.take_idle_ns();
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

/// Records idle only while the input stream is `Pending` (waiting for a message).
struct IdleTimingStream {
    inner: MessageStream,
    metrics: Arc<TaskTimeMetrics>,
    wait: Option<IdleWaitGuard>,
}

fn idle_timing_stream(inner: MessageStream, metrics: Arc<TaskTimeMetrics>) -> MessageStream {
    Box::pin(IdleTimingStream {
        inner,
        metrics,
        wait: None,
    })
}

impl Stream for IdleTimingStream {
    type Item = Message;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Message>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Pending => {
                if this.wait.is_none() {
                    this.wait = Some(this.metrics.begin_idle());
                }
                Poll::Pending
            }
            Poll::Ready(item) => {
                this.wait = None;
                Poll::Ready(item)
            }
        }
    }
}
// serde imports removed; this module does not define serializable DTOs directly.
use crate::common::grpc::master::master_client as connect_master_client;
use crate::common::grpc::GrpcConfig;
use crate::runtime::master::server::master_service::master_service_client::MasterServiceClient;
use std::sync::atomic::AtomicBool;
use crate::runtime::VertexId;
use crate::runtime::watermark::WatermarkAssignConfig;
use crate::runtime::watermark::WatermarkManager;

pub static MESSAGE_TRACE_ENABLED: AtomicBool = AtomicBool::new(false);

// Watermark assignment + watermark merge/advancement live in `runtime/watermark/`.

#[derive(Debug)]
pub(crate) struct CheckpointAligner {
    upstream_count: usize,
    current_checkpoint: Option<u64>,
    seen_upstreams: HashSet<String>,
    /// Earliest create stamp among barriers for the current checkpoint (for propagation).
    inject_stamp: Option<u64>,
    align_started: Option<Instant>,
    reader_control: DataReaderControl,
}

impl CheckpointAligner {
    pub(crate) fn new(upstream_vertices: &[String], reader_control: DataReaderControl) -> Self {
        Self {
            upstream_count: upstream_vertices.len(),
            current_checkpoint: None,
            seen_upstreams: HashSet::new(),
            inject_stamp: None,
            align_started: None,
            reader_control,
        }
    }

    /// Returns `Some((checkpoint_id, inject_stamp, align_wait_ms))` when fully aligned.
    fn on_barrier(
        &mut self,
        barrier: &crate::common::message::CheckpointBarrierMessage,
        expected_attempt_id: u64,
    ) -> Result<Option<(u64, Option<u64>, f64)>, String> {
        if barrier.execution_attempt_id != expected_attempt_id {
            // Orphan from a previous attempt — drop without failing the new attempt.
            println!(
                "[CHECKPOINT] dropping stale barrier checkpoint_id={} attempt={} expected_attempt={}",
                barrier.checkpoint_id, barrier.execution_attempt_id, expected_attempt_id
            );
            return Ok(None);
        }

        let upstream_vertex_id = barrier.metadata.upstream_vertex_id.clone().ok_or_else(|| {
            format!(
                "checkpoint barrier missing upstream_vertex_id checkpoint_id={}",
                barrier.checkpoint_id
            )
        })?;

        let checkpoint_id = barrier.checkpoint_id;
        if self.current_checkpoint.is_none() {
            self.current_checkpoint = Some(checkpoint_id);
            self.inject_stamp = barrier.metadata.ingest_timestamp;
            self.align_started = Some(Instant::now());
        } else if let (Some(existing), Some(stamp)) =
            (self.inject_stamp, barrier.metadata.ingest_timestamp)
        {
            self.inject_stamp = Some(existing.min(stamp));
        } else if self.inject_stamp.is_none() {
            self.inject_stamp = barrier.metadata.ingest_timestamp;
        }
        if self.current_checkpoint != Some(checkpoint_id) {
            return Err(format!(
                "unexpected checkpoint barrier id={} while aligning {:?}",
                checkpoint_id, self.current_checkpoint
            ));
        }

        self.reader_control.block_upstream(&upstream_vertex_id);
        self.seen_upstreams.insert(upstream_vertex_id);

        if self.seen_upstreams.len() == self.upstream_count {
            let stamp = self.inject_stamp.take();
            let align_wait_ms = self
                .align_started
                .take()
                .map(|t| t.elapsed().as_secs_f64() * 1000.0)
                .unwrap_or(0.0);
            self.current_checkpoint = None;
            self.seen_upstreams.clear();
            return Ok(Some((checkpoint_id, stamp, align_wait_ms)));
        }
        Ok(None)
    }

    // Unblocking is done in StreamTask after checkpointing.
}

// Helper function to get current timestamp
fn timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .to_string()
}

// StreamTaskStatus/StreamTaskState moved to runtime/observability/snapshot_types.rs

#[derive(Debug)]
pub struct StreamTask {
    vertex_id: VertexId,
    runtime_context: RuntimeContext,
    metrics_labels: Option<MetricsLabels>,
    status: Arc<AtomicU8>,
    run_loop_handle: Option<JoinHandle<Result<()>>>,
    operator_config: OperatorConfig,
    transport_client_config: Option<TransportClientConfig>,
    execution_graph: ExecutionGraph,
    run_signal_sender: Option<watch::Sender<bool>>,
    close_signal_sender: Option<watch::Sender<bool>>,
    checkpoint_trigger_sender: Option<mpsc::UnboundedSender<u64>>,
    upstream_watermarks: Arc<Mutex<HashMap<String, u64>>>, // upstream_vertex_id -> watermark_value
    current_watermark: Arc<AtomicU64>,
    master_addr: Option<String>,
    restore_data: Option<SerializedRestore>,
    execution_attempt_id: u64,
    worker_health: Arc<WorkerHealth>,
}

impl StreamTask {
    pub fn new(
        vertex_id: VertexId,
        operator_config: OperatorConfig,
        transport_client_config: TransportClientConfig,
        runtime_context: RuntimeContext,
        execution_graph: ExecutionGraph,
        worker_health: Arc<WorkerHealth>,
        restore_data: Option<SerializedRestore>,
    ) -> Self {
        init_metrics();
        let master_addr = runtime_context
            .job_config()
            .get("master_addr")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let execution_attempt_id = runtime_context
            .job_config()
            .get("execution_attempt_id")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        let metrics_labels = runtime_context.metrics_labels();
        Self {
            vertex_id: vertex_id.clone(),
            runtime_context,
            metrics_labels,
            status: Arc::new(AtomicU8::new(StreamTaskStatus::Created as u8)),
            run_loop_handle: None,
            operator_config,
            transport_client_config: Some(transport_client_config),
            execution_graph,
            run_signal_sender: None,
            close_signal_sender: None,
            checkpoint_trigger_sender: None,
            upstream_watermarks: Arc::new(Mutex::new(HashMap::new())),
            current_watermark: Arc::new(AtomicU64::new(0)),
            master_addr,
            restore_data,
            execution_attempt_id,
            worker_health,
        }
    }

    async fn report_checkpoint_propagation(
        master_client: &mut Option<MasterServiceClient<tonic::transport::Channel>>,
        checkpoint_id: u64,
        vertex_id: &str,
        task_index: i32,
        execution_attempt_id: u64,
        phase: crate::runtime::master::server::master_service::CheckpointPropagationPhase,
    ) -> Result<()> {
        let Some(client) = master_client.as_mut() else {
            return Ok(());
        };
        let resp = client
            .report_checkpoint_propagation(tonic::Request::new(
                crate::runtime::master::server::master_service::ReportCheckpointPropagationRequest {
                    checkpoint_id,
                    vertex_id: vertex_id.to_string(),
                    task_index,
                    execution_attempt_id,
                    phase: phase as i32,
                },
            ))
            .await
            .map_err(|e| anyhow::anyhow!("report_checkpoint_propagation rpc failed: {e}"))?
            .into_inner();
        if !resp.success {
            return Err(anyhow::anyhow!(
                "report_checkpoint_propagation rejected: {}",
                resp.error_message
            ));
        }
        Ok(())
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn record_histogram(
        name: &'static str,
        value_ms: f64,
        vertex_id: &VertexId,
        labels: Option<&MetricsLabels>,
    ) {
        record_task_histogram(name, value_ms, vertex_id.as_ref(), labels);
    }

    fn record_control_propagation(
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

    fn set_watermark_lag_gauge(
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

    fn record_metrics(
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

            // Path latency: recv − source ingest (omit when ingest absent, e.g. WO outputs).
            if let Some(ingest_timestamp) = message.ingest_timestamp() {
                let path_latency = Self::now_ms().saturating_sub(ingest_timestamp);
                Self::record_histogram(
                    METRIC_STREAM_TASK_PATH_LATENCY,
                    path_latency as f64,
                    &vertex_id,
                    labels,
                );
            }
        } else {
            if let Some(labels) = labels {
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
    }

    // Create preprocessed input stream that handles watermark advancement + barrier alignment (blocks upstreams).
    // It never yields checkpoint barriers to the operator; instead it notifies the run loop via `aligned_checkpoint_sender`.
    pub(crate) fn create_preprocessed_input_stream(
        input_stream: MessageStream,
        vertex_id: VertexId,
        watermark_assign: Option<WatermarkAssignConfig>,
        upstream_vertices: Vec<String>,
        upstream_watermarks: Arc<Mutex<HashMap<String, u64>>>,
        current_watermark: Arc<AtomicU64>,
        _status: Arc<AtomicU8>,
        mut aligner: CheckpointAligner,
        labels: Option<MetricsLabels>,
        execution_attempt_id: u64,
    ) -> MessageStream {
        Box::pin(stream! {
            let mut input_stream = input_stream;
            let mut watermark_manager = WatermarkManager::new(
                watermark_assign,
                upstream_vertices.clone(),
                upstream_watermarks.clone(),
                current_watermark.clone(),
            );
            while let Some(message) = input_stream.next().await {
                Self::record_metrics(vertex_id.clone(), &message, true, labels.as_ref());
                Self::record_control_propagation(&vertex_id, &message, labels.as_ref());

                match &message {
                    Message::Watermark(watermark) => {
                        if watermark_manager.assigner_enabled() && watermark.watermark_value != MAX_WATERMARK_VALUE {
                            continue;
                        }
                        // Advance watermark and only forward to operator if advanced.
                        if let Some(new_watermark) = watermark_manager.merge_watermark(watermark.clone()).await {
                            Self::set_watermark_lag_gauge(&vertex_id, new_watermark, labels.as_ref());
                            let mut wm = watermark.clone();
                            wm.watermark_value = new_watermark;
                            wm.metadata.upstream_vertex_id = Some(vertex_id.as_ref().to_string());
                            // Keep create stamp for downstream propagation.
                            yield Message::Watermark(wm);
                        }
                    }
                    Message::CheckpointBarrier(barrier) => {
                        match aligner.on_barrier(barrier, execution_attempt_id) {
                            Ok(Some((checkpoint_id, inject_stamp, align_wait_ms))) => {
                                Self::record_histogram(
                                    METRIC_STREAM_TASK_CHECKPOINT_ALIGN_WAIT_MS,
                                    align_wait_ms,
                                    &vertex_id,
                                    labels.as_ref(),
                                );
                                // Once fully aligned, yield a single barrier; preserve inject stamp.
                                yield Message::CheckpointBarrier(
                                    crate::common::message::CheckpointBarrierMessage::new(
                                        vertex_id.as_ref().to_string(),
                                        checkpoint_id,
                                        execution_attempt_id,
                                        inject_stamp,
                                    ),
                                );
                            }
                            Ok(None) => {}
                            Err(error) => {
                                panic!(
                                    "[CHECKPOINT] {} alignment failed: {}",
                                    vertex_id.as_ref(),
                                    error
                                );
                            }
                        }
                    }
                    _ => {
                        // Generate per-upstream watermark from this data message (if enabled),
                        // then run it through the same min-upstream merge logic.
                        let upstream_for_msg = message
                            .upstream_vertex_id()
                            .unwrap_or_else(|| vertex_id.as_ref().to_string());
                        let injected = watermark_manager.on_data_message(&upstream_for_msg, &message);
                        yield message;
                        if let Some(wm) = injected {
                            // Treat injected watermark exactly like an upstream watermark: merge and
                            // emit a task-local watermark only if it advances.
                            if let Some(new_watermark) = watermark_manager.merge_watermark(wm.clone()).await {
                                Self::set_watermark_lag_gauge(
                                    &vertex_id,
                                    new_watermark,
                                    labels.as_ref(),
                                );
                                yield Message::Watermark(WatermarkMessage::new(
                                    vertex_id.as_ref().to_string(),
                                    new_watermark,
                                    Some(Self::now_ms()),
                                ));
                            }
                        }
                    }
                }
            }
        })
    }

    // Helper function to send message to collectors (similar to original stream_task.rs).
    async fn send_to_collectors_if_needed(
        collectors_per_target_operator: &mut HashMap<String, Collector>,
        message: Message,
    ) {
        if collectors_per_target_operator.is_empty() {
            // Edge operator - no downstream collectors
            return;
        }

        let mut channels_to_send_per_operator = HashMap::new();
        for (target_operator_id, collector) in collectors_per_target_operator.iter_mut() {
            let partitioned_channels = collector.gen_partitioned_channels(&message);

            channels_to_send_per_operator.insert(target_operator_id.clone(), partitioned_channels);
        }

        // Wait-until-queued: false means the edge already reported fatal. Do not resend.
        let _ = Collector::write_message_to_operators(
            collectors_per_target_operator,
            &message,
            channels_to_send_per_operator,
        )
        .await;
    }

    fn report_task_time_metrics(
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

    pub async fn start(&mut self) {
        let vertex_id = self.vertex_id.clone();
        let runtime_context = self.runtime_context.clone();
        let status = self.status.clone();
        let operator_config = self.operator_config.clone();
        let execution_graph = self.execution_graph.clone();
        let master_addr = self.master_addr.clone();
        let restore_data = self.restore_data.take();
        let execution_attempt_id = self.execution_attempt_id;
        let worker_health = self.worker_health.clone();
        let run_loop_health = worker_health.clone();
        
        let upstream_watermarks = self.upstream_watermarks.clone();
        let current_watermark = self.current_watermark.clone();
        
        let upstream_vertices: Vec<String> = execution_graph
            .get_edges_for_vertex(vertex_id.as_ref())
            .map(|(input_edges, _)| {
                input_edges
                    .iter()
                    .map(|e| e.source_vertex_id.as_ref().to_string())
                    .collect()
            })
            .unwrap_or_default();

        let transport_client_config = self.transport_client_config.take().unwrap();
        
        let (run_sender, run_receiver) = watch::channel(false);
        self.run_signal_sender = Some(run_sender);

        let (close_sender, close_receiver) = watch::channel(false);
        self.close_signal_sender = Some(close_sender);

        let (checkpoint_sender, mut checkpoint_receiver) = mpsc::unbounded_channel::<u64>();
        self.checkpoint_trigger_sender = Some(checkpoint_sender);
        
        // Main stream task lifecycle loop
        let metrics_labels = self.metrics_labels.clone();
        let task_vertex_id = vertex_id.clone();
        let run_loop = async move {
            let mut operator = create_operator(operator_config);
            
            let mut transport_client =
                TransportClient::new(vertex_id.clone(), transport_client_config, run_loop_health);

            // Install before collectors clone the writer so ratio + BP time share one tracker.
            let task_time_metrics = Arc::new(TaskTimeMetrics::default());
            if let Some(writer) = transport_client.writer.as_mut() {
                writer.set_backpressure_tracker(task_time_metrics.backpressure_tracker());
            }
            
            let mut collectors_per_target_operator: HashMap<String, Collector> = HashMap::new();

            let (_, output_edges) = execution_graph.get_edges_for_vertex(&vertex_id).unwrap();
            let num_output_edges = output_edges.len();

            for edge in output_edges {
                let channel = edge.get_channel();
                let partition_type = edge.partition_type.clone();
                let target_operator_id = edge.target_operator_id.clone();

                let writer = transport_client.writer.as_ref()
                    .unwrap_or_else(|| panic!("Writer not initialized"));
            
                let partition = partition_type.create();
                
                let collector = collectors_per_target_operator.entry(target_operator_id).or_insert_with(|| {
                    Collector::new(
                        writer.clone(),
                        partition,
                    )
                });
                
                collector.add_output_channel(channel);
            }

            // start collectors
            for (_, collector) in collectors_per_target_operator.iter_mut() {
                collector.start().await;
            }

            // Optional master client used for checkpoint reporting.
            let mut master_client: Option<MasterServiceClient<tonic::transport::Channel>> =
                if let Some(master_addr) = master_addr {
                    Some(
                        connect_master_client(&master_addr, &GrpcConfig::new())
                            .await
                            .map_err(|e| {
                                anyhow::anyhow!("Failed to connect to master service: {}", e)
                            })?,
                    )
                } else {
                    None
                };

            operator.open(&runtime_context).await?;
            println!("{:?} Operator {:?} opened with {} output edges", timestamp(), vertex_id, num_output_edges);

            // Optional restore hook (after open, before run).
            // Important: some operators (e.g. sources) initialize internal state in open();
            // restoring before open would be overwritten by that initialization.
            if let Some(restore) = restore_data {
                operator.restore(restore).await?;
                println!("[CHECKPOINT] {} restore applied", vertex_id);
            }

            // if task is not finished/closed early, mark as opened
            if status.load(Ordering::SeqCst) != StreamTaskStatus::Finished as u8 && status.load(Ordering::SeqCst) != StreamTaskStatus::Closed as u8 {
                status.store(StreamTaskStatus::Opened as u8, Ordering::SeqCst);
            }
            
            // Wait for signal to start processing
            println!("{:?} Task {:?} waiting for run signal", timestamp(), vertex_id);
            Self::wait_for_signal(run_receiver, status.clone(), true).await;
            println!("{:?} Task {:?} received run signal, starting processing", timestamp(), vertex_id);
            
            // if task is not finished/closed early, mark as runnning
            if status.load(Ordering::SeqCst) != StreamTaskStatus::Finished as u8 && status.load(Ordering::SeqCst) != StreamTaskStatus::Closed as u8 {
                status.store(StreamTaskStatus::Running as u8, Ordering::SeqCst);
            }

            let operator_type = operator.operator_type();
            let is_source = operator_type == OperatorType::Source || operator_type == OperatorType::ChainedSourceSink;
            let mut data_reader_control: Option<crate::transport::transport_client::DataReaderControl> = None;
            let vertex_meta = execution_graph
                .get_vertex(&vertex_id)
                .expect("vertex should exist in execution graph");
            let watermark_assign = vertex_meta.watermark_assign.clone();

            // Runtime invariant: watermark assigners must not run in Batch mode.
            // Batch pipelines rely on source completion to emit a terminal MAX_WATERMARK_VALUE.
            if runtime_context
                .job_config()
                .get("execution_mode")
                .and_then(|v| v.as_str())
                == Some("Batch")
            {
                assert!(
                    watermark_assign.is_none(),
                    "StreamTask {}: watermark_assign is not allowed in Batch execution mode",
                    vertex_id.as_ref()
                );
            }
            let mut source_watermark_manager = if is_source {
                Some(WatermarkManager::new(
                    watermark_assign.clone(),
                    upstream_vertices.clone(),
                    upstream_watermarks.clone(),
                    current_watermark.clone(),
                ))
            } else {
                None
            };
            
            let mut metrics_window_start = Instant::now();

            let mut _rx_queue_ticker = None;
            if !is_source {
                // Pre-process input stream for non-source operators
                let reader = transport_client.reader.take()
                    .expect("Reader should be initialized for non-SOURCE operator");
                _rx_queue_ticker = Some(reader.spawn_rx_queue_ticker(metrics_labels.clone()));
                let (input_stream, reader_control) = reader.message_stream_with_control();
                data_reader_control = Some(reader_control.clone());

                let checkpoint_aligner = CheckpointAligner::new(&upstream_vertices, reader_control);

                let preprocessed_stream = Self::create_preprocessed_input_stream(
                    input_stream,
                    vertex_id.clone(),
                    watermark_assign.clone(),
                    upstream_vertices.clone(),
                    upstream_watermarks.clone(),
                    current_watermark.clone(),
                    status.clone(),
                    checkpoint_aligner,
                    metrics_labels.clone(),
                    execution_attempt_id,
                );

                operator.set_input(Some(idle_timing_stream(
                    preprocessed_stream,
                    task_time_metrics.clone(),
                )))
            };

            while status.load(Ordering::SeqCst) == StreamTaskStatus::Running as u8 {
                // For sources: if checkpoint is triggered, synthesize a CheckpointBarrier message and treat it exactly
                // like a poll_next() output (same code path, no duplication).
                let produced = if is_source && crate::runtime::operators::operator::operator_config_requires_checkpoint(operator.operator_config()) {
                    match checkpoint_receiver.try_recv() {
                        Ok(checkpoint_id) => {
                            println!(
                                "[CHECKPOINT] {} received trigger for checkpoint_id={}",
                                vertex_id, checkpoint_id
                            );
                            Some(Message::CheckpointBarrier(
                                crate::common::message::CheckpointBarrierMessage::new(
                                    vertex_id.as_ref().to_string(),
                                    checkpoint_id,
                                    execution_attempt_id,
                                    Some(Self::now_ms()),
                                ),
                            ))
                        }
                        Err(mpsc::error::TryRecvError::Empty) => None,
                        Err(mpsc::error::TryRecvError::Disconnected) => None,
                    }
                } else {
                    None
                };

                let poll_res = if let Some(msg) = produced {
                    OperatorPollResult::Ready(msg)
                } else if is_source {
                    // Source has no input stream: idle = fetch Pending (Kafka wait,
                    // rate-limit sleep), not CPU in a Ready poll (datagen produce).
                    task_time_metrics
                        .while_pending(operator.poll_next())
                        .await
                } else {
                    operator.poll_next().await
                };

                match poll_res {
                    OperatorPollResult::Ready(mut message) => {
                        let injected_wm = if is_source && matches!(message, Message::Regular(_)) {
                            source_watermark_manager
                                .as_mut()
                                .and_then(|manager| manager.on_data_message(vertex_id.as_ref(), &message))
                        } else {
                            None
                        };
                        if let Message::CheckpointBarrier(ref barrier) = message {
                            let checkpoint_id = barrier.checkpoint_id;
                            let propagation_phase = if is_source {
                                crate::runtime::master::server::master_service::CheckpointPropagationPhase::BarrierInjected
                            } else {
                                crate::runtime::master::server::master_service::CheckpointPropagationPhase::Aligned
                            };
                            Self::report_checkpoint_propagation(
                                &mut master_client,
                                checkpoint_id,
                                vertex_id.as_ref(),
                                runtime_context.task_index(),
                                execution_attempt_id,
                                propagation_phase,
                            )
                            .await?;

                            if crate::runtime::operators::operator::operator_config_requires_checkpoint(operator.operator_config()) {
                                println!(
                                    "[CHECKPOINT] {} checkpointing checkpoint_id={}",
                                    vertex_id, checkpoint_id
                                );
                                let started = Instant::now();
                                let checkpoint_result = async {
                                    let checkpoint: SerializedCheckpoint =
                                        operator.checkpoint(checkpoint_id).await?;
                                    let checkpoint_data = checkpoint.into_bytes();
                                    let payload_len = checkpoint_data.len() as u64;

                                    let report_resp = master_client
                                        .as_mut()
                                        .expect("Master client not initialized")
                                        .report_checkpoint(tonic::Request::new(
                                            crate::runtime::master::server::master_service::ReportCheckpointRequest {
                                                checkpoint_id,
                                                vertex_id: vertex_id.as_ref().to_string(),
                                                task_index: runtime_context.task_index(),
                                                checkpoint_data,
                                                execution_attempt_id,
                                            },
                                        ))
                                        .await?
                                        .into_inner();
                                    if !report_resp.success {
                                        return Err(anyhow::anyhow!(
                                            "report_checkpoint rejected: {}",
                                            report_resp.error_message
                                        ));
                                    }
                                    Ok::<u64, anyhow::Error>(payload_len)
                                }
                                .await;

                                let duration_ms = started.elapsed().as_secs_f64() * 1000.0;
                                match checkpoint_result {
                                    Ok(payload_len) => {
                                        Self::record_histogram(
                                            METRIC_STREAM_TASK_CHECKPOINT_DURATION_MS,
                                            duration_ms,
                                            &vertex_id,
                                            metrics_labels.as_ref(),
                                        );
                                        Self::record_histogram(
                                            METRIC_STREAM_TASK_CHECKPOINT_PAYLOAD_BYTES,
                                            payload_len as f64,
                                            &vertex_id,
                                            metrics_labels.as_ref(),
                                        );
                                        increment_task_counter(
                                            METRIC_STREAM_TASK_CHECKPOINT_SUCCESS,
                                            1,
                                            vertex_id.as_ref(),
                                            metrics_labels.as_ref(),
                                        );
                                        println!(
                                            "[CHECKPOINT] {} reported checkpoint_id={}",
                                            vertex_id, checkpoint_id
                                        );
                                    }
                                    Err(error) => {
                                        increment_task_counter(
                                            METRIC_STREAM_TASK_CHECKPOINT_FAILED,
                                            1,
                                            vertex_id.as_ref(),
                                            metrics_labels.as_ref(),
                                        );
                                        return Err(error);
                                    }
                                }
                            }

                            // Resume input after checkpointing
                            if let Some(ctrl) = &data_reader_control {
                                ctrl.unblock_all();
                            }
                        }

                        // if vertex_id == "Window_1_2" {
                        //     // todo remove after debugging
                        //     println!("StreamTask {:?} produced message {:?}", vertex_id, message);
                        // }
                        if is_source {
                            // Stamp data (path latency) and control (propagation) at source egress
                            // unless already stamped (e.g. barrier inject time).
                            if message.ingest_timestamp().is_none() {
                                message.set_ingest_timestamp(Self::now_ms());
                            }
                        }
                        if let Message::Watermark(ref mut watermark) = message {
                            if is_source {
                                if let Some(manager) = source_watermark_manager.as_mut() {
                                    if let Some(new_watermark) = manager.merge_watermark(watermark.clone()).await {
                                        watermark.watermark_value = new_watermark;
                                    }
                                }
                                Self::set_watermark_lag_gauge(
                                    &vertex_id,
                                    watermark.watermark_value,
                                    metrics_labels.as_ref(),
                                );
                            }
                            // If operator outputs max watermark, finish task
                            if watermark.watermark_value == MAX_WATERMARK_VALUE {
                                status.store(StreamTaskStatus::Finished as u8, Ordering::SeqCst);
                            }
                            println!("StreamTask {:?} produced watermark {:?}", vertex_id, watermark.watermark_value);
                        }
                        
                        if MESSAGE_TRACE_ENABLED.load(Ordering::Relaxed) {
                            message.append_trace(&vertex_id);
                        }

                        // Set upstream vertex id for all messages before sending downstream
                        message.set_upstream_vertex_id(vertex_id.as_ref().to_string());
                        Self::record_metrics(vertex_id.clone(), &message, false, metrics_labels.as_ref());

                        // Send to collectors (queue waits record on BackpressureTracker)
                        Self::send_to_collectors_if_needed(
                            &mut collectors_per_target_operator,
                            message,
                        ).await;

                        if let Some(wm) = injected_wm {
                            let mut injected = Message::Watermark(wm);
                            injected.set_upstream_vertex_id(vertex_id.as_ref().to_string());
                            injected.set_ingest_timestamp(Self::now_ms());
                            if let Message::Watermark(ref w) = injected {
                                Self::set_watermark_lag_gauge(
                                    &vertex_id,
                                    w.watermark_value,
                                    metrics_labels.as_ref(),
                                );
                            }
                            Self::record_metrics(
                                vertex_id.clone(),
                                &injected,
                                false,
                                metrics_labels.as_ref(),
                            );
                            Self::send_to_collectors_if_needed(
                                &mut collectors_per_target_operator,
                                injected,
                            )
                            .await;
                        }
                    }
                    OperatorPollResult::None => {
                        if is_source {
                            let wm = Message::Watermark(WatermarkMessage::new(
                                vertex_id.as_ref().to_string(),
                                MAX_WATERMARK_VALUE,
                                Some(Self::now_ms()),
                            ));
                            Self::record_metrics(vertex_id.clone(), &wm, false, metrics_labels.as_ref());
                            Self::send_to_collectors_if_needed(
                                &mut collectors_per_target_operator,
                                wm,
                            )
                            .await;
                        } else {
                            // Peer/transport teardown can drop the input without a max watermark.
                            println!(
                                "{:?} StreamTask {}: upstream ended without terminal watermark; finishing",
                                timestamp(),
                                vertex_id.as_ref()
                            );
                        }
                        status.store(StreamTaskStatus::Finished as u8, Ordering::SeqCst);
                        break;
                    }
                    OperatorPollResult::Continue => { /* no output; busy = residual on report */ }
                }
                Self::report_task_time_metrics(
                    &task_time_metrics,
                    &mut metrics_window_start,
                    &vertex_id,
                    metrics_labels.as_ref(),
                    &current_watermark,
                );
            }
            
            // Flush and close collectors
            for (_, collector) in collectors_per_target_operator.iter_mut() {
                collector.flush_and_close().await.unwrap();
            }
            
            println!("{:?} Task {:?} waiting for close signal", timestamp(), vertex_id);
            Self::wait_for_signal(close_receiver, status.clone(), false).await;
            println!("{:?} Task {:?} received close signal, performing close", timestamp(), vertex_id);

            operator.close().await?;

            status.store(StreamTaskStatus::Closed as u8, Ordering::SeqCst);
            println!("{:?} StreamTask {:?} closed", timestamp(), vertex_id);
            
            Ok(())
        };
        let run_loop_handle = tokio::spawn(run_loop.map(move |result| {
            if let Err(error) = &result {
                worker_health.report_fatal(
                    WorkerFatalReason::TaskFailure,
                    format!("StreamTask {} failed: {}", task_vertex_id, error),
                );
            }
            result
        }));

        self.run_loop_handle = Some(run_loop_handle);
    }

    pub async fn get_state(&self) -> TaskSnapshot {
        // Detach from the live bag so later operator writes do not mutate the snapshot.
        // Throughput/latency for WorkerSnapshot are collected once by the worker poll.
        let metadata = TaskMetadata::default();
        metadata.extend(&self.runtime_context.task_metadata());
        TaskSnapshot {
            vertex_id: self.vertex_id.clone(),
            status: StreamTaskStatus::from(self.status.load(Ordering::SeqCst)),
            metadata,
        }
    }

    pub fn signal_to_run(&mut self) {
        let run_signal_sender = self.run_signal_sender.as_ref().unwrap();
        let _ = run_signal_sender.send(true);
    }

    /// Set the close watch. If still mid-run, abort the run loop (watch is only
    /// observed after Finished). After Finished, signal alone runs operator.close.
    pub fn close(&mut self) {
        if let Some(close_signal_sender) = self.close_signal_sender.as_ref() {
            let _ = close_signal_sender.send(true);
        }
        let status = self.status.load(Ordering::SeqCst);
        if status == StreamTaskStatus::Created as u8
            || status == StreamTaskStatus::Opened as u8
            || status == StreamTaskStatus::Running as u8
        {
            if let Some(handle) = self.run_loop_handle.take() {
                handle.abort();
            }
        }
    }

    pub fn signal_trigger_checkpoint(&mut self, checkpoint_id: u64) {
        let sender = self.checkpoint_trigger_sender.as_ref().expect("checkpoint trigger sender not set");
        let _ = sender.send(checkpoint_id);
    }

    async fn wait_for_signal(mut receiver: watch::Receiver<bool>, status: Arc<AtomicU8>, skip_on_finished: bool) {
        let timeout = Duration::from_millis(50000);
        let start_time = SystemTime::now();

        loop {
            if skip_on_finished {
                let current_status = status.load(Ordering::SeqCst);
                if current_status == StreamTaskStatus::Finished as u8 || current_status == StreamTaskStatus::Closed as u8 {
                    println!("{:?} Task status is {:?}, stopping wait for signal", timestamp(), StreamTaskStatus::from(current_status));
                    return;
                }
            }

            if start_time.elapsed().unwrap_or_default() > timeout {
                panic!("Timeout waiting for signal after {:?}, current status is {:?}", timeout, StreamTaskStatus::from(status.load(Ordering::SeqCst)));
            }

            match tokio::time::timeout(Duration::from_millis(50), receiver.changed()).await {
                Ok(_) => return,
                Err(_) => continue,
            }
        }
    }
}

#[cfg(test)]
mod task_time_metrics_tests {
    use super::*;
    use async_stream::stream;
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::time::sleep;

    fn wm(i: u64) -> Message {
        Message::Watermark(WatermarkMessage::new("t".to_string(), i, Some(0)))
    }

    fn assert_ns_near(actual: u64, expected: Duration, lo_frac: f64, hi_frac: f64) {
        let expected_ns = expected.as_nanos() as f64;
        let lo = (expected_ns * lo_frac) as u64;
        let hi = (expected_ns * hi_frac) as u64;
        assert!(
            actual >= lo && actual <= hi,
            "idle_ns={actual} not in [{lo}, {hi}] for expected {expected:?}"
        );
    }

    fn delayed_input(hold: Duration, msg: Message) -> MessageStream {
        Box::pin(stream! {
            sleep(hold).await;
            yield msg;
        })
    }

    #[tokio::test]
    async fn ready_input_records_no_idle() {
        let metrics = Arc::new(TaskTimeMetrics::default());
        let mut input = idle_timing_stream(
            Box::pin(futures::stream::iter(vec![wm(1)])),
            metrics.clone(),
        );
        assert!(input.next().await.is_some());
        assert_eq!(metrics.take_idle_ns(), 0);
    }

    #[tokio::test]
    async fn input_wait_records_idle_near_hold_time() {
        let metrics = Arc::new(TaskTimeMetrics::default());
        let hold = Duration::from_millis(80);
        let mut input = idle_timing_stream(delayed_input(hold, wm(1)), metrics.clone());
        assert!(input.next().await.is_some());
        assert_ns_near(metrics.take_idle_ns(), hold, 0.5, 2.5);
    }

    #[tokio::test]
    async fn operator_compute_after_input_is_not_idle() {
        // Reproduces #262: insert_batch / process_due sit after next_input.
        let metrics = Arc::new(TaskTimeMetrics::default());
        let wait = Duration::from_millis(80);
        let compute = Duration::from_millis(50);
        let mut input = idle_timing_stream(delayed_input(wait, wm(1)), metrics.clone());
        assert!(input.next().await.is_some());
        sleep(compute).await;
        let idle = metrics.take_idle_ns();
        assert_ns_near(idle, wait, 0.5, 2.5);
        assert!(
            idle < (wait + compute).as_nanos() as u64,
            "compute after input was counted as idle: idle_ns={idle}"
        );
    }

    #[tokio::test]
    async fn source_pending_wait_is_idle() {
        let metrics = Arc::new(TaskTimeMetrics::default());
        let hold = Duration::from_millis(80);
        metrics.while_pending(sleep(hold)).await;
        assert_ns_near(metrics.take_idle_ns(), hold, 0.5, 2.5);
    }

    #[tokio::test]
    async fn source_ready_poll_cpu_is_not_idle() {
        let metrics = Arc::new(TaskTimeMetrics::default());
        let hold = Duration::from_millis(50);
        metrics
            .while_pending(async {
                let start = Instant::now();
                while start.elapsed() < hold {}
            })
            .await;
        assert!(
            metrics.take_idle_ns() < Duration::from_millis(10).as_nanos() as u64,
            "CPU work in a Ready poll should not count as idle"
        );
    }

    #[tokio::test]
    async fn busy_is_residual_of_idle_in_window() {
        let metrics = Arc::new(TaskTimeMetrics::default());
        let hold = Duration::from_millis(80);
        let window = Duration::from_millis(200);
        metrics.while_pending(sleep(hold)).await;
        let (busy, idle, bp) = metrics.take_ms_per_second(window);
        assert_eq!(bp, 0.0);
        let expected_idle = hold.as_secs_f64() / window.as_secs_f64() * 1000.0;
        assert!(
            (idle - expected_idle).abs() < 150.0,
            "idle={idle} expected≈{expected_idle}"
        );
        assert!(
            (busy + idle + bp - 1000.0).abs() < 1e-6,
            "busy={busy} idle={idle} bp={bp}"
        );
    }
}
