use crate::{
    common::MAX_WATERMARK_VALUE,
    runtime::{
        checkpoint::SerializedRestore,
        collector::Collector,
        execution_graph::ExecutionGraph,
        health::{WorkerFatalReason, WorkerHealth},
        metrics::{
            init_metrics, record_task_histogram, set_task_gauge,
            MetricsLabels, LABEL_PIPELINE_ID, LABEL_TASK_ID, LABEL_WORKER_ID,
            METRIC_STREAM_TASK_BACKPRESSURED_TIME_MS_PER_SECOND,
            METRIC_STREAM_TASK_BUSY_TIME_MS_PER_SECOND, METRIC_STREAM_TASK_BYTES_RECV,
            METRIC_STREAM_TASK_BYTES_SENT,
            METRIC_STREAM_TASK_CHECKPOINT_BARRIER_PROPAGATION_MS,
            METRIC_STREAM_TASK_IDLE_TIME_MS_PER_SECOND,
            METRIC_STREAM_TASK_MESSAGES_RECV, METRIC_STREAM_TASK_MESSAGES_SENT,
            METRIC_STREAM_TASK_PATH_LATENCY, METRIC_STREAM_TASK_RECORDS_RECV,
            METRIC_STREAM_TASK_RECORDS_SENT, METRIC_STREAM_TASK_WATERMARK_LAG_MS,
            METRIC_STREAM_TASK_WM_PROPAGATION_MS,
        },
        observability::{TaskMetadata, TaskSnapshot, StreamTaskStatus},
        operators::operator::{MessageStream, OperatorConfig},
        runtime_context::RuntimeContext,
    },
    transport::transport_client::TransportClientConfig,
};
use anyhow::Result;
use async_stream::stream;
use futures::{FutureExt, StreamExt};
use metrics::counter;
use tokio::{task::JoinHandle, sync::Mutex, sync::watch, sync::mpsc};
use crate::transport::transport_client::DataReaderControl;
use crate::common::message::{Message, WatermarkMessage};
use std::{collections::{HashMap, HashSet}, sync::{atomic::{AtomicU8, AtomicU64, Ordering}, Arc}, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};

/// Accumulates Flink-style task time metrics within a report window.
///
/// Idle = waiting on the input stream (`next()`), not operator compute such as
/// Window `insert_batch` / `process_due`. Backpressured = exclusive tx-queue
/// wait via [`BackpressureTracker`]; busy = residual wall time so the three
/// ms/s gauges sum to ~1000.
#[derive(Debug, Default)]
pub(crate) struct TaskTimeMetrics {
    idle_ns: AtomicU64,
    backpressure_tracker: Arc<crate::transport::batch_channel::BackpressureTracker>,
}

impl TaskTimeMetrics {
    pub(crate) fn add_idle_ns(&self, ns: u64) {
        self.idle_ns.fetch_add(ns, Ordering::Relaxed);
    }

    pub(crate) fn backpressure_tracker(&self) -> Arc<crate::transport::batch_channel::BackpressureTracker> {
        self.backpressure_tracker.clone()
    }

    /// Scale to ms-per-second and reset. Returns `(busy, idle, backpressured)`.
    pub(crate) fn take_ms_per_second(&self, window: Duration) -> (f64, f64, f64) {
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
pub(crate) fn input_with_idle_tracking(
    mut input: MessageStream,
    metrics: Arc<TaskTimeMetrics>,
) -> MessageStream {
    Box::pin(stream! {
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

use crate::runtime::master::server::master_service::master_service_client::MasterServiceClient;
use std::sync::atomic::AtomicBool;
use crate::runtime::VertexId;

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
    pub(crate) fn on_barrier(
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
}

pub(crate) fn timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .to_string()
}

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

    pub(crate) async fn report_checkpoint_propagation(
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

    pub(crate) fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    pub(crate) fn record_histogram(
        name: &'static str,
        value_ms: f64,
        vertex_id: &VertexId,
        labels: Option<&MetricsLabels>,
    ) {
        record_task_histogram(name, value_ms, vertex_id.as_ref(), labels);
    }

    pub(crate) fn record_control_propagation(
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

    pub(crate) fn set_watermark_lag_gauge(
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

    pub(crate) fn record_metrics(
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

    pub(crate) async fn send_source_assigned_watermark(
        collectors_per_target_operator: &mut HashMap<String, Collector>,
        vertex_id: &VertexId,
        labels: Option<&MetricsLabels>,
        wm: WatermarkMessage,
    ) {
        let mut injected = Message::Watermark(wm);
        injected.set_upstream_vertex_id(vertex_id.as_ref().to_string());
        injected.set_ingest_timestamp(Self::now_ms());
        if let Message::Watermark(ref w) = injected {
            Self::set_watermark_lag_gauge(vertex_id, w.watermark_value, labels);
        }
        Self::record_metrics(vertex_id.clone(), &injected, false, labels);
        Self::send_to_collectors_if_needed(collectors_per_target_operator, injected).await;
    }

    pub(crate) async fn send_source_assigned_watermarks(
        collectors_per_target_operator: &mut HashMap<String, Collector>,
        vertex_id: &VertexId,
        labels: Option<&MetricsLabels>,
        wms: Vec<WatermarkMessage>,
    ) {
        for wm in wms {
            Self::send_source_assigned_watermark(
                collectors_per_target_operator,
                vertex_id,
                labels,
                wm,
            )
            .await;
        }
    }

    pub(crate) async fn send_to_collectors_if_needed(
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

    pub(crate) fn report_task_time_metrics(
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

        let (checkpoint_sender, checkpoint_receiver) = mpsc::unbounded_channel::<u64>();
        self.checkpoint_trigger_sender = Some(checkpoint_sender);

        let metrics_labels = self.metrics_labels.clone();
        let task_vertex_id = vertex_id.clone();
        let run_loop = crate::runtime::stream_task_run::run(
            crate::runtime::stream_task_run::RunParams {
                vertex_id,
                runtime_context,
                status,
                operator_config,
                execution_graph,
                master_addr,
                restore_data,
                execution_attempt_id,
                worker_health: run_loop_health,
                upstream_watermarks,
                current_watermark,
                upstream_vertices,
                transport_client_config,
                metrics_labels,
                run_receiver,
                close_receiver,
                checkpoint_receiver,
            },
        );
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

    pub(crate) async fn wait_for_signal(mut receiver: watch::Receiver<bool>, status: Arc<AtomicU8>, skip_on_finished: bool) {
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

