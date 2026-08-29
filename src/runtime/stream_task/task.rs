use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use futures::FutureExt;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;

use crate::runtime::checkpoint::SerializedRestore;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::health::{WorkerFatalReason, WorkerHealth};
use crate::runtime::metrics::{init_metrics, MetricsLabels};
use crate::runtime::observability::{StreamTaskStatus, TaskMetadata, TaskSnapshot};
use crate::runtime::operators::operator::OperatorConfig;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::VertexId;
use crate::transport::transport_client::TransportClientConfig;

use super::run::{run, RunParams};

pub static MESSAGE_TRACE_ENABLED: AtomicBool = AtomicBool::new(false);

pub(super) fn timestamp() -> String {
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
    run_signal_sender: Option<oneshot::Sender<()>>,
    close_signal_sender: Option<oneshot::Sender<()>>,
    checkpoint_trigger_sender: Option<mpsc::UnboundedSender<u64>>,
    upstream_watermarks: Arc<Mutex<HashMap<String, u64>>>,
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

    pub(super) fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
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

        let (run_sender, run_receiver) = oneshot::channel();
        self.run_signal_sender = Some(run_sender);

        let (close_sender, close_receiver) = oneshot::channel();
        self.close_signal_sender = Some(close_sender);

        let (checkpoint_sender, checkpoint_receiver) = mpsc::unbounded_channel::<u64>();
        self.checkpoint_trigger_sender = Some(checkpoint_sender);

        let metrics_labels = self.metrics_labels.clone();
        let task_vertex_id = vertex_id.clone();
        let run_loop = run(RunParams {
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
        });
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
        let metadata = TaskMetadata::default();
        metadata.extend(&self.runtime_context.task_metadata());
        TaskSnapshot {
            vertex_id: self.vertex_id.clone(),
            status: StreamTaskStatus::from(self.status.load(Ordering::SeqCst)),
            metadata,
        }
    }

    pub fn signal_to_run(&mut self) {
        let run_signal_sender = self
            .run_signal_sender
            .take()
            .expect("run signal sender not set");
        let _ = run_signal_sender.send(());
    }

    /// Signal close. If still mid-run, abort the run loop (the close receiver
    /// is only observed after Finished). After Finished, signal alone runs operator.close.
    pub fn close(&mut self) {
        if let Some(close_signal_sender) = self.close_signal_sender.take() {
            let _ = close_signal_sender.send(());
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
        let sender = self
            .checkpoint_trigger_sender
            .as_ref()
            .expect("checkpoint trigger sender not set");
        let _ = sender.send(checkpoint_id);
    }

    pub(super) async fn wait_for_run(receiver: oneshot::Receiver<()>, status: Arc<AtomicU8>) {
        let current_status = status.load(Ordering::SeqCst);
        if current_status == StreamTaskStatus::Finished as u8
            || current_status == StreamTaskStatus::Closed as u8
        {
            println!(
                "{:?} Task status is {:?}, stopping wait for run signal",
                timestamp(),
                StreamTaskStatus::from(current_status)
            );
            return;
        }

        let timeout = Duration::from_millis(50000);
        match tokio::time::timeout(timeout, receiver).await {
            Ok(_) => {}
            Err(_) => panic!(
                "Timeout waiting for run signal after {:?}, current status is {:?}",
                timeout,
                StreamTaskStatus::from(status.load(Ordering::SeqCst))
            ),
        }
    }

    pub(super) async fn wait_for_close(receiver: oneshot::Receiver<()>, status: Arc<AtomicU8>) {
        let timeout = Duration::from_millis(50000);
        match tokio::time::timeout(timeout, receiver).await {
            Ok(_) => {}
            Err(_) => panic!(
                "Timeout waiting for close signal after {:?}, current status is {:?}",
                timeout,
                StreamTaskStatus::from(status.load(Ordering::SeqCst))
            ),
        }
    }
}
