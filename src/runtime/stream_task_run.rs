//! Dispatcher loop for [`StreamTask`]: mailbox wait, checkpoint RPC, emit via [`Output`].

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;
use tokio::sync::{mpsc, watch, Mutex};

use crate::common::grpc::master::master_client as connect_master_client;
use crate::common::grpc::GrpcConfig;
use crate::common::message::{CheckpointBarrierMessage, Message, WatermarkMessage};
use crate::common::MAX_WATERMARK_VALUE;
use crate::runtime::checkpoint::SerializedRestore;
use crate::runtime::collector::Collector;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::functions::source::FetchResult;
use crate::runtime::health::WorkerHealth;
use crate::runtime::master::server::master_service::master_service_client::MasterServiceClient;
use crate::runtime::metrics::{
    increment_task_counter, MetricsLabels, METRIC_STREAM_TASK_CHECKPOINT_DURATION_MS,
    METRIC_STREAM_TASK_CHECKPOINT_FAILED, METRIC_STREAM_TASK_CHECKPOINT_PAYLOAD_BYTES,
    METRIC_STREAM_TASK_CHECKPOINT_SUCCESS,
};
use crate::runtime::observability::StreamTaskStatus;
use crate::runtime::operators::operator::{
    create_operator, drain_ready_inputs, operator_config_requires_checkpoint, NextInputs, Operator,
    OperatorConfig, OperatorTrait, OperatorType, Output, SourceOperator as SourceFetch,
    StreamOperator, INGEST_MAX_RECORDS,
};
use crate::runtime::operators::source::source_operator::SourceOperator;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::stream_task::{
    input_with_idle_tracking, timestamp, CheckpointAligner, StreamTask, TaskTimeMetrics,
    MESSAGE_TRACE_ENABLED,
};
use crate::runtime::stream_task_preprocess::{
    create_preprocessed_input_stream, sleep_until_deadline,
};
use crate::runtime::watermark::WatermarkManager;
use crate::runtime::VertexId;
use crate::transport::transport_client::{DataReaderControl, TransportClient};
use crate::transport::transport_client::TransportClientConfig;

pub(crate) struct RunParams {
    pub vertex_id: VertexId,
    pub runtime_context: RuntimeContext,
    pub status: Arc<AtomicU8>,
    pub operator_config: OperatorConfig,
    pub execution_graph: ExecutionGraph,
    pub master_addr: Option<String>,
    pub restore_data: Option<SerializedRestore>,
    pub execution_attempt_id: u64,
    pub worker_health: WorkerHealth,
    pub upstream_watermarks: Arc<Mutex<HashMap<String, u64>>>,
    pub current_watermark: Arc<AtomicU64>,
    pub upstream_vertices: Vec<String>,
    pub transport_client_config: TransportClientConfig,
    pub metrics_labels: Option<MetricsLabels>,
    pub run_receiver: watch::Receiver<bool>,
    pub close_receiver: watch::Receiver<bool>,
    pub checkpoint_receiver: mpsc::UnboundedReceiver<u64>,
}

struct TransportOutput<'a> {
    collectors: &'a mut HashMap<String, Collector>,
    vertex_id: VertexId,
    labels: Option<&'a MetricsLabels>,
    stamp_ingest_if_missing: bool,
    status: Arc<AtomicU8>,
}

#[async_trait]
impl Output for TransportOutput<'_> {
    async fn emit(&mut self, mut message: Message) -> Result<()> {
        if self.stamp_ingest_if_missing && message.ingest_timestamp().is_none() {
            message.set_ingest_timestamp(StreamTask::now_ms());
        }
        if let Message::Watermark(ref watermark) = message {
            if watermark.watermark_value == MAX_WATERMARK_VALUE {
                self.status
                    .store(StreamTaskStatus::Finished as u8, Ordering::SeqCst);
            }
            println!(
                "StreamTask {:?} produced watermark {:?}",
                self.vertex_id, watermark.watermark_value
            );
        }
        if MESSAGE_TRACE_ENABLED.load(Ordering::Relaxed) {
            message.append_trace(&self.vertex_id);
        }
        message.set_upstream_vertex_id(self.vertex_id.as_ref().to_string());
        StreamTask::record_metrics(
            self.vertex_id.clone(),
            &message,
            false,
            self.labels,
        );
        StreamTask::send_to_collectors_if_needed(self.collectors, message).await;
        Ok(())
    }
}

pub(crate) async fn run(params: RunParams) -> Result<()> {
    let RunParams {
        vertex_id,
        runtime_context,
        status,
        operator_config,
        execution_graph,
        master_addr,
        restore_data,
        execution_attempt_id,
        worker_health,
        upstream_watermarks,
        current_watermark,
        upstream_vertices,
        transport_client_config,
        metrics_labels,
        run_receiver,
        close_receiver,
        mut checkpoint_receiver,
    } = params;

    let mut operator = create_operator(operator_config);

    let mut transport_client =
        TransportClient::new(vertex_id.clone(), transport_client_config, worker_health);

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

        let writer = transport_client
            .writer
            .as_ref()
            .unwrap_or_else(|| panic!("Writer not initialized"));

        let partition = partition_type.create();

        let collector = collectors_per_target_operator
            .entry(target_operator_id)
            .or_insert_with(|| Collector::new(writer.clone(), partition));

        collector.add_output_channel(channel);
    }

    for (_, collector) in collectors_per_target_operator.iter_mut() {
        collector.start().await;
    }

    let mut master_client: Option<MasterServiceClient<tonic::transport::Channel>> =
        if let Some(master_addr) = master_addr {
            Some(
                connect_master_client(&master_addr, &GrpcConfig::new())
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to connect to master service: {}", e))?,
            )
        } else {
            None
        };

    operator.open(&runtime_context).await?;
    println!(
        "{:?} Operator {:?} opened with {} output edges",
        timestamp(),
        vertex_id,
        num_output_edges
    );

    if let Some(restore) = restore_data {
        operator.restore(restore).await?;
        println!("[CHECKPOINT] {} restore applied", vertex_id);
    }

    if status.load(Ordering::SeqCst) != StreamTaskStatus::Finished as u8
        && status.load(Ordering::SeqCst) != StreamTaskStatus::Closed as u8
    {
        status.store(StreamTaskStatus::Opened as u8, Ordering::SeqCst);
    }

    println!(
        "{:?} Task {:?} waiting for run signal",
        timestamp(),
        vertex_id
    );
    StreamTask::wait_for_signal(run_receiver, status.clone(), true).await;
    println!(
        "{:?} Task {:?} received run signal, starting processing",
        timestamp(),
        vertex_id
    );

    if status.load(Ordering::SeqCst) != StreamTaskStatus::Finished as u8
        && status.load(Ordering::SeqCst) != StreamTaskStatus::Closed as u8
    {
        status.store(StreamTaskStatus::Running as u8, Ordering::SeqCst);
    }

    let is_source = operator.operator_type() == OperatorType::Source;
    let vertex_meta = execution_graph
        .get_vertex(&vertex_id)
        .expect("vertex should exist in execution graph");
    let watermark_assign = vertex_meta.watermark_assign.clone();

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

    if is_source {
        let Operator::Source(ref mut source) = operator else {
            unreachable!("source operator_type with non-Source variant");
        };
        let mut source_watermark_manager = WatermarkManager::new(
            watermark_assign,
            upstream_vertices.clone(),
            upstream_watermarks.clone(),
            current_watermark.clone(),
        );
        source_loop(
            source,
            SourceLoopParams {
                vertex_id: vertex_id.clone(),
                runtime_context: &runtime_context,
                status: status.clone(),
                execution_attempt_id,
                collectors: &mut collectors_per_target_operator,
                master_client: &mut master_client,
                metrics_labels: metrics_labels.as_ref(),
                task_time_metrics: task_time_metrics.clone(),
                current_watermark: current_watermark.clone(),
                checkpoint_receiver: &mut checkpoint_receiver,
                source_watermark_manager: &mut source_watermark_manager,
            },
        )
        .await?;
    } else {
        let reader = transport_client
            .reader
            .take()
            .expect("Reader should be initialized for non-SOURCE operator");
        let _rx_queue_ticker = reader.spawn_rx_queue_ticker(metrics_labels.clone());
        let (input_stream, reader_control) = reader.message_stream_with_control();
        let checkpoint_aligner = CheckpointAligner::new(&upstream_vertices, reader_control.clone());

        let preprocessed_stream = create_preprocessed_input_stream(
            input_stream,
            vertex_id.clone(),
            watermark_assign,
            upstream_vertices.clone(),
            upstream_watermarks.clone(),
            current_watermark.clone(),
            status.clone(),
            checkpoint_aligner,
            metrics_labels.clone(),
            execution_attempt_id,
        );

        processor_loop(
            &mut operator,
            ProcessorLoopParams {
                vertex_id: vertex_id.clone(),
                runtime_context: &runtime_context,
                status: status.clone(),
                execution_attempt_id,
                collectors: &mut collectors_per_target_operator,
                master_client: &mut master_client,
                metrics_labels: metrics_labels.as_ref(),
                task_time_metrics: task_time_metrics.clone(),
                current_watermark: current_watermark.clone(),
                input: input_with_idle_tracking(preprocessed_stream, task_time_metrics.clone()),
                reader_control,
            },
        )
        .await?;
    }

    for (_, collector) in collectors_per_target_operator.iter_mut() {
        collector.flush_and_close().await.unwrap();
    }

    println!(
        "{:?} Task {:?} waiting for close signal",
        timestamp(),
        vertex_id
    );
    StreamTask::wait_for_signal(close_receiver, status.clone(), false).await;
    println!(
        "{:?} Task {:?} received close signal, performing close",
        timestamp(),
        vertex_id
    );

    operator.close().await?;

    status.store(StreamTaskStatus::Closed as u8, Ordering::SeqCst);
    println!("{:?} StreamTask {:?} closed", timestamp(), vertex_id);

    Ok(())
}

struct ProcessorLoopParams<'a> {
    vertex_id: VertexId,
    runtime_context: &'a RuntimeContext,
    status: Arc<AtomicU8>,
    execution_attempt_id: u64,
    collectors: &'a mut HashMap<String, Collector>,
    master_client: &'a mut Option<MasterServiceClient<tonic::transport::Channel>>,
    metrics_labels: Option<&'a MetricsLabels>,
    task_time_metrics: Arc<TaskTimeMetrics>,
    current_watermark: Arc<AtomicU64>,
    input: crate::runtime::operators::operator::MessageStream,
    reader_control: DataReaderControl,
}

async fn processor_loop(operator: &mut Operator, params: ProcessorLoopParams<'_>) -> Result<()> {
    let ProcessorLoopParams {
        vertex_id,
        runtime_context,
        status,
        execution_attempt_id,
        collectors,
        master_client,
        metrics_labels,
        task_time_metrics,
        current_watermark,
        input,
        reader_control,
    } = params;

    let mut input = input.peekable();
    let mut metrics_window_start = Instant::now();

    while status.load(Ordering::SeqCst) == StreamTaskStatus::Running as u8 {
        match drain_ready_inputs(&mut input, INGEST_MAX_RECORDS).await {
            NextInputs::Exhausted => {
                println!(
                    "{:?} StreamTask {}: upstream ended without terminal watermark; finishing",
                    timestamp(),
                    vertex_id.as_ref()
                );
                status.store(StreamTaskStatus::Finished as u8, Ordering::SeqCst);
                break;
            }
            NextInputs::Data(data) => {
                let mut out = TransportOutput {
                    collectors,
                    vertex_id: vertex_id.clone(),
                    labels: metrics_labels,
                    stamp_ingest_if_missing: false,
                    status: status.clone(),
                };
                operator.process_data(data, &mut out).await?;
            }
            NextInputs::Control(Message::Watermark(wm)) => {
                let mut out = TransportOutput {
                    collectors,
                    vertex_id: vertex_id.clone(),
                    labels: metrics_labels,
                    stamp_ingest_if_missing: false,
                    status: status.clone(),
                };
                operator.handle_watermark(wm, &mut out).await?;
            }
            NextInputs::Control(Message::CheckpointBarrier(barrier)) => {
                on_checkpoint_barrier(
                    operator,
                    master_client,
                    &barrier,
                    false,
                    &vertex_id,
                    runtime_context.task_index(),
                    execution_attempt_id,
                    metrics_labels,
                    Some(&reader_control),
                )
                .await?;
                let mut out = TransportOutput {
                    collectors,
                    vertex_id: vertex_id.clone(),
                    labels: metrics_labels,
                    stamp_ingest_if_missing: false,
                    status: status.clone(),
                };
                operator.handle_barrier(barrier, &mut out).await?;
            }
            NextInputs::Control(other) => {
                panic!("unexpected control message in processor loop: {other:?}");
            }
        }
        StreamTask::report_task_time_metrics(
            &task_time_metrics,
            &mut metrics_window_start,
            &vertex_id,
            metrics_labels,
            &current_watermark,
        );
    }
    Ok(())
}

struct SourceLoopParams<'a> {
    vertex_id: VertexId,
    runtime_context: &'a RuntimeContext,
    status: Arc<AtomicU8>,
    execution_attempt_id: u64,
    collectors: &'a mut HashMap<String, Collector>,
    master_client: &'a mut Option<MasterServiceClient<tonic::transport::Channel>>,
    metrics_labels: Option<&'a MetricsLabels>,
    task_time_metrics: Arc<TaskTimeMetrics>,
    current_watermark: Arc<AtomicU64>,
    checkpoint_receiver: &'a mut mpsc::UnboundedReceiver<u64>,
    source_watermark_manager: &'a mut WatermarkManager,
}

async fn source_loop(source: &mut SourceOperator, params: SourceLoopParams<'_>) -> Result<()> {
    let SourceLoopParams {
        vertex_id,
        runtime_context,
        status,
        execution_attempt_id,
        collectors,
        master_client,
        metrics_labels,
        task_time_metrics,
        current_watermark,
        checkpoint_receiver,
        source_watermark_manager,
    } = params;

    let mut metrics_window_start = Instant::now();
    let checkpointable = operator_config_requires_checkpoint(source.operator_config());

    while status.load(Ordering::SeqCst) == StreamTaskStatus::Running as u8 {
        let injected_barrier = if checkpointable {
            match checkpoint_receiver.try_recv() {
                Ok(checkpoint_id) => {
                    println!(
                        "[CHECKPOINT] {} received trigger for checkpoint_id={}",
                        vertex_id, checkpoint_id
                    );
                    StreamTask::send_source_assigned_watermarks(
                        collectors,
                        &vertex_id,
                        metrics_labels,
                        source_watermark_manager.flush_pending(),
                    )
                    .await;
                    Some(CheckpointBarrierMessage::new(
                        vertex_id.as_ref().to_string(),
                        checkpoint_id,
                        execution_attempt_id,
                        Some(StreamTask::now_ms()),
                    ))
                }
                Err(mpsc::error::TryRecvError::Empty)
                | Err(mpsc::error::TryRecvError::Disconnected) => None,
            }
        } else {
            None
        };

        if let Some(barrier) = injected_barrier {
            on_checkpoint_barrier(
                source,
                master_client,
                &barrier,
                true,
                &vertex_id,
                runtime_context.task_index(),
                execution_attempt_id,
                metrics_labels,
                None,
            )
            .await?;
            let mut out = TransportOutput {
                collectors,
                vertex_id: vertex_id.clone(),
                labels: metrics_labels,
                stamp_ingest_if_missing: true,
                status: status.clone(),
            };
            out.emit(Message::CheckpointBarrier(barrier)).await?;
        } else {
            let use_timer = source_watermark_manager.assigner_enabled();
            let fetch = if use_timer {
                let deadline = source_watermark_manager.next_emit_deadline();
                tokio::select! {
                    biased;
                    res = SourceFetch::fetch_next(source, None) => Some(res),
                    _ = sleep_until_deadline(deadline) => {
                        StreamTask::send_source_assigned_watermarks(
                            collectors,
                            &vertex_id,
                            metrics_labels,
                            source_watermark_manager.try_emit_due(),
                        )
                        .await;
                        None
                    }
                }
            } else {
                Some(SourceFetch::fetch_next(source, None).await)
            };

            match fetch {
                None | Some(FetchResult::Interrupted) => {}
                Some(FetchResult::Data(mut message)) => {
                    let injected_wm = if matches!(message, Message::Regular(_)) {
                        source_watermark_manager.on_data_message(vertex_id.as_ref(), &message)
                    } else {
                        None
                    };
                    if let Message::Watermark(ref mut watermark) = message {
                        if let Some(new_watermark) =
                            source_watermark_manager.merge_watermark(watermark.clone()).await
                        {
                            watermark.watermark_value = new_watermark;
                        }
                        StreamTask::set_watermark_lag_gauge(
                            &vertex_id,
                            watermark.watermark_value,
                            metrics_labels,
                        );
                    }
                    {
                        let mut out = TransportOutput {
                            collectors,
                            vertex_id: vertex_id.clone(),
                            labels: metrics_labels,
                            stamp_ingest_if_missing: true,
                            status: status.clone(),
                        };
                        out.emit(message).await?;
                    }
                    if let Some(wm) = injected_wm {
                        StreamTask::send_source_assigned_watermarks(
                            collectors,
                            &vertex_id,
                            metrics_labels,
                            vec![wm],
                        )
                        .await;
                    }
                }
                Some(FetchResult::Idle) => {
                    StreamTask::send_source_assigned_watermarks(
                        collectors,
                        &vertex_id,
                        metrics_labels,
                        source_watermark_manager.flush_pending(),
                    )
                    .await;
                    let wm = Message::Watermark(WatermarkMessage::new(
                        vertex_id.as_ref().to_string(),
                        MAX_WATERMARK_VALUE,
                        Some(StreamTask::now_ms()),
                    ));
                    StreamTask::record_metrics(vertex_id.clone(), &wm, false, metrics_labels);
                    StreamTask::send_to_collectors_if_needed(collectors, wm).await;
                    status.store(StreamTaskStatus::Finished as u8, Ordering::SeqCst);
                    break;
                }
            }
        }

        StreamTask::report_task_time_metrics(
            &task_time_metrics,
            &mut metrics_window_start,
            &vertex_id,
            metrics_labels,
            &current_watermark,
        );
    }
    Ok(())
}

async fn on_checkpoint_barrier(
    operator: &mut dyn OperatorTrait,
    master_client: &mut Option<MasterServiceClient<tonic::transport::Channel>>,
    barrier: &CheckpointBarrierMessage,
    is_source: bool,
    vertex_id: &VertexId,
    task_index: i32,
    execution_attempt_id: u64,
    metrics_labels: Option<&MetricsLabels>,
    data_reader_control: Option<&DataReaderControl>,
) -> Result<()> {
    let checkpoint_id = barrier.checkpoint_id;
    let propagation_phase = if is_source {
        crate::runtime::master::server::master_service::CheckpointPropagationPhase::BarrierInjected
    } else {
        crate::runtime::master::server::master_service::CheckpointPropagationPhase::Aligned
    };
    StreamTask::report_checkpoint_propagation(
        master_client,
        checkpoint_id,
        vertex_id.as_ref(),
        task_index,
        execution_attempt_id,
        propagation_phase,
    )
    .await?;

    if operator_config_requires_checkpoint(operator.operator_config()) {
        println!(
            "[CHECKPOINT] {} checkpointing checkpoint_id={}",
            vertex_id, checkpoint_id
        );
        let started = Instant::now();
        let checkpoint_result = async {
            let checkpoint = operator.checkpoint(checkpoint_id).await?;
            let checkpoint_data = checkpoint.into_bytes();
            let payload_len = checkpoint_data.len() as u64;

            let report_resp = master_client
                .as_mut()
                .expect("Master client not initialized")
                .report_checkpoint(tonic::Request::new(
                    crate::runtime::master::server::master_service::ReportCheckpointRequest {
                        checkpoint_id,
                        vertex_id: vertex_id.as_ref().to_string(),
                        task_index,
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
                StreamTask::record_histogram(
                    METRIC_STREAM_TASK_CHECKPOINT_DURATION_MS,
                    duration_ms,
                    vertex_id,
                    metrics_labels,
                );
                StreamTask::record_histogram(
                    METRIC_STREAM_TASK_CHECKPOINT_PAYLOAD_BYTES,
                    payload_len as f64,
                    vertex_id,
                    metrics_labels,
                );
                increment_task_counter(
                    METRIC_STREAM_TASK_CHECKPOINT_SUCCESS,
                    1,
                    vertex_id.as_ref(),
                    metrics_labels,
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
                    metrics_labels,
                );
                return Err(error);
            }
        }
    }

    if let Some(ctrl) = data_reader_control {
        ctrl.unblock_all();
    }
    Ok(())
}
