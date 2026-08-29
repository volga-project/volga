use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::{mpsc, watch, Mutex};

use crate::common::grpc::master::master_client as connect_master_client;
use crate::common::grpc::GrpcConfig;
use crate::runtime::checkpoint::SerializedRestore;
use crate::runtime::collector::Collector;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::health::WorkerHealth;
use crate::runtime::master::server::master_service::master_service_client::MasterServiceClient;
use crate::runtime::metrics::MetricsLabels;
use crate::runtime::observability::StreamTaskStatus;
use crate::runtime::operators::operator::{create_operator, Operator, OperatorTrait, OperatorType};
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::VertexId;
use crate::transport::transport_client::{TransportClient, TransportClientConfig};

use super::checkpoint::CheckpointAligner;
use super::metrics::{input_with_idle_tracking, TaskTimeMetrics};
use super::preprocess::create_preprocessed_input_stream;
use super::processor::{processor_loop, ProcessorLoopParams};
use super::source::{source_loop, SourceLoopParams};
use super::task::{timestamp, StreamTask};
use super::watermark::WatermarkManager;

pub(super) struct RunParams {
    pub vertex_id: VertexId,
    pub runtime_context: RuntimeContext,
    pub status: Arc<AtomicU8>,
    pub operator_config: crate::runtime::operators::operator::OperatorConfig,
    pub execution_graph: ExecutionGraph,
    pub master_addr: Option<String>,
    pub restore_data: Option<SerializedRestore>,
    pub execution_attempt_id: u64,
    pub worker_health: Arc<WorkerHealth>,
    pub upstream_watermarks: Arc<Mutex<HashMap<String, u64>>>,
    pub current_watermark: Arc<AtomicU64>,
    pub upstream_vertices: Vec<String>,
    pub transport_client_config: TransportClientConfig,
    pub metrics_labels: Option<MetricsLabels>,
    pub run_receiver: watch::Receiver<bool>,
    pub close_receiver: watch::Receiver<bool>,
    pub checkpoint_receiver: mpsc::UnboundedReceiver<u64>,
}

pub(super) async fn run(params: RunParams) -> Result<()> {
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
