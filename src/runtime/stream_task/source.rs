use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::common::message::{CheckpointBarrierMessage, Message, WatermarkMessage};
use crate::common::MAX_WATERMARK_VALUE;
use crate::runtime::collector::Collector;
use crate::runtime::functions::source::FetchResult;
use crate::runtime::master::server::master_service::master_service_client::MasterServiceClient;
use crate::runtime::metrics::MetricsLabels;
use crate::runtime::observability::StreamTaskStatus;
use crate::runtime::operators::operator::{
    operator_config_requires_checkpoint, OperatorTrait, Output, SourceOperator as SourceFetch,
};
use crate::runtime::operators::source::source_operator::SourceOperator;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::VertexId;

use super::checkpoint::on_checkpoint_barrier;
use super::metrics::TaskTimeMetrics;
use super::output::TransportOutput;
use super::preprocess::sleep_until_deadline;
use super::task::StreamTask;
use super::watermark::WatermarkManager;

pub(super) struct SourceLoopParams<'a> {
    pub vertex_id: VertexId,
    pub runtime_context: &'a RuntimeContext,
    pub status: Arc<AtomicU8>,
    pub execution_attempt_id: u64,
    pub collectors: &'a mut HashMap<String, Collector>,
    pub master_client: &'a mut Option<MasterServiceClient<tonic::transport::Channel>>,
    pub metrics_labels: Option<&'a MetricsLabels>,
    pub task_time_metrics: Arc<TaskTimeMetrics>,
    pub current_watermark: Arc<AtomicU64>,
    pub checkpoint_receiver: &'a mut mpsc::UnboundedReceiver<u64>,
    pub source_watermark_manager: &'a mut WatermarkManager,
}

pub(super) async fn source_loop(
    source: &mut SourceOperator,
    params: SourceLoopParams<'_>,
) -> Result<()> {
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
