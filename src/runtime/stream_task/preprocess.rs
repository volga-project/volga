use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8};
use std::sync::Arc;
use std::time::Instant;

use async_stream::stream;
use futures::StreamExt;
use tokio::sync::Mutex;

use crate::common::message::{Message, WatermarkMessage};
use crate::common::MAX_WATERMARK_VALUE;
use crate::runtime::metrics::{MetricsLabels, METRIC_STREAM_TASK_CHECKPOINT_ALIGN_WAIT_MS};
use crate::runtime::operators::operator::MessageStream;
use crate::runtime::watermark::WatermarkAssignConfig;

use super::checkpoint::CheckpointAligner;
use super::task::StreamTask;
use super::watermark::WatermarkManager;
use crate::runtime::VertexId;

pub(super) async fn sleep_until_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(when) => {
            tokio::time::sleep_until(tokio::time::Instant::from_std(when)).await;
        }
        None => std::future::pending::<()>().await,
    }
}

async fn push_assigned_watermarks(
    manager: &mut WatermarkManager,
    vertex_id: &VertexId,
    labels: Option<&MetricsLabels>,
    outgoing: &mut Vec<Message>,
    wms: Vec<WatermarkMessage>,
) {
    for wm in wms {
        if let Some(out) = merge_assigned_watermark(manager, vertex_id, labels, wm).await {
            outgoing.push(Message::Watermark(out));
        }
    }
}

async fn merge_assigned_watermark(
    manager: &mut WatermarkManager,
    vertex_id: &VertexId,
    labels: Option<&MetricsLabels>,
    wm: WatermarkMessage,
) -> Option<WatermarkMessage> {
    let new_watermark = manager.merge_watermark(wm).await?;
    StreamTask::set_watermark_lag_gauge(vertex_id, new_watermark, labels);
    Some(WatermarkMessage::new(
        vertex_id.as_ref().to_string(),
        new_watermark,
        Some(StreamTask::now_ms()),
    ))
}

/// Watermark assignment + merge, coalesce timer, and checkpoint barrier alignment.
/// Does not yield barriers to the operator; the run loop sees them on this stream.
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
        loop {
            let deadline = watermark_manager.next_emit_deadline();
            let mut outgoing: Vec<Message> = Vec::new();
            let mut input_ended = false;
            tokio::select! {
                biased;
                message = input_stream.next() => {
                    if let Some(message) = message {
                    StreamTask::record_metrics(vertex_id.clone(), &message, true, labels.as_ref());
                    StreamTask::record_control_propagation(&vertex_id, &message, labels.as_ref());

                    match &message {
                        Message::Watermark(watermark) => {
                            if watermark.watermark_value == MAX_WATERMARK_VALUE {
                                let wms = watermark_manager.flush_pending();
                                push_assigned_watermarks(
                                    &mut watermark_manager,
                                    &vertex_id,
                                    labels.as_ref(),
                                    &mut outgoing,
                                    wms,
                                ).await;
                            }
                            if watermark_manager.assigner_enabled() && watermark.watermark_value != MAX_WATERMARK_VALUE {
                                // Drop punctuated upstream WMs when this task assigns.
                            } else if let Some(new_watermark) = watermark_manager.merge_watermark(watermark.clone()).await {
                                StreamTask::set_watermark_lag_gauge(&vertex_id, new_watermark, labels.as_ref());
                                let mut wm = watermark.clone();
                                wm.watermark_value = new_watermark;
                                wm.metadata.upstream_vertex_id = Some(vertex_id.as_ref().to_string());
                                outgoing.push(Message::Watermark(wm));
                            }
                        }
                        Message::CheckpointBarrier(barrier) => {
                            match aligner.on_barrier(barrier, execution_attempt_id) {
                                Ok(Some((checkpoint_id, inject_stamp, align_wait_ms))) => {
                                    StreamTask::record_histogram(
                                        METRIC_STREAM_TASK_CHECKPOINT_ALIGN_WAIT_MS,
                                        align_wait_ms,
                                        &vertex_id,
                                        labels.as_ref(),
                                    );
                                    // Held coalesced WMs must precede the barrier so downstream
                                    // snapshots include event time the assigner already has.
                                    let wms = watermark_manager.flush_pending();
                                    push_assigned_watermarks(
                                        &mut watermark_manager,
                                        &vertex_id,
                                        labels.as_ref(),
                                        &mut outgoing,
                                        wms,
                                    ).await;
                                    outgoing.push(Message::CheckpointBarrier(
                                        crate::common::message::CheckpointBarrierMessage::new(
                                            vertex_id.as_ref().to_string(),
                                            checkpoint_id,
                                            execution_attempt_id,
                                            inject_stamp,
                                        ),
                                    ));
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
                            let upstream_for_msg = message
                                .upstream_vertex_id()
                                .unwrap_or_else(|| vertex_id.as_ref().to_string());
                            let injected = watermark_manager.on_data_message(&upstream_for_msg, &message);
                            outgoing.push(message);
                            if let Some(wm) = injected {
                                push_assigned_watermarks(
                                    &mut watermark_manager,
                                    &vertex_id,
                                    labels.as_ref(),
                                    &mut outgoing,
                                    vec![wm],
                                ).await;
                            }
                        }
                    }
                    } else {
                        let wms = watermark_manager.flush_pending();
                        push_assigned_watermarks(
                            &mut watermark_manager,
                            &vertex_id,
                            labels.as_ref(),
                            &mut outgoing,
                            wms,
                        ).await;
                        input_ended = true;
                    }
                }
                _ = sleep_until_deadline(deadline) => {
                    let wms = watermark_manager.try_emit_due();
                    push_assigned_watermarks(
                        &mut watermark_manager,
                        &vertex_id,
                        labels.as_ref(),
                        &mut outgoing,
                        wms,
                    ).await;
                }
            }
            for message in outgoing {
                yield message;
            }
            if input_ended {
                break;
            }
        }
    })
}
