use std::time::Instant;

use crate::common::message::{CheckpointBarrierMessage, Message, WatermarkMessage};
use crate::common::MAX_WATERMARK_VALUE;
use crate::runtime::metrics::{MetricsLabels, METRIC_STREAM_TASK_CHECKPOINT_ALIGN_WAIT_MS};
use crate::runtime::VertexId;

use super::checkpoint::CheckpointAligner;
use super::task::StreamTask;
use super::watermark::WatermarkManager;

pub(super) async fn sleep_until_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(when) => {
            tokio::time::sleep_until(tokio::time::Instant::from_std(when)).await;
        }
        None => std::future::pending::<()>().await,
    }
}

async fn merge_assigned(
    manager: &mut WatermarkManager,
    vertex_id: &str,
    wm: WatermarkMessage,
) -> Option<WatermarkMessage> {
    let new_watermark = manager.merge_watermark(wm).await?;
    Some(WatermarkMessage::new(
        vertex_id.to_string(),
        new_watermark,
        Some(StreamTask::now_ms()),
    ))
}

async fn merge_emits(
    manager: &mut WatermarkManager,
    vertex_id: &str,
    wms: Vec<WatermarkMessage>,
) -> Vec<WatermarkMessage> {
    let mut out = Vec::new();
    for wm in wms {
        if let Some(merged) = merge_assigned(manager, vertex_id, wm).await {
            out.push(merged);
        }
    }
    out
}

pub(super) fn assign_on_data(
    manager: &mut WatermarkManager,
    vertex_id: &str,
    message: &Message,
) -> Option<WatermarkMessage> {
    let upstream = message
        .upstream_vertex_id()
        .unwrap_or_else(|| vertex_id.to_string());
    manager.on_data_message(&upstream, message)
}

pub(super) async fn assign_and_merge(
    manager: &mut WatermarkManager,
    vertex_id: &str,
    message: &Message,
) -> Option<WatermarkMessage> {
    let wm = assign_on_data(manager, vertex_id, message)?;
    merge_assigned(manager, vertex_id, wm).await
}

pub(super) async fn on_upstream_watermark(
    manager: &mut WatermarkManager,
    vertex_id: &str,
    watermark: WatermarkMessage,
) -> Vec<WatermarkMessage> {
    let mut out = Vec::new();
    if watermark.watermark_value == MAX_WATERMARK_VALUE {
        out.extend(flush_pending(manager, vertex_id).await);
    }
    if manager.assigner_enabled() && watermark.watermark_value != MAX_WATERMARK_VALUE {
        return out;
    }
    if let Some(new_watermark) = manager.merge_watermark(watermark.clone()).await {
        let mut wm = watermark;
        wm.watermark_value = new_watermark;
        wm.metadata.upstream_vertex_id = Some(vertex_id.to_string());
        out.push(wm);
    }
    out
}

pub(super) async fn on_aligned_barrier(
    aligner: &mut CheckpointAligner,
    manager: &mut WatermarkManager,
    vertex_id: &VertexId,
    labels: Option<&MetricsLabels>,
    execution_attempt_id: u64,
    barrier: &CheckpointBarrierMessage,
) -> Result<Option<(u64, Option<u64>, Vec<WatermarkMessage>)>, String> {
    let Some((checkpoint_id, inject_stamp, align_wait_ms)) =
        aligner.on_barrier(barrier, execution_attempt_id)?
    else {
        return Ok(None);
    };
    StreamTask::record_histogram(
        METRIC_STREAM_TASK_CHECKPOINT_ALIGN_WAIT_MS,
        align_wait_ms,
        vertex_id,
        labels,
    );
    let wms = flush_pending(manager, vertex_id.as_ref()).await;
    Ok(Some((checkpoint_id, inject_stamp, wms)))
}

pub(super) async fn on_timer(
    manager: &mut WatermarkManager,
    vertex_id: &str,
) -> Vec<WatermarkMessage> {
    let due = manager.try_emit_due();
    merge_emits(manager, vertex_id, due).await
}

pub(super) async fn flush_pending(
    manager: &mut WatermarkManager,
    vertex_id: &str,
) -> Vec<WatermarkMessage> {
    let pending = manager.flush_pending();
    merge_emits(manager, vertex_id, pending).await
}

#[cfg(test)]
pub(super) fn empty_aligner(upstreams: &[String]) -> CheckpointAligner {
    use crate::transport::transport_client::DataReaderControl;
    CheckpointAligner::new(upstreams, DataReaderControl::empty_for_test())
}
