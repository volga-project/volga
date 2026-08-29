use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Mutex;

use crate::common::message::{CheckpointBarrierMessage, Message, WatermarkMessage};
use crate::common::MAX_WATERMARK_VALUE;
use crate::runtime::metrics::{MetricsLabels, METRIC_STREAM_TASK_CHECKPOINT_ALIGN_WAIT_MS};
use crate::runtime::watermark::WatermarkAssignConfig;
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

/// Mailbox-side WM assign/merge and barrier align. Operators only see the result.
pub(super) struct MailboxFront {
    vertex_id: VertexId,
    manager: WatermarkManager,
    aligner: CheckpointAligner,
    labels: Option<MetricsLabels>,
    execution_attempt_id: u64,
}

impl MailboxFront {
    pub(super) fn new(
        vertex_id: VertexId,
        watermark_assign: Option<WatermarkAssignConfig>,
        upstream_vertices: Vec<String>,
        upstream_watermarks: Arc<Mutex<HashMap<String, u64>>>,
        current_watermark: Arc<AtomicU64>,
        aligner: CheckpointAligner,
        labels: Option<MetricsLabels>,
        execution_attempt_id: u64,
    ) -> Self {
        let manager = WatermarkManager::new(
            watermark_assign,
            upstream_vertices,
            upstream_watermarks,
            current_watermark,
        );
        Self {
            vertex_id,
            manager,
            aligner,
            labels,
            execution_attempt_id,
        }
    }

    pub(super) fn next_emit_deadline(&self) -> Option<Instant> {
        self.manager.next_emit_deadline()
    }

    pub(super) async fn merge_assigned(
        &mut self,
        wm: WatermarkMessage,
    ) -> Option<WatermarkMessage> {
        let new_watermark = self.manager.merge_watermark(wm).await?;
        StreamTask::set_watermark_lag_gauge(&self.vertex_id, new_watermark, self.labels.as_ref());
        Some(WatermarkMessage::new(
            self.vertex_id.as_ref().to_string(),
            new_watermark,
            Some(StreamTask::now_ms()),
        ))
    }

    pub(super) fn assign_on_data(&mut self, message: &Message) -> Option<WatermarkMessage> {
        let upstream = message
            .upstream_vertex_id()
            .unwrap_or_else(|| self.vertex_id.as_ref().to_string());
        self.manager.on_data_message(&upstream, message)
    }

    pub(super) async fn assign_and_merge(
        &mut self,
        message: &Message,
    ) -> Option<WatermarkMessage> {
        let wm = self.assign_on_data(message)?;
        self.merge_assigned(wm).await
    }

    pub(super) async fn on_upstream_watermark(
        &mut self,
        watermark: WatermarkMessage,
    ) -> Vec<WatermarkMessage> {
        let mut out = Vec::new();
        if watermark.watermark_value == MAX_WATERMARK_VALUE {
            out.extend(self.flush_pending().await);
        }
        if self.manager.assigner_enabled() && watermark.watermark_value != MAX_WATERMARK_VALUE {
            return out;
        }
        if let Some(new_watermark) = self.manager.merge_watermark(watermark.clone()).await {
            StreamTask::set_watermark_lag_gauge(
                &self.vertex_id,
                new_watermark,
                self.labels.as_ref(),
            );
            let mut wm = watermark;
            wm.watermark_value = new_watermark;
            wm.metadata.upstream_vertex_id = Some(self.vertex_id.as_ref().to_string());
            out.push(wm);
        }
        out
    }

    pub(super) async fn on_aligned_barrier(
        &mut self,
        barrier: &CheckpointBarrierMessage,
    ) -> Result<Option<(u64, Option<u64>, f64, Vec<WatermarkMessage>)>, String> {
        let Some((checkpoint_id, inject_stamp, align_wait_ms)) =
            self.aligner.on_barrier(barrier, self.execution_attempt_id)?
        else {
            return Ok(None);
        };
        StreamTask::record_histogram(
            METRIC_STREAM_TASK_CHECKPOINT_ALIGN_WAIT_MS,
            align_wait_ms,
            &self.vertex_id,
            self.labels.as_ref(),
        );
        let wms = self.flush_pending().await;
        Ok(Some((checkpoint_id, inject_stamp, align_wait_ms, wms)))
    }

    pub(super) async fn on_timer(&mut self) -> Vec<WatermarkMessage> {
        let mut out = Vec::new();
        for wm in self.manager.try_emit_due() {
            if let Some(merged) = self.merge_assigned(wm).await {
                out.push(merged);
            }
        }
        out
    }

    pub(super) async fn flush_pending(&mut self) -> Vec<WatermarkMessage> {
        let mut out = Vec::new();
        for wm in self.manager.flush_pending() {
            if let Some(merged) = self.merge_assigned(wm).await {
                out.push(merged);
            }
        }
        out
    }
}

#[cfg(test)]
impl MailboxFront {
    pub(super) fn for_test(
        vertex_id: VertexId,
        watermark_assign: Option<WatermarkAssignConfig>,
        upstream_vertices: Vec<String>,
        aligner: CheckpointAligner,
    ) -> Self {
        Self::new(
            vertex_id,
            watermark_assign,
            upstream_vertices,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(AtomicU64::new(0)),
            aligner,
            None,
            0,
        )
    }
}

#[cfg(test)]
pub(super) fn empty_aligner(upstreams: &[String]) -> CheckpointAligner {
    use crate::transport::transport_client::DataReaderControl;
    CheckpointAligner::new(upstreams, DataReaderControl::empty_for_test())
}
