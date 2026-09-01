use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;

use crate::common::message::{CheckpointBarrierMessage, Message, WatermarkMessage};
use crate::common::MAX_WATERMARK_VALUE;
use crate::runtime::collector::Collector;
use crate::runtime::master::server::master_service::master_service_client::MasterServiceClient;
use crate::runtime::metrics::MetricsLabels;
use crate::runtime::observability::StreamTaskStatus;
use crate::runtime::operators::operator::{Output, StreamOperator};
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::VertexId;

use super::metrics::TaskTimeMetrics;
use super::output::TransportOutput;
use super::task::StreamTask;

pub(super) struct TaskCtx<'a> {
    pub vertex_id: VertexId,
    pub runtime_context: &'a RuntimeContext,
    pub status: Arc<AtomicU8>,
    pub execution_attempt_id: u64,
    pub collectors: &'a mut HashMap<String, Collector>,
    pub master_client: &'a mut Option<MasterServiceClient<tonic::transport::Channel>>,
    pub metrics_labels: Option<&'a MetricsLabels>,
    pub task_time_metrics: Arc<TaskTimeMetrics>,
    pub current_watermark: Arc<AtomicU64>,
}

impl TaskCtx<'_> {
    pub fn output(&mut self) -> TransportOutput<'_> {
        TransportOutput {
            collectors: self.collectors,
            vertex_id: self.vertex_id.clone(),
            labels: self.metrics_labels,
        }
    }

    pub fn is_running(&self) -> bool {
        self.status.load(Ordering::SeqCst) == StreamTaskStatus::Running as u8
    }

    pub fn mark_finished(&self) {
        self.status
            .store(StreamTaskStatus::Finished as u8, Ordering::SeqCst);
    }

    pub fn add_idle(&self, idle_start: Instant) {
        self.task_time_metrics
            .add_idle_ns(idle_start.elapsed().as_nanos() as u64);
    }

    pub fn record_recv(&self, message: &Message) {
        StreamTask::record_metrics(self.vertex_id.clone(), message, true, self.metrics_labels);
        self.record_control(message);
    }

    pub fn record_control(&self, message: &Message) {
        StreamTask::record_control_propagation(&self.vertex_id, message, self.metrics_labels);
    }

    pub fn report_time_metrics(&self, window_start: &mut Instant) {
        StreamTask::report_task_time_metrics(
            &self.task_time_metrics,
            window_start,
            &self.vertex_id,
            self.metrics_labels,
            &self.current_watermark,
        );
    }

    pub fn new_barrier(
        &self,
        checkpoint_id: u64,
        inject_stamp: Option<u64>,
    ) -> CheckpointBarrierMessage {
        CheckpointBarrierMessage::new(
            self.vertex_id.as_ref().to_string(),
            checkpoint_id,
            self.execution_attempt_id,
            inject_stamp,
        )
    }

    pub fn mark_finished_if_max_wm(&self, wm: &WatermarkMessage) {
        if wm.watermark_value == MAX_WATERMARK_VALUE {
            self.mark_finished();
        }
    }

    pub fn stamp_ingest_if_missing(message: &mut Message) {
        if message.ingest_timestamp().is_none() {
            message.set_ingest_timestamp(StreamTask::now_ms());
        }
    }

    pub async fn emit_watermarks(
        &mut self,
        wms: Vec<WatermarkMessage>,
        stamp_ingest: bool,
        mut through: Option<&mut dyn StreamOperator>,
    ) -> Result<()> {
        if wms.is_empty() {
            return Ok(());
        }
        for wm in &wms {
            self.mark_finished_if_max_wm(wm);
        }
        let mut out = self.output();
        for wm in wms {
            if let Some(op) = through.as_mut() {
                op.handle_watermark(wm, &mut out).await?;
            } else {
                let mut message = Message::Watermark(wm);
                if stamp_ingest {
                    Self::stamp_ingest_if_missing(&mut message);
                }
                out.emit(message).await?;
            }
        }
        Ok(())
    }
}
