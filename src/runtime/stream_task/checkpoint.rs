use std::collections::HashSet;
use std::time::Instant;

use anyhow::Result;

use crate::common::message::CheckpointBarrierMessage;
use crate::runtime::master::server::master_service::master_service_client::MasterServiceClient;
use crate::runtime::metrics::{
    increment_task_counter, METRIC_STREAM_TASK_CHECKPOINT_DURATION_MS,
    METRIC_STREAM_TASK_CHECKPOINT_FAILED, METRIC_STREAM_TASK_CHECKPOINT_PAYLOAD_BYTES,
    METRIC_STREAM_TASK_CHECKPOINT_SUCCESS,
};
use crate::runtime::operators::operator::{operator_config_requires_checkpoint, OperatorTrait};
use crate::transport::transport_client::DataReaderControl;

use super::ctx::TaskCtx;
use super::task::StreamTask;

#[derive(Debug)]
pub(crate) struct CheckpointAligner {
    upstream_count: usize,
    current_checkpoint: Option<u64>,
    seen_upstreams: HashSet<String>,
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
        barrier: &CheckpointBarrierMessage,
        expected_attempt_id: u64,
    ) -> Result<Option<(u64, Option<u64>, f64)>, String> {
        if barrier.execution_attempt_id != expected_attempt_id {
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

impl StreamTask {
    pub(super) async fn report_checkpoint_propagation(
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
}

pub(super) async fn on_checkpoint_barrier(
    operator: &mut dyn OperatorTrait,
    ctx: &mut TaskCtx<'_>,
    barrier: &CheckpointBarrierMessage,
    is_source: bool,
    data_reader_control: Option<&DataReaderControl>,
) -> Result<()> {
    let checkpoint_id = barrier.checkpoint_id;
    let task_index = ctx.runtime_context.task_index();
    let propagation_phase = if is_source {
        crate::runtime::master::server::master_service::CheckpointPropagationPhase::BarrierInjected
    } else {
        crate::runtime::master::server::master_service::CheckpointPropagationPhase::Aligned
    };
    StreamTask::report_checkpoint_propagation(
        ctx.master_client,
        checkpoint_id,
        ctx.vertex_id.as_ref(),
        task_index,
        ctx.execution_attempt_id,
        propagation_phase,
    )
    .await?;

    if operator_config_requires_checkpoint(operator.operator_config()) {
        println!(
            "[CHECKPOINT] {} checkpointing checkpoint_id={}",
            ctx.vertex_id, checkpoint_id
        );
        let started = Instant::now();
        let checkpoint_result = async {
            let checkpoint = operator.checkpoint(checkpoint_id).await?;
            let checkpoint_data = checkpoint.into_bytes();
            let payload_len = checkpoint_data.len() as u64;

            let report_resp = ctx
                .master_client
                .as_mut()
                .expect("Master client not initialized")
                .report_checkpoint(tonic::Request::new(
                    crate::runtime::master::server::master_service::ReportCheckpointRequest {
                        checkpoint_id,
                        vertex_id: ctx.vertex_id.as_ref().to_string(),
                        task_index,
                        checkpoint_data,
                        execution_attempt_id: ctx.execution_attempt_id,
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
                    &ctx.vertex_id,
                    ctx.metrics_labels,
                );
                StreamTask::record_histogram(
                    METRIC_STREAM_TASK_CHECKPOINT_PAYLOAD_BYTES,
                    payload_len as f64,
                    &ctx.vertex_id,
                    ctx.metrics_labels,
                );
                increment_task_counter(
                    METRIC_STREAM_TASK_CHECKPOINT_SUCCESS,
                    1,
                    ctx.vertex_id.as_ref(),
                    ctx.metrics_labels,
                );
                println!(
                    "[CHECKPOINT] {} reported checkpoint_id={}",
                    ctx.vertex_id, checkpoint_id
                );
            }
            Err(error) => {
                increment_task_counter(
                    METRIC_STREAM_TASK_CHECKPOINT_FAILED,
                    1,
                    ctx.vertex_id.as_ref(),
                    ctx.metrics_labels,
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
