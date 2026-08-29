use std::time::Instant;

use anyhow::Result;
use futures::StreamExt;

use crate::common::message::Message;
use crate::runtime::observability::StreamTaskStatus;
use crate::runtime::operators::operator::{
    drain_ready_after, MessageStream, StreamOperator, INGEST_MAX_RECORDS,
};
use crate::transport::transport_client::DataReaderControl;

use super::checkpoint::on_checkpoint_barrier;
use super::ctx::TaskCtx;
use super::progress::{sleep_until_deadline, InputProgress};
use super::task::{timestamp, StreamTask};

pub(super) async fn processor_loop(
    operator: &mut dyn StreamOperator,
    mut ctx: TaskCtx<'_>,
    mailbox: MessageStream,
    mut progress: InputProgress,
    reader_control: DataReaderControl,
) -> Result<()> {
    let mut mailbox = mailbox.peekable();
    let mut metrics_window_start = Instant::now();

    while ctx.status.load(std::sync::atomic::Ordering::SeqCst) == StreamTaskStatus::Running as u8 {
        let idle_start = Instant::now();
        tokio::select! {
            biased;
            message = mailbox.next() => {
                ctx.task_time_metrics
                    .add_idle_ns(idle_start.elapsed().as_nanos() as u64);
                match message {
                    None => {
                        ctx.emit_watermarks(progress.flush_pending().await, false, Some(operator))
                            .await?;
                        println!(
                            "{:?} StreamTask {}: upstream ended without terminal watermark; finishing",
                            timestamp(),
                            ctx.vertex_id.as_ref()
                        );
                        ctx.mark_finished();
                        break;
                    }
                    Some(Message::Watermark(wm)) => {
                        let msg = Message::Watermark(wm);
                        StreamTask::record_control_propagation(
                            &ctx.vertex_id,
                            &msg,
                            ctx.metrics_labels,
                        );
                        let Message::Watermark(wm) = msg else {
                            unreachable!()
                        };
                        ctx.emit_watermarks(
                            progress.on_upstream_watermark(wm).await,
                            false,
                            Some(operator),
                        )
                        .await?;
                    }
                    Some(Message::CheckpointBarrier(barrier)) => {
                        let msg = Message::CheckpointBarrier(barrier);
                        StreamTask::record_control_propagation(
                            &ctx.vertex_id,
                            &msg,
                            ctx.metrics_labels,
                        );
                        let Message::CheckpointBarrier(barrier) = msg else {
                            unreachable!()
                        };
                        match progress.on_aligned_barrier(&barrier).await {
                            Ok(None) => {}
                            Ok(Some((checkpoint_id, inject_stamp, wms))) => {
                                ctx.emit_watermarks(wms, false, Some(operator)).await?;
                                let aligned = crate::common::message::CheckpointBarrierMessage::new(
                                    ctx.vertex_id.as_ref().to_string(),
                                    checkpoint_id,
                                    ctx.execution_attempt_id,
                                    inject_stamp,
                                );
                                on_checkpoint_barrier(
                                    operator,
                                    &mut ctx,
                                    &aligned,
                                    false,
                                    Some(&reader_control),
                                )
                                .await?;
                                let mut out = ctx.output();
                                operator.handle_barrier(aligned, &mut out).await?;
                            }
                            Err(error) => {
                                panic!(
                                    "[CHECKPOINT] {} alignment failed: {}",
                                    ctx.vertex_id.as_ref(),
                                    error
                                );
                            }
                        }
                    }
                    Some(first) => {
                        record_recv(&ctx, &first);
                        let mut assigned = Vec::new();
                        if let Some(wm) = progress.assign_and_merge(&first).await {
                            assigned.push(wm);
                        }
                        let batch =
                            drain_ready_after(first, &mut mailbox, INGEST_MAX_RECORDS).await;
                        for extra in &batch[1..] {
                            record_recv(&ctx, extra);
                            if let Some(wm) = progress.assign_and_merge(extra).await {
                                assigned.push(wm);
                            }
                        }
                        {
                            let mut out = ctx.output();
                            operator.process_data(batch, &mut out).await?;
                        }
                        ctx.emit_watermarks(assigned, false, Some(operator)).await?;
                    }
                }
            }
            _ = sleep_until_deadline(progress.next_emit_deadline()) => {
                ctx.task_time_metrics
                    .add_idle_ns(idle_start.elapsed().as_nanos() as u64);
                ctx.emit_watermarks(progress.on_timer().await, false, Some(operator))
                    .await?;
            }
        }

        StreamTask::report_task_time_metrics(
            &ctx.task_time_metrics,
            &mut metrics_window_start,
            &ctx.vertex_id,
            ctx.metrics_labels,
            &ctx.current_watermark,
        );
    }
    Ok(())
}

fn record_recv(ctx: &TaskCtx<'_>, message: &Message) {
    StreamTask::record_metrics(ctx.vertex_id.clone(), message, true, ctx.metrics_labels);
    StreamTask::record_control_propagation(&ctx.vertex_id, message, ctx.metrics_labels);
}
