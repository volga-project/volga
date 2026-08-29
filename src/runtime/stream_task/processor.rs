use std::time::Instant;

use anyhow::Result;
use futures::StreamExt;

use crate::common::message::{CheckpointBarrierMessage, Message, WatermarkMessage};
use crate::runtime::observability::StreamTaskStatus;
use crate::runtime::operators::operator::{
    drain_ready_after, MessageStream, StreamOperator, INGEST_MAX_RECORDS,
};
use crate::transport::transport_client::DataReaderControl;

use super::checkpoint::on_checkpoint_barrier;
use super::ctx::TaskCtx;
use super::mailbox::{sleep_until_deadline, MailboxFront};
use super::task::{timestamp, StreamTask};

pub(super) async fn processor_loop(
    operator: &mut dyn StreamOperator,
    mut ctx: TaskCtx<'_>,
    mailbox: MessageStream,
    mut front: MailboxFront,
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
                        emit_watermarks(operator, &mut ctx, front.flush_pending().await).await?;
                        println!(
                            "{:?} StreamTask {}: upstream ended without terminal watermark; finishing",
                            timestamp(),
                            ctx.vertex_id.as_ref()
                        );
                        ctx.status.store(
                            StreamTaskStatus::Finished as u8,
                            std::sync::atomic::Ordering::SeqCst,
                        );
                        break;
                    }
                    Some(Message::Watermark(wm)) => {
                        record_recv(&ctx, &Message::Watermark(wm.clone()));
                        emit_watermarks(operator, &mut ctx, front.on_upstream_watermark(wm).await)
                            .await?;
                    }
                    Some(Message::CheckpointBarrier(barrier)) => {
                        record_recv(&ctx, &Message::CheckpointBarrier(barrier.clone()));
                        match front.on_aligned_barrier(&barrier).await {
                            Ok(None) => {}
                            Ok(Some((checkpoint_id, inject_stamp, wms))) => {
                                emit_watermarks(operator, &mut ctx, wms).await?;
                                let aligned = CheckpointBarrierMessage::new(
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
                                let mut out = ctx.output(false);
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
                        if let Some(wm) = front.assign_and_merge(&first).await {
                            assigned.push(wm);
                        }
                        let batch =
                            drain_ready_after(first, &mut mailbox, INGEST_MAX_RECORDS).await;
                        for extra in &batch[1..] {
                            record_recv(&ctx, extra);
                            if let Some(wm) = front.assign_and_merge(extra).await {
                                assigned.push(wm);
                            }
                        }
                        {
                            let mut out = ctx.output(false);
                            operator.process_data(batch, &mut out).await?;
                        }
                        emit_watermarks(operator, &mut ctx, assigned).await?;
                    }
                }
            }
            _ = sleep_until_deadline(front.next_emit_deadline()) => {
                ctx.task_time_metrics
                    .add_idle_ns(idle_start.elapsed().as_nanos() as u64);
                emit_watermarks(operator, &mut ctx, front.on_timer().await).await?;
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

async fn emit_watermarks(
    operator: &mut dyn StreamOperator,
    ctx: &mut TaskCtx<'_>,
    wms: Vec<WatermarkMessage>,
) -> Result<()> {
    if wms.is_empty() {
        return Ok(());
    }
    let mut out = ctx.output(false);
    for wm in wms {
        operator.handle_watermark(wm, &mut out).await?;
    }
    Ok(())
}
