use std::time::Instant;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::common::message::{CheckpointBarrierMessage, Message, WatermarkMessage};
use crate::common::MAX_WATERMARK_VALUE;
use crate::runtime::functions::source::FetchResult;
use crate::runtime::observability::StreamTaskStatus;
use crate::runtime::operators::operator::{
    operator_config_requires_checkpoint, OperatorTrait, Output, SourceOperator as SourceFetch,
};
use crate::runtime::operators::source::source_operator::SourceOperator;

use super::checkpoint::on_checkpoint_barrier;
use super::ctx::TaskCtx;
use super::mailbox::sleep_until_deadline;
use super::task::StreamTask;
use super::watermark::WatermarkManager;

pub(super) struct SourceLoopParams<'a> {
    pub ctx: TaskCtx<'a>,
    pub checkpoint_receiver: &'a mut mpsc::UnboundedReceiver<u64>,
    pub source_watermark_manager: &'a mut WatermarkManager,
}

pub(super) async fn source_loop(
    source: &mut SourceOperator,
    params: SourceLoopParams<'_>,
) -> Result<()> {
    let SourceLoopParams {
        mut ctx,
        checkpoint_receiver,
        source_watermark_manager,
    } = params;

    let mut metrics_window_start = Instant::now();
    let checkpointable = operator_config_requires_checkpoint(source.operator_config());

    while ctx.status.load(std::sync::atomic::Ordering::SeqCst) == StreamTaskStatus::Running as u8 {
        let injected_barrier = if checkpointable {
            match checkpoint_receiver.try_recv() {
                Ok(checkpoint_id) => {
                    println!(
                        "[CHECKPOINT] {} received trigger for checkpoint_id={}",
                        ctx.vertex_id, checkpoint_id
                    );
                    emit_assigned(
                        &mut ctx,
                        source_watermark_manager.flush_pending(),
                    )
                    .await?;
                    Some(CheckpointBarrierMessage::new(
                        ctx.vertex_id.as_ref().to_string(),
                        checkpoint_id,
                        ctx.execution_attempt_id,
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
                ctx.master_client,
                &barrier,
                true,
                &ctx.vertex_id,
                ctx.runtime_context.task_index(),
                ctx.execution_attempt_id,
                ctx.metrics_labels,
                None,
            )
            .await?;
            let mut out = ctx.output(true);
            out.emit(Message::CheckpointBarrier(barrier)).await?;
        } else {
            let use_timer = source_watermark_manager.assigner_enabled();
            let idle_start = Instant::now();
            let fetch = if use_timer {
                let deadline = source_watermark_manager.next_emit_deadline();
                tokio::select! {
                    biased;
                    res = SourceFetch::fetch_next(source, None) => {
                        ctx.task_time_metrics
                            .add_idle_ns(idle_start.elapsed().as_nanos() as u64);
                        Some(res)
                    }
                    _ = sleep_until_deadline(deadline) => {
                        ctx.task_time_metrics
                            .add_idle_ns(idle_start.elapsed().as_nanos() as u64);
                        emit_assigned(&mut ctx, source_watermark_manager.try_emit_due()).await?;
                        None
                    }
                }
            } else {
                let res = SourceFetch::fetch_next(source, None).await;
                ctx.task_time_metrics
                    .add_idle_ns(idle_start.elapsed().as_nanos() as u64);
                Some(res)
            };

            match fetch {
                None | Some(FetchResult::Interrupted) => {}
                Some(FetchResult::Data(mut message)) => {
                    let injected_wm = if matches!(message, Message::Regular(_)) {
                        source_watermark_manager.on_data_message(ctx.vertex_id.as_ref(), &message)
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
                            &ctx.vertex_id,
                            watermark.watermark_value,
                            ctx.metrics_labels,
                        );
                    }
                    {
                        let mut out = ctx.output(true);
                        out.emit(message).await?;
                    }
                    if let Some(wm) = injected_wm {
                        emit_assigned(&mut ctx, vec![wm]).await?;
                    }
                }
                Some(FetchResult::Idle) => {
                    emit_assigned(&mut ctx, source_watermark_manager.flush_pending()).await?;
                    let max_wm = WatermarkMessage::new(
                        ctx.vertex_id.as_ref().to_string(),
                        MAX_WATERMARK_VALUE,
                        Some(StreamTask::now_ms()),
                    );
                    let mut out = ctx.output(true);
                    out.emit(Message::Watermark(max_wm)).await?;
                    break;
                }
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

async fn emit_assigned(
    ctx: &mut TaskCtx<'_>,
    wms: Vec<WatermarkMessage>,
) -> Result<()> {
    if wms.is_empty() {
        return Ok(());
    }
    let mut out = ctx.output(true);
    for wm in wms {
        out.emit(Message::Watermark(wm)).await?;
    }
    Ok(())
}
