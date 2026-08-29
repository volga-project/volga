use anyhow::Result;
use tokio::sync::mpsc;

use crate::common::message::{CheckpointBarrierMessage, Message, WatermarkMessage};
use crate::common::MAX_WATERMARK_VALUE;
use crate::runtime::functions::source::FetchResult;
use crate::runtime::observability::StreamTaskStatus;
use crate::runtime::operators::operator::{
    operator_config_requires_checkpoint, Output, SourceOperator,
};

use super::checkpoint::on_checkpoint_barrier;
use super::ctx::TaskCtx;
use super::mailbox::sleep_until_deadline;
use super::task::StreamTask;
use super::watermark::WatermarkManager;

pub(super) async fn source_loop(
    source: &mut dyn SourceOperator,
    mut ctx: TaskCtx<'_>,
    checkpoint_receiver: &mut mpsc::UnboundedReceiver<u64>,
    source_watermark_manager: &mut WatermarkManager,
) -> Result<()> {
    let mut metrics_window_start = std::time::Instant::now();
    let checkpointable = operator_config_requires_checkpoint(source.operator_config());

    while ctx.status.load(std::sync::atomic::Ordering::SeqCst) == StreamTaskStatus::Running as u8 {
        let deadline = if source_watermark_manager.assigner_enabled() {
            source_watermark_manager.next_emit_deadline()
        } else {
            None
        };

        tokio::select! {
            biased;
            checkpoint_id = checkpoint_receiver.recv(), if checkpointable => {
                let Some(checkpoint_id) = checkpoint_id else {
                    continue;
                };
                inject_source_barrier(
                    source,
                    &mut ctx,
                    source_watermark_manager,
                    checkpoint_id,
                )
                .await?;
            }
            res = source.fetch_next() => {
                match res {
                    FetchResult::Interrupted => {
                        if source.is_stopped() {
                            continue;
                        }
                        if checkpointable {
                            if let Some(checkpoint_id) = checkpoint_receiver.recv().await {
                                inject_source_barrier(
                                    source,
                                    &mut ctx,
                                    source_watermark_manager,
                                    checkpoint_id,
                                )
                                .await?;
                            }
                        }
                    }
                    FetchResult::Data(mut message) => {
                        if matches!(message, Message::Watermark(_)) {
                            panic!(
                                "source fetch_next must not return watermarks; assign them in the task"
                            );
                        }
                        let injected_wm = if matches!(message, Message::Regular(_)) {
                            source_watermark_manager
                                .on_data_message(ctx.vertex_id.as_ref(), &message)
                        } else {
                            None
                        };
                        TaskCtx::stamp_ingest_if_missing(&mut message);
                        {
                            let mut out = ctx.output();
                            out.emit(message).await?;
                        }
                        if let Some(wm) = injected_wm {
                            ctx.emit_watermarks(vec![wm], true, None).await?;
                        }
                    }
                    FetchResult::Idle => {
                        ctx.emit_watermarks(
                            source_watermark_manager.flush_pending(),
                            true,
                            None,
                        )
                        .await?;
                        let max_wm = WatermarkMessage::new(
                            ctx.vertex_id.as_ref().to_string(),
                            MAX_WATERMARK_VALUE,
                            Some(StreamTask::now_ms()),
                        );
                        ctx.mark_finished();
                        ctx.emit_watermarks(vec![max_wm], true, None).await?;
                        break;
                    }
                }
            }
            _ = sleep_until_deadline(deadline) => {
                ctx.emit_watermarks(
                    source_watermark_manager.try_emit_due(),
                    true,
                    None,
                )
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

async fn inject_source_barrier(
    source: &mut dyn SourceOperator,
    ctx: &mut TaskCtx<'_>,
    source_watermark_manager: &mut WatermarkManager,
    checkpoint_id: u64,
) -> Result<()> {
    println!(
        "[CHECKPOINT] {} received trigger for checkpoint_id={}",
        ctx.vertex_id, checkpoint_id
    );
    ctx.emit_watermarks(source_watermark_manager.flush_pending(), true, None)
        .await?;
    let barrier = CheckpointBarrierMessage::new(
        ctx.vertex_id.as_ref().to_string(),
        checkpoint_id,
        ctx.execution_attempt_id,
        Some(StreamTask::now_ms()),
    );
    on_checkpoint_barrier(source, ctx, &barrier, true, None).await?;
    let mut barrier_msg = Message::CheckpointBarrier(barrier);
    TaskCtx::stamp_ingest_if_missing(&mut barrier_msg);
    let mut out = ctx.output();
    out.emit(barrier_msg).await
}
