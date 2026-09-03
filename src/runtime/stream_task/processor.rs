use std::time::Instant;

use anyhow::Result;
use futures::StreamExt;

use crate::common::message::Message;
use crate::runtime::consts::{runtime_consts, WINDOW_INGEST_MAX_RECORDS};
use crate::runtime::operators::operator::{drain_ready_after, MessageStream, StreamOperator};
use crate::transport::transport_client::DataReaderControl;

use super::checkpoint::on_checkpoint_barrier;
use super::ctx::TaskCtx;
use super::progress::{sleep_until_deadline, InputProgress};
use super::task::timestamp;

pub(super) async fn processor_loop(
    operator: &mut dyn StreamOperator,
    mut ctx: TaskCtx<'_>,
    mailbox: MessageStream,
    mut progress: InputProgress,
    reader_control: DataReaderControl,
) -> Result<()> {
    let mut mailbox = mailbox.peekable();
    let mut metrics_window_start = Instant::now();

    while ctx.is_running() {
        let idle_start = Instant::now();
        tokio::select! {
            biased;
            message = mailbox.next() => {
                ctx.add_idle(idle_start);
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
                        ctx.record_control(&msg);
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
                        ctx.record_control(&msg);
                        let Message::CheckpointBarrier(barrier) = msg else {
                            unreachable!()
                        };
                        match progress.on_aligned_barrier(&barrier).await {
                            Ok(None) => {}
                            Ok(Some((checkpoint_id, inject_stamp, wms))) => {
                                ctx.emit_watermarks(wms, false, Some(operator)).await?;
                                let aligned = ctx.new_barrier(checkpoint_id, inject_stamp);
                                // handle_barrier first: sink flush must finish before Aligned
                                // (sink has no state ack; kill-after-complete can drop a buffered batch).
                                {
                                    let mut out = ctx.output();
                                    operator.handle_barrier(aligned.clone(), &mut out).await?;
                                }
                                on_checkpoint_barrier(
                                    operator,
                                    &mut ctx,
                                    &aligned,
                                    false,
                                    Some(&reader_control),
                                )
                                .await?;
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
                        ctx.record_recv(&first);
                        let mut assigned = Vec::new();
                        if let Some(wm) = progress.assign_and_merge(&first).await {
                            assigned.push(wm);
                        }
                        let batch =
                            drain_ready_after(
                                first,
                                &mut mailbox,
                                runtime_consts().u64(WINDOW_INGEST_MAX_RECORDS).max(1) as usize,
                            )
                            .await;
                        for extra in &batch[1..] {
                            ctx.record_recv(extra);
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
                ctx.add_idle(idle_start);
                ctx.emit_watermarks(progress.on_timer().await, false, Some(operator))
                    .await?;
            }
        }

        ctx.report_time_metrics(&mut metrics_window_start);
    }
    Ok(())
}
