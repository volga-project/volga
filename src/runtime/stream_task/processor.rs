use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;

use crate::common::message::Message;
use crate::runtime::collector::Collector;
use crate::runtime::master::server::master_service::master_service_client::MasterServiceClient;
use crate::runtime::metrics::MetricsLabels;
use crate::runtime::observability::StreamTaskStatus;
use crate::runtime::operators::operator::{
    drain_ready_inputs, NextInputs, Operator, StreamOperator, INGEST_MAX_RECORDS,
};
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::VertexId;
use crate::transport::transport_client::DataReaderControl;

use super::checkpoint::on_checkpoint_barrier;
use super::metrics::TaskTimeMetrics;
use super::output::TransportOutput;
use super::task::{timestamp, StreamTask};

pub(super) struct ProcessorLoopParams<'a> {
    pub vertex_id: VertexId,
    pub runtime_context: &'a RuntimeContext,
    pub status: Arc<AtomicU8>,
    pub execution_attempt_id: u64,
    pub collectors: &'a mut HashMap<String, Collector>,
    pub master_client: &'a mut Option<MasterServiceClient<tonic::transport::Channel>>,
    pub metrics_labels: Option<&'a MetricsLabels>,
    pub task_time_metrics: Arc<TaskTimeMetrics>,
    pub current_watermark: Arc<AtomicU64>,
    pub input: crate::runtime::operators::operator::MessageStream,
    pub reader_control: DataReaderControl,
}

pub(super) async fn processor_loop(
    operator: &mut Operator,
    params: ProcessorLoopParams<'_>,
) -> Result<()> {
    let ProcessorLoopParams {
        vertex_id,
        runtime_context,
        status,
        execution_attempt_id,
        collectors,
        master_client,
        metrics_labels,
        task_time_metrics,
        current_watermark,
        input,
        reader_control,
    } = params;

    let mut input = {
        use futures::StreamExt;
        input.peekable()
    };
    let mut metrics_window_start = Instant::now();

    while status.load(Ordering::SeqCst) == StreamTaskStatus::Running as u8 {
        match drain_ready_inputs(&mut input, INGEST_MAX_RECORDS).await {
            NextInputs::Exhausted => {
                println!(
                    "{:?} StreamTask {}: upstream ended without terminal watermark; finishing",
                    timestamp(),
                    vertex_id.as_ref()
                );
                status.store(StreamTaskStatus::Finished as u8, Ordering::SeqCst);
                break;
            }
            NextInputs::Data(data) => {
                let mut out = TransportOutput {
                    collectors,
                    vertex_id: vertex_id.clone(),
                    labels: metrics_labels,
                    stamp_ingest_if_missing: false,
                    status: status.clone(),
                };
                operator.process_data(data, &mut out).await?;
            }
            NextInputs::Control(Message::Watermark(wm)) => {
                let mut out = TransportOutput {
                    collectors,
                    vertex_id: vertex_id.clone(),
                    labels: metrics_labels,
                    stamp_ingest_if_missing: false,
                    status: status.clone(),
                };
                operator.handle_watermark(wm, &mut out).await?;
            }
            NextInputs::Control(Message::CheckpointBarrier(barrier)) => {
                on_checkpoint_barrier(
                    operator,
                    master_client,
                    &barrier,
                    false,
                    &vertex_id,
                    runtime_context.task_index(),
                    execution_attempt_id,
                    metrics_labels,
                    Some(&reader_control),
                )
                .await?;
                let mut out = TransportOutput {
                    collectors,
                    vertex_id: vertex_id.clone(),
                    labels: metrics_labels,
                    stamp_ingest_if_missing: false,
                    status: status.clone(),
                };
                operator.handle_barrier(barrier, &mut out).await?;
            }
            NextInputs::Control(other) => {
                panic!("unexpected control message in processor loop: {other:?}");
            }
        }
        StreamTask::report_task_time_metrics(
            &task_time_metrics,
            &mut metrics_window_start,
            &vertex_id,
            metrics_labels,
            &current_watermark,
        );
    }
    Ok(())
}
