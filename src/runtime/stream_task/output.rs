use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::common::message::Message;
use crate::common::MAX_WATERMARK_VALUE;
use crate::runtime::collector::Collector;
use crate::runtime::metrics::MetricsLabels;
use crate::runtime::observability::StreamTaskStatus;
use crate::runtime::operators::operator::Output;
use crate::runtime::VertexId;

use super::task::{StreamTask, MESSAGE_TRACE_ENABLED};

pub(super) struct TransportOutput<'a> {
    pub collectors: &'a mut HashMap<String, Collector>,
    pub vertex_id: VertexId,
    pub labels: Option<&'a MetricsLabels>,
    pub stamp_ingest_if_missing: bool,
    pub status: Arc<AtomicU8>,
}

#[async_trait]
impl Output for TransportOutput<'_> {
    async fn emit(&mut self, mut message: Message) -> Result<()> {
        if self.stamp_ingest_if_missing && message.ingest_timestamp().is_none() {
            message.set_ingest_timestamp(StreamTask::now_ms());
        }
        if let Message::Watermark(ref watermark) = message {
            if watermark.watermark_value == MAX_WATERMARK_VALUE {
                self.status
                    .store(StreamTaskStatus::Finished as u8, Ordering::SeqCst);
            }
            StreamTask::set_watermark_lag_gauge(
                &self.vertex_id,
                watermark.watermark_value,
                self.labels,
            );
        }
        if MESSAGE_TRACE_ENABLED.load(Ordering::Relaxed) {
            message.append_trace(&self.vertex_id);
        }
        message.set_upstream_vertex_id(self.vertex_id.as_ref().to_string());
        StreamTask::record_metrics(self.vertex_id.clone(), &message, false, self.labels);
        StreamTask::send_to_collectors_if_needed(self.collectors, message).await;
        Ok(())
    }
}

impl StreamTask {
    pub(super) async fn send_to_collectors_if_needed(
        collectors_per_target_operator: &mut HashMap<String, Collector>,
        message: Message,
    ) {
        if collectors_per_target_operator.is_empty() {
            return;
        }

        let mut channels_to_send_per_operator = HashMap::new();
        for (target_operator_id, collector) in collectors_per_target_operator.iter_mut() {
            let partitioned_channels = collector.gen_partitioned_channels(&message);
            channels_to_send_per_operator.insert(target_operator_id.clone(), partitioned_channels);
        }

        let _ = Collector::write_message_to_operators(
            collectors_per_target_operator,
            &message,
            channels_to_send_per_operator,
        )
        .await;
    }
}
