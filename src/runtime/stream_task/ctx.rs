use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8};
use std::sync::Arc;

use crate::runtime::collector::Collector;
use crate::runtime::master::server::master_service::master_service_client::MasterServiceClient;
use crate::runtime::metrics::MetricsLabels;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::VertexId;

use super::metrics::TaskTimeMetrics;
use super::output::TransportOutput;

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
    pub fn output(&mut self, stamp_ingest_if_missing: bool) -> TransportOutput<'_> {
        TransportOutput {
            collectors: self.collectors,
            vertex_id: self.vertex_id.clone(),
            labels: self.metrics_labels,
            stamp_ingest_if_missing,
            status: self.status.clone(),
        }
    }
}
