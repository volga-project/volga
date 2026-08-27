use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, Mutex};
use serde_json::Value;

use crate::api::spec::state::{OperatorStateBackendConfig, RequestStoreConfig};
use crate::common::types::PipelineId;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::metrics::MetricsLabels;
use crate::{common::Message, runtime::functions::source::request_source::PendingRequest};
use crate::runtime::operators::source::SourceHandles;
use crate::runtime::observability::TaskMetadata;
use crate::runtime::state::StateRegistry;
use crate::runtime::VertexId;

#[derive(Clone, Debug)]
pub struct RuntimeContext {
    vertex_id: VertexId,
    task_index: i32,
    parallelism: i32,
    max_parallelism: i32,
    job_config: HashMap<String, Value>,
    state_registry: Option<Arc<StateRegistry>>,
    execution_graph: Option<ExecutionGraph>,
    operator_state_backend: Option<OperatorStateBackendConfig>,
    request_store: Option<RequestStoreConfig>,
    pipeline_id: Option<PipelineId>,
    operator_id: Option<String>,
    source_handles: Option<SourceHandles>,
    task_metadata: TaskMetadata,

    request_sink_source_request_receiver: Option<Arc<Mutex<mpsc::Receiver<PendingRequest>>>>,
    request_sink_source_response_sender: Option<mpsc::Sender<Message>>,
}

impl RuntimeContext {
    pub fn new(
        vertex_id: VertexId,
        task_index: i32,
        parallelism: i32,
        job_config: Option<HashMap<String, Value>>,
        state_registry: Option<Arc<StateRegistry>>,
        execution_graph: Option<ExecutionGraph>,
    ) -> Self {
        Self {
            vertex_id,
            task_index,
            parallelism,
            max_parallelism: parallelism,
            job_config: job_config.unwrap_or_default(),
            state_registry,
            execution_graph,
            operator_state_backend: None,
            request_store: None,
            pipeline_id: None,
            operator_id: None,
            source_handles: None,
            task_metadata: TaskMetadata::default(),
            request_sink_source_request_receiver: None,
            request_sink_source_response_sender: None,
        }
    }

    pub fn vertex_id(&self) -> &str {
        self.vertex_id.as_ref()
    }
    pub fn vertex_id_arc(&self) -> VertexId {
        self.vertex_id.clone()
    }
    pub fn task_index(&self) -> i32 {
        self.task_index
    }
    pub fn parallelism(&self) -> i32 {
        self.parallelism
    }
    pub fn max_parallelism(&self) -> i32 {
        self.max_parallelism.max(self.parallelism).max(1)
    }
    pub fn with_max_parallelism(mut self, max_parallelism: i32) -> Self {
        self.max_parallelism = max_parallelism.max(self.parallelism).max(1);
        self
    }
    pub fn job_config(&self) -> &HashMap<String, Value> {
        &self.job_config
    }

    /// Worker id from job config (`worker_id` string), when present.
    pub fn worker_id(&self) -> Option<&str> {
        self.job_config
            .get("worker_id")
            .and_then(|v| v.as_str())
    }

    /// Pipeline + worker labels for Prom, when both are configured.
    pub fn metrics_labels(&self) -> Option<MetricsLabels> {
        let pipeline_id = self
            .pipeline_id
            .as_ref()
            .map(|p| p.0.clone())
            .or_else(|| {
                self.job_config
                    .get("pipeline_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })?;
        let worker_id = self.worker_id()?.to_string();
        Some(MetricsLabels {
            pipeline_id,
            worker_id,
        })
    }

    pub fn state_registry(&self) -> Option<&Arc<StateRegistry>> {
        self.state_registry.as_ref()
    }
    pub fn execution_graph(&self) -> &ExecutionGraph {
        self.execution_graph
            .as_ref()
            .expect("execution graph should be set")
    }

    pub fn with_state_config(
        mut self,
        operator_state_backend: OperatorStateBackendConfig,
        request_store: Option<RequestStoreConfig>,
        pipeline_id: PipelineId,
        operator_id: String,
    ) -> Self {
        self.operator_state_backend = Some(operator_state_backend);
        self.request_store = request_store;
        self.pipeline_id = Some(pipeline_id);
        self.operator_id = Some(operator_id);
        self
    }

    pub fn with_state_registry(mut self, registry: Arc<StateRegistry>) -> Self {
        self.state_registry = Some(registry);
        self
    }

    pub fn operator_state_backend(&self) -> Option<&OperatorStateBackendConfig> {
        self.operator_state_backend.as_ref()
    }

    pub fn request_store(&self) -> Option<&RequestStoreConfig> {
        self.request_store.as_ref()
    }

    pub fn pipeline_id(&self) -> Option<&PipelineId> {
        self.pipeline_id.as_ref()
    }

    pub fn operator_id(&self) -> Option<&str> {
        self.operator_id.as_deref()
    }

    pub fn set_source_handles(&mut self, handles: Arc<SourceHandles>) {
        self.source_handles = Some((*handles).clone());
    }

    pub fn source_handles(&self) -> Option<&SourceHandles> {
        self.source_handles.as_ref()
    }

    pub fn task_metadata(&self) -> TaskMetadata {
        self.task_metadata.clone()
    }

    pub fn set_request_sink_source_request_receiver(
        &mut self,
        receiver: Arc<Mutex<mpsc::Receiver<PendingRequest>>>,
    ) {
        self.request_sink_source_request_receiver = Some(receiver)
    }

    pub fn get_request_sink_source_request_receiver(
        &self,
    ) -> Option<Arc<Mutex<mpsc::Receiver<PendingRequest>>>> {
        self.request_sink_source_request_receiver.clone()
    }

    pub fn set_request_sink_source_response_sender(&mut self, sender: mpsc::Sender<Message>) {
        self.request_sink_source_response_sender = Some(sender)
    }

    pub fn get_request_sink_source_response_sender(&self) -> Option<mpsc::Sender<Message>> {
        self.request_sink_source_response_sender.clone()
    }
}
