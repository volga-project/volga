use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, Mutex};
use serde_json::Value;

use crate::api::spec::state::OperatorStateBackendConfig;
use crate::common::types::PipelineId;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::{common::Message, runtime::functions::source::request_source::PendingRequest};
use crate::runtime::operators::source::SourceHandles;
use crate::runtime::state::OperatorStates;
use crate::runtime::VertexId;

#[derive(Clone, Debug)]
pub struct RuntimeContext {
    vertex_id: VertexId,
    task_index: i32,
    parallelism: i32,
    job_config: HashMap<String, Value>,
    operator_states: Option<Arc<OperatorStates>>,
    execution_graph: Option<ExecutionGraph>,
    state_backend: Option<OperatorStateBackendConfig>,
    pipeline_id: Option<PipelineId>,
    operator_id: Option<String>,
    source_handles: Option<SourceHandles>,

    request_sink_source_request_receiver: Option<Arc<Mutex<mpsc::Receiver<PendingRequest>>>>,
    request_sink_source_response_sender: Option<mpsc::Sender<Message>>,
}

impl RuntimeContext {
    pub fn new(
        vertex_id: VertexId,
        task_index: i32,
        parallelism: i32,
        job_config: Option<HashMap<String, Value>>,
        operator_states: Option<Arc<OperatorStates>>,
        execution_graph: Option<ExecutionGraph>,
    ) -> Self {
        Self {
            vertex_id,
            task_index,
            parallelism,
            job_config: job_config.unwrap_or_default(),
            operator_states,
            execution_graph,
            state_backend: None,
            pipeline_id: None,
            operator_id: None,
            source_handles: None,
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
    pub fn job_config(&self) -> &HashMap<String, Value> {
        &self.job_config
    }
    pub fn operator_states(&self) -> &Arc<OperatorStates> {
        self.operator_states
            .as_ref()
            .expect("operator states should be set")
    }
    pub fn execution_graph(&self) -> &ExecutionGraph {
        self.execution_graph
            .as_ref()
            .expect("execution graph should be set")
    }

    pub fn with_state_backend(
        mut self,
        state_backend: OperatorStateBackendConfig,
        pipeline_id: PipelineId,
        operator_id: String,
    ) -> Self {
        self.state_backend = Some(state_backend);
        self.pipeline_id = Some(pipeline_id);
        self.operator_id = Some(operator_id);
        self
    }

    pub fn state_backend(&self) -> Option<&OperatorStateBackendConfig> {
        self.state_backend.as_ref()
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
