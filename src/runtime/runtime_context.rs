use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, Mutex};
use serde_json::Value;

use crate::runtime::execution_graph::ExecutionGraph;
use crate::{common::Message, runtime::functions::source::request_source::PendingRequest};
use crate::runtime::state::OperatorStates;
use crate::storage::WorkerStorageContext;
use crate::runtime::VertexId;

#[derive(Clone, Debug)]
pub struct RuntimeContext {
    vertex_id: VertexId,
    task_index: i32,
    parallelism: i32,
    job_config: HashMap<String, Value>,
    operator_states: Option<Arc<OperatorStates>>,
    execution_graph: Option<ExecutionGraph>,
    worker_storage: Option<Arc<WorkerStorageContext>>,

    // if we have request source+sink configured:
    // shared receiver for source tasks to receive requests from RequestSourceProcessor
    request_sink_source_request_receiver: Option<Arc<Mutex<mpsc::Receiver<PendingRequest>>>>,

    // sender to send request responses from sink tasks back to RequestSourceProcessor
    request_sink_source_response_sender: Option<mpsc::Sender<Message>>
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
            worker_storage: None,
            request_sink_source_request_receiver: None,
            request_sink_source_response_sender: None
        }
    }

    pub fn vertex_id(&self) -> &str { self.vertex_id.as_ref() }
    pub fn vertex_id_arc(&self) -> VertexId { self.vertex_id.clone() }
    pub fn task_index(&self) -> i32 { self.task_index }
    pub fn parallelism(&self) -> i32 { self.parallelism }
    pub fn job_config(&self) -> &HashMap<String, Value> { &self.job_config }
    pub fn operator_states(&self) -> &Arc<OperatorStates> { self.operator_states.as_ref().expect("operator states should be set") }
    pub fn execution_graph(&self) -> &ExecutionGraph { self.execution_graph.as_ref().expect("execution graph should be set") }

    pub fn set_worker_storage_context(&mut self, storage: Arc<WorkerStorageContext>) {
        self.worker_storage = Some(storage);
    }

    pub fn worker_storage_context(&self) -> Option<Arc<WorkerStorageContext>> {
        self.worker_storage.clone()
    }

    pub fn set_request_sink_source_request_receiver(&mut self, receiver: Arc<Mutex<mpsc::Receiver<PendingRequest>>>) {
        self.request_sink_source_request_receiver = Some(receiver)
    }

    pub fn get_request_sink_source_request_receiver(&self) -> Option<Arc<Mutex<mpsc::Receiver<PendingRequest>>>> {
        self.request_sink_source_request_receiver.clone()
    }

    pub fn set_request_sink_source_response_sender(&mut self, sender: mpsc::Sender<Message>) {
        self.request_sink_source_response_sender = Some(sender)
    }

    pub fn get_request_sink_source_response_sender(&self) -> Option<mpsc::Sender<Message>> {
        self.request_sink_source_response_sender.clone()
    }
}
