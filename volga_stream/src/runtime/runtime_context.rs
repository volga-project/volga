use std::{collections::HashMap, sync::Arc};
use tokio::sync::{mpsc, Mutex};
use serde_json::Value;

use crate::{common::Message, runtime::functions::source::request_source::PendingRequest};

#[derive(Clone, Debug)]
pub struct RuntimeContext {
    vertex_id: String,
    task_index: i32,
    parallelism: i32,
    job_config: HashMap<String, Value>,

    // if we have request source+sink configured:
    // shared receiver for source tasks to receive requests from RequestSourceProcessor
    request_sink_source_request_receiver: Option<Arc<Mutex<mpsc::Receiver<PendingRequest>>>>,

    // sender to send request responses from sink tasks back to RequestSourceProcessor
    request_sink_source_response_sender: Option<mpsc::Sender<Message>>
}

impl RuntimeContext {
    pub fn new(
        vertex_id: String,
        task_index: i32,
        parallelism: i32,
        job_config: Option<HashMap<String, Value>>,
    ) -> Self {
        Self {
            vertex_id,
            task_index,
            parallelism,
            job_config: job_config.unwrap_or_default(),
            request_sink_source_request_receiver: None,
            request_sink_source_response_sender: None
        }
    }

    pub fn vertex_id(&self) -> &str { &self.vertex_id }
    pub fn task_index(&self) -> i32 { self.task_index }
    pub fn parallelism(&self) -> i32 { self.parallelism }
    pub fn job_config(&self) -> &HashMap<String, Value> { &self.job_config }

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
