use async_trait::async_trait;
use anyhow::Result;
use std::any::Any;
use tokio::sync::mpsc;

use crate::common::message::Message;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use super::sink_function::SinkFunctionTrait;
use crate::runtime::functions::source::request_source::{SOURCE_TASK_INDEX_FIELD, SOURCE_REQUEST_ID_FIELD};

#[derive(Debug)]
pub struct RequestSinkFunction {
    response_sender: Option<mpsc::Sender<Message>>,
    runtime_context: Option<RuntimeContext>,
}

impl RequestSinkFunction {
    pub fn new() -> Self {
        Self {
            response_sender: None,
            runtime_context: None,
        }
    }
    
    /// Validate that the message contains required metadata fields in extras
    fn validate_message(&self, message: &Message) -> Result<()> {
        // Check for required metadata fields in extras
        let extras = message.get_extras();
        if extras.is_none() {
            return Err(anyhow::anyhow!("Message missing extras"));
        }
        let extras = extras.unwrap();
        if extras.get(SOURCE_TASK_INDEX_FIELD).is_none() {
            return Err(anyhow::anyhow!("Message missing {} in extras", SOURCE_TASK_INDEX_FIELD));
        }
        
        if extras.get(SOURCE_REQUEST_ID_FIELD).is_none() {
            return Err(anyhow::anyhow!("Message missing {} in extras", SOURCE_REQUEST_ID_FIELD));
        }
        
        Ok(())
    }
}

#[async_trait]
impl SinkFunctionTrait for RequestSinkFunction {
    async fn sink(&mut self, message: Message) -> Result<()> {
        // Validate the message has required metadata
        self.validate_message(&message)?;
        
        // Forward the message to the response channel
        // This will be received by the corresponding request source
        if let Err(e) = self.response_sender.as_ref().unwrap().send(message).await {
            return Err(anyhow::anyhow!("Failed to send response: {}", e));
        }
        
        Ok(())
    }
}

#[async_trait]
impl FunctionTrait for RequestSinkFunction {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.runtime_context = Some(context.clone());

        let response_sender = context.get_request_sink_source_response_sender().expect("request_sink_source_response_sender should be set");
        
        if self.response_sender.is_some() {
            panic!("response_sender is already set")
        }
        
        self.response_sender = Some(response_sender);

        Ok(())
    }
    
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
    
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
