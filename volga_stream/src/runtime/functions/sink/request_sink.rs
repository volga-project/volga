use async_trait::async_trait;
use anyhow::Result;
use std::any::Any;
use tokio::sync::mpsc;

use crate::common::message::Message;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use super::sink_function::SinkFunctionTrait;
use crate::runtime::functions::source::request_source::{SOURCE_VERTEX_ID_FIELD, SOURCE_REQUEST_ID_FIELD};

#[derive(Debug, Clone)]
pub struct RequestSinkConfig {
    pub response_sender: mpsc::Sender<Message>,
}

impl RequestSinkConfig {
    pub fn new(response_sender: mpsc::Sender<Message>) -> Self {
        Self {
            response_sender,
        }
    }
}

#[derive(Debug)]
pub struct RequestSinkFunction {
    response_sender: mpsc::Sender<Message>,
    runtime_context: Option<RuntimeContext>,
}

impl RequestSinkFunction {
    pub fn new(response_sender: mpsc::Sender<Message>) -> Self {
        Self {
            response_sender,
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
        if extras.get(SOURCE_VERTEX_ID_FIELD).is_none() {
            return Err(anyhow::anyhow!("Message missing {} in extras", SOURCE_VERTEX_ID_FIELD));
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
        if let Err(e) = self.response_sender.send(message).await {
            return Err(anyhow::anyhow!("Failed to send response: {}", e));
        }
        
        Ok(())
    }
}

#[async_trait]
impl FunctionTrait for RequestSinkFunction {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.runtime_context = Some(context.clone());
        Ok(())
    }
    
    async fn close(&mut self) -> Result<()> {
        // Nothing special to do on close
        Ok(())
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
    
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
