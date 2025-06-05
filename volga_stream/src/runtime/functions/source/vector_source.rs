use async_trait::async_trait;
use anyhow::Result;
use std::fmt;
use crate::common::message::Message;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use std::any::Any;
use tokio::sync::mpsc::{self, Sender, Receiver};
use tokio::time::{sleep, timeout, Duration};
use super::source_function::SourceFunctionTrait;

#[derive(Debug)]
pub struct VectorSourceFunction {
    channel: Option<Receiver<Message>>,
    sender: Option<Sender<Message>>,
    initial_messages: Vec<Message>,
}

impl VectorSourceFunction {
    pub fn new(messages: Vec<Message>) -> Self {
        Self {
            channel: None,
            sender: None,
            initial_messages: messages,
        }
    }
}

#[async_trait]
impl FunctionTrait for VectorSourceFunction {
    async fn open(&mut self, _context: &RuntimeContext) -> Result<()> {
        let (sender, receiver) = mpsc::channel(100);
        self.sender = Some(sender);
        self.channel = Some(receiver);

        // Move all messages to the channel
        if let Some(sender) = &self.sender {
            for message in self.initial_messages.drain(..) {
                sender.send(message).await?;
            }
        }
        Ok(())
    }
    
    async fn close(&mut self) -> Result<()> {
        // Drop sender to signal end of stream
        self.sender.take();
        self.channel.take();
        Ok(())
    }
    
    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[async_trait]
impl SourceFunctionTrait for VectorSourceFunction {
    async fn fetch(&mut self) -> Result<Option<Message>> {
        if let Some(receiver) = &mut self.channel {
            match timeout(Duration::from_millis(1), receiver.recv()).await {
                Ok(Some(message)) => Ok(Some(message)),
                Ok(None) => panic!("VectorSourceFunction channel closed"),
                Err(_) => Ok(None),   // Timeout
            }
        } else {
            panic!("VectorSourceFunction channel not inited")
        }
    }
} 