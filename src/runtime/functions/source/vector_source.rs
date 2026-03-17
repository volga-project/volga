use async_trait::async_trait;
use anyhow::Result;
use crate::common::message::Message;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use std::any::Any;
use super::source_function::SourceFunctionTrait;

#[derive(Debug)]
pub struct VectorSourceFunction {
    messages: Vec<Message>,
    next_index: usize,
}

impl VectorSourceFunction {
    pub fn new(messages: Vec<Message>) -> Self {
        Self {
            messages,
            next_index: 0,
        }
    }
}

#[async_trait]
impl FunctionTrait for VectorSourceFunction {
    async fn open(&mut self, _context: &RuntimeContext) -> Result<()> {
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

#[async_trait]
impl SourceFunctionTrait for VectorSourceFunction {
    async fn fetch(&mut self) -> Option<Message> {
        if self.next_index >= self.messages.len() {
            return None;
        }
        let msg = self.messages[self.next_index].clone();
        self.next_index += 1;
        Some(msg)
    }
} 