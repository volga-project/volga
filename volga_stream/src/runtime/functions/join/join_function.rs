use core::fmt;
use std::any::Any;

use crate::{common::Message, runtime::{functions::function_trait::FunctionTrait, runtime_context::RuntimeContext}};
use async_trait::async_trait;
use anyhow::Result;


#[async_trait]
pub trait JoinFunctionTrait: Send + Sync + fmt::Debug {
    fn join(&self, message: Message) -> Result<Message>;
}

#[derive(Debug, Clone)]
pub enum JoinFunction {
    // TODO implement variants
}

impl JoinFunctionTrait for JoinFunction {

    fn join(&self, message: Message) -> Result<Message> {
        panic!("Not implemented")
    }
}

#[async_trait]
impl FunctionTrait for JoinFunction {
    async fn open(&mut self, _context: &RuntimeContext) -> Result<()> {
        panic!("Not implemented")
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