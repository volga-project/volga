use async_trait::async_trait;
use anyhow::Result;
use crate::runtime::runtime_context::RuntimeContext;
use std::fmt;
use std::any::Any;

#[async_trait]
pub trait FunctionTrait: Send + Sync + fmt::Debug + Any {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        Ok(())
    }
    
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
    
    fn as_any(&self) -> &dyn Any;
    
    fn as_any_mut(&mut self) -> &mut dyn Any;
} 