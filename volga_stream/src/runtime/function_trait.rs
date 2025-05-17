use async_trait::async_trait;
use anyhow::Result;
use crate::runtime::runtime_context::RuntimeContext;
use std::fmt;
use std::any::Any;

/// Common trait for all functions in the execution graph
/// Provides lifecycle methods that all functions should implement
#[async_trait]
pub trait FunctionTrait: Send + Sync + fmt::Debug + Any {
    /// Initialize the function with runtime context
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        // Default implementation does nothing
        Ok(())
    }
    
    /// Clean up any resources when the function is no longer needed
    async fn close(&mut self) -> Result<()> {
        // Default implementation does nothing
        Ok(())
    }
    
    /// Finalize the function and emit any pending results
    async fn finish(&mut self) -> Result<()> {
        // Default implementation does nothing
        Ok(())
    }
    
    /// Downcasts to a specific function type
    fn as_any(&self) -> &dyn Any;
    
    /// Downcasts to a mutable specific function type
    fn as_any_mut(&mut self) -> &mut dyn Any;
} 