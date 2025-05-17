use std::sync::Arc;
use async_trait::async_trait;
use crate::common::data_batch::DataBatch;
use anyhow::Result;
use std::fmt;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::function_trait::FunctionTrait;
use std::any::Any;

#[async_trait]
pub trait MapFunctionTrait: Send + Sync + fmt::Debug {
    async fn map(&self, batch: DataBatch) -> Result<DataBatch>;
}

#[derive(Debug, Clone)]
pub enum MapFunction {
    CustomMapFunction(Arc<dyn MapFunctionTrait>),
}

impl MapFunction {
    pub fn new_custom<F>(function: F) -> Self 
    where
        F: MapFunctionTrait + 'static,
    {
        Self::CustomMapFunction(Arc::new(function))
    }

    pub async fn map(&self, batch: DataBatch) -> Result<DataBatch> {
        match self {
            MapFunction::CustomMapFunction(function) => function.map(batch).await,
        }
    }
}

#[async_trait]
impl FunctionTrait for MapFunction {
    async fn open(&mut self, _context: &RuntimeContext) -> Result<()> {
        // Default implementation does nothing
        Ok(())
    }
    
    async fn close(&mut self) -> Result<()> {
        // Default implementation does nothing
        Ok(())
    }
    
    async fn finish(&mut self) -> Result<()> {
        // Default implementation does nothing
        Ok(())
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
    
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
} 