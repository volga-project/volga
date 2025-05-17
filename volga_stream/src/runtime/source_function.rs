use async_trait::async_trait;
use anyhow::Result;
use std::fmt;
use crate::common::data_batch::DataBatch;
use crate::runtime::execution_graph::SourceConfig;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::function_trait::FunctionTrait;
use std::any::Any;

#[async_trait]
pub trait SourceFunctionTrait: Send + Sync + fmt::Debug {
    async fn fetch(&mut self) -> Result<Option<DataBatch>>;
}

#[derive(Debug)]
pub enum SourceFunction {
    Vector(VectorSourceFunction),
}

#[async_trait]
impl SourceFunctionTrait for SourceFunction {
    async fn fetch(&mut self) -> Result<Option<DataBatch>> {
        match self {
            SourceFunction::Vector(f) => f.fetch().await,
        }
    }
}

#[async_trait]
impl FunctionTrait for SourceFunction {
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

#[derive(Debug)]
pub struct VectorSourceFunction {
    batches: Vec<DataBatch>,
    current_index: usize,
}

impl VectorSourceFunction {
    pub fn new(batches: Vec<DataBatch>) -> Self {
        Self {
            batches,
            current_index: 0,
        }
    }
}

#[async_trait]
impl SourceFunctionTrait for VectorSourceFunction {
    async fn fetch(&mut self) -> Result<Option<DataBatch>> {
        if self.current_index < self.batches.len() {
            let batch = self.batches[self.current_index].clone();
            self.current_index += 1;
            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }
}

pub fn create_source_function(config: SourceConfig) -> SourceFunction {
    match config {
        SourceConfig::VectorSourceConfig(batches) => {
            SourceFunction::Vector(VectorSourceFunction::new(batches))
        }
    }
} 