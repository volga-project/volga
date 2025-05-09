use std::sync::Arc;
use async_trait::async_trait;
use crate::common::data_batch::DataBatch;
use anyhow::Result;
use std::fmt;
use dyn_clone::DynClone;

#[async_trait]
pub trait MapFunctionTrait: Send + Sync + fmt::Debug + DynClone {
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