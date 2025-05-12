use std::sync::Arc;
use async_trait::async_trait;
use crate::common::data_batch::{DataBatch, KeyedDataBatch, BaseDataBatch};
use anyhow::Result;
use std::fmt;

#[async_trait]
pub trait KeyByFunctionTrait: Send + Sync + fmt::Debug {
    async fn key_by(&self, batch: DataBatch) -> Result<Vec<KeyedDataBatch>>;
}

#[derive(Debug, Clone)]
pub enum KeyByFunction {
    CustomKeyByFunction(Arc<dyn KeyByFunctionTrait>),
}

impl KeyByFunction {
    pub fn new_custom<F>(function: F) -> Self 
    where
        F: KeyByFunctionTrait + 'static,
    {
        Self::CustomKeyByFunction(Arc::new(function))
    }

    pub async fn key_by(&self, batch: DataBatch) -> Result<Vec<KeyedDataBatch>> {
        match self {
            KeyByFunction::CustomKeyByFunction(function) => function.key_by(batch).await,
        }
    }
} 