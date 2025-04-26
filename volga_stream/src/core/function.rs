// Core Function traits
use async_trait::async_trait;
use crate::core::runtime_context::RuntimeContext;
use anyhow::Result;
use serde_json::Value;

use super::source::SourceContext;

#[async_trait]
pub trait Function: Send + Sync {
    async fn open(&mut self, runtime_context: RuntimeContext) -> Result<()> {
        Ok(())
    }
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait MapFunction: Function {
    async fn map(&mut self, value: &Value) -> Result<Value>;
}

#[async_trait]
pub trait FilterFunction: Function {
    async fn filter(&mut self, value: &Value) -> Result<bool>;
}

#[async_trait]
pub trait JoinFunction: Function {
    async fn join(&mut self, left: &Value, right: &Value) -> Result<Value>;
}

#[async_trait]
pub trait SourceFunction: Function {
    async fn fetch(&mut self, ctx: &mut dyn SourceContext) -> Result<()>;
}