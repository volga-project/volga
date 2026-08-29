use crate::{
    common::Message,
    runtime::{
        operators::operator::{
            OperatorBase, OperatorConfig, OperatorTrait, OperatorType, Output, StreamOperator,
        },
        runtime_context::RuntimeContext,
    },
};
use anyhow::Result;
use async_trait::async_trait;

#[derive(Debug)]
pub struct JoinOperator {
    base: OperatorBase,
}

impl JoinOperator {
    pub fn new(config: OperatorConfig) -> Self {
        let join_function = match config.clone() {
            OperatorConfig::JoinConfig(join_function) => join_function,
            _ => panic!("Expected JoinConfig, got {:?}", config),
        };
        Self {
            base: OperatorBase::new_with_function(join_function, config),
        }
    }
}

#[async_trait]
impl OperatorTrait for JoinOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    fn operator_config(&self) -> &OperatorConfig {
        self.base.operator_config()
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }
}

#[async_trait]
impl StreamOperator for JoinOperator {
    async fn process_data(&mut self, data: Vec<Message>, out: &mut dyn Output) -> Result<()> {
        for message in data {
            out.emit(message).await?;
        }
        Ok(())
    }
}
