use crate::{
    common::Message,
    runtime::{
        operators::operator::{
            HasOperatorBase, OperatorBase, OperatorConfig, Output, StreamOperator,
        },
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

impl HasOperatorBase for JoinOperator {
    fn operator_base(&self) -> &OperatorBase {
        &self.base
    }

    fn operator_base_mut(&mut self) -> &mut OperatorBase {
        &mut self.base
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
