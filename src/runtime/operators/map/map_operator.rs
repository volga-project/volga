use std::fmt;

use crate::{
    common::Message,
    runtime::{
        functions::map::MapFunction,
        operators::operator::{
            OperatorBase, OperatorConfig, OperatorTrait, OperatorType, Output, StreamOperator,
        },
        runtime_context::RuntimeContext,
    },
};
use anyhow::Result;
use async_trait::async_trait;

pub struct MapOperator {
    base: OperatorBase,
}

impl fmt::Debug for MapOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MapOperator")
            .field("base", &self.base)
            .finish()
    }
}

impl MapOperator {
    pub fn new(config: OperatorConfig) -> Self {
        let map_function = match config.clone() {
            OperatorConfig::MapConfig(map_function) => map_function,
            _ => panic!("Expected MapConfig, got {:?}", config),
        };
        Self {
            base: OperatorBase::new_with_function(map_function, config),
        }
    }
}

#[async_trait]
impl OperatorTrait for MapOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    fn operator_config(&self) -> &OperatorConfig {
        self.base.operator_config()
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }
}

#[async_trait]
impl StreamOperator for MapOperator {
    async fn process_data(&mut self, data: Vec<Message>, out: &mut dyn Output) -> Result<()> {
        let function = self.base.get_function_mut::<MapFunction>().unwrap().clone();
        for message in data {
            out.emit(function.map(message)?).await?;
        }
        Ok(())
    }
}
