use crate::{
    common::Message,
    runtime::{
        functions::map::MapFunction,
        operators::operator::{
            HasOperatorBase, OperatorBase, OperatorConfig, Output, StreamOperator,
        },
    },
};
use anyhow::Result;
use async_trait::async_trait;

#[derive(Debug)]
pub struct MapOperator {
    base: OperatorBase,
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

impl HasOperatorBase for MapOperator {
    fn operator_base(&self) -> &OperatorBase {
        &self.base
    }

    fn operator_base_mut(&mut self) -> &mut OperatorBase {
        &mut self.base
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
