use std::fmt;

use anyhow::Result;
use async_trait::async_trait;

use crate::{
    common::Message,
    runtime::{
        functions::key_by::KeyByFunction,
        operators::operator::{
            OperatorBase, OperatorConfig, OperatorTrait, OperatorType, Output, StreamOperator,
        },
        runtime_context::RuntimeContext,
    },
};

pub struct KeyByOperator {
    base: OperatorBase,
    parallelism: usize,
    max_parallelism: usize,
}

impl fmt::Debug for KeyByOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyByOperator")
            .field("base", &self.base)
            .field("parallelism", &self.parallelism)
            .field("max_parallelism", &self.max_parallelism)
            .finish()
    }
}

impl KeyByOperator {
    pub fn new(config: OperatorConfig) -> Self {
        let key_by_function = match config.clone() {
            OperatorConfig::KeyByConfig(key_by_function) => key_by_function,
            _ => panic!("Expected KeyByConfig, got {:?}", config),
        };
        Self {
            base: OperatorBase::new_with_function(key_by_function, config),
            parallelism: 1,
            max_parallelism: 1,
        }
    }
}

#[async_trait]
impl OperatorTrait for KeyByOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        // dest = key_group → subtask. Planner assigns the same p to KeyBy and
        // its Hash downstream, so dest < collector.output_channels.len() ([#263](https://github.com/volga-project/volga/issues/263)).
        // Uneven rescale is [#121](https://github.com/volga-project/volga/issues/121).
        self.parallelism = context.parallelism().max(1) as usize;
        self.max_parallelism = context.max_parallelism();
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
impl StreamOperator for KeyByOperator {
    async fn process_data(&mut self, data: Vec<Message>, out: &mut dyn Output) -> Result<()> {
        let function = self.base.get_function_mut::<KeyByFunction>().unwrap().clone();
        for message in data {
            let messages = function.key_by(message, self.parallelism, self.max_parallelism);
            if messages.is_empty() {
                panic!("KeyBy operator produced no messages");
            }
            for keyed in messages {
                out.emit(keyed).await?;
            }
        }
        Ok(())
    }
}
