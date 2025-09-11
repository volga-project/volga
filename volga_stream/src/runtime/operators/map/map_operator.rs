use std::sync::Arc;

use crate::{common::Message, runtime::{functions::map::MapFunction, operators::operator::{OperatorBase, OperatorConfig, OperatorTrait, OperatorType}, runtime_context::RuntimeContext}, storage::storage::Storage};
use anyhow::Result;
use async_trait::async_trait;
use tokio_rayon::AsyncThreadPool;

#[derive(Debug)]
pub struct MapOperator {
    base: OperatorBase,
}

impl MapOperator {
    pub fn new(config: OperatorConfig, storage: Arc<Storage>) -> Self {
        let map_function = match config.clone() {
            OperatorConfig::MapConfig(map_function) => map_function,
            _ => panic!("Expected MapConfig, got {:?}", config),
        };
        Self { 
            base: OperatorBase::new_with_function(map_function, config, storage),
        }
    }
}

#[async_trait]
impl OperatorTrait for MapOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn process_message(&mut self, message: Message) -> Option<Vec<Message>> {
        let function = self.base.get_function_mut::<MapFunction>().unwrap();
        let function = function.clone();
        let message = message.clone();

        // TODO this slow perf - remove?
        // make sure we use fifo to maintain order
        self.base.thread_pool.spawn_fifo_async(move || {
            let processed = function.map(message).unwrap();
            Some(vec![processed])
        }).await
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }
}