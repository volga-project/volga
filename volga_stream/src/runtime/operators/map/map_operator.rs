use crate::{common::Message, runtime::{functions::map::MapFunction, operators::operator::{OperatorBase, OperatorTrait, OperatorType}, runtime_context::RuntimeContext}};
use anyhow::Result;
use async_trait::async_trait;
use tokio_rayon::AsyncThreadPool;

#[derive(Debug)]
pub struct MapOperator {
    base: OperatorBase,
}

impl MapOperator {
    pub fn new(map_function: MapFunction) -> Self {
        Self { 
            base: OperatorBase::new_with_function(map_function),
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

        // make sure we use fifo to maintain order
        self.base.thread_pool.spawn_fifo_async(move || {
            let processed = function.map(message).unwrap();
            Some(vec![processed])
        }).await
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }
}