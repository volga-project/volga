use std::{sync::Arc, fmt};

use crate::{common::Message, runtime::{functions::map::MapFunction, operators::operator::{MessageStream, OperatorBase, OperatorConfig, OperatorPollResult, OperatorTrait, OperatorType}, runtime_context::RuntimeContext}, storage::storage::Storage};
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;

pub struct MapOperator {
    base: OperatorBase,
    // input_stream: Option<MessageStream>,
}

impl fmt::Debug for MapOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MapOperator")
            .field("base", &self.base)
            // .field("input_stream", &"<MessageStream>")
            .finish()
    }
}

impl MapOperator {
    pub fn new(config: OperatorConfig, storage: Arc<Storage>) -> Self {
        let map_function = match config.clone() {
            OperatorConfig::MapConfig(map_function) => map_function,
            _ => panic!("Expected MapConfig, got {:?}", config),
        };
        Self { 
            base: OperatorBase::new_with_function(map_function, config, storage),
            // input_stream: None,
        }
    }
}

#[async_trait]
impl OperatorTrait for MapOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    fn set_input(&mut self, input: Option<MessageStream>) {
        self.base.set_input(input);
    }

    async fn poll_next(&mut self) -> OperatorPollResult {
        let input_stream = self.base.input.as_mut().expect("input stream not set");
        match input_stream.next().await {
            Some(Message::Watermark(watermark)) => {
                return OperatorPollResult::Ready(Message::Watermark(watermark));
            }
            Some(message) => {
                let function = self.base.get_function_mut::<MapFunction>().unwrap();
                let function = function.clone();

                // TODO this using thread pool is slow perf - benchmark?
                // make sure we use fifo to maintain order
                // self.base.thread_pool.spawn_fifo_async(move || {
                //     let processed = function.map(message).unwrap();
                //     Some(processed)
                // }).await
                let processed = function.map(message).unwrap();
                OperatorPollResult::Ready(processed)
            }
            None => OperatorPollResult::None,
        }
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }
}
