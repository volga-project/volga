use async_trait::async_trait;
use anyhow::Result;
use tokio_rayon::AsyncThreadPool;

use crate::{common::Message, runtime::{functions::key_by::{KeyByFunction, KeyByFunctionTrait}, operators::operator::{OperatorBase, OperatorTrait, OperatorType, OperatorConfig}, runtime_context::RuntimeContext}};

#[derive(Debug)]
pub struct KeyByOperator {
    base: OperatorBase,
}

impl KeyByOperator {
    pub fn new(config: OperatorConfig) -> Self {
        let key_by_function = match config.clone() {
            OperatorConfig::KeyByConfig(key_by_function) => key_by_function,
            _ => panic!("Expected KeyByConfig, got {:?}", config),
        };
        Self { 
            base: OperatorBase::new_with_function(key_by_function, config),
        }
    }
}

#[async_trait]
impl OperatorTrait for KeyByOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn process_message(&mut self, message: Message) -> Option<Vec<Message>> {
        let function = self.base.get_function_mut::<KeyByFunction>().unwrap();
        let function = function.clone();
        let message = message.clone();
        
        // make sure we use fifo to maintain order
        // self.base.thread_pool.spawn_fifo_async(move || {
        //     let keyed_messages = function.key_by(message);
        //     // Convert
        //     let messages = keyed_messages.into_iter()
        //         .map(Message::Keyed)
        //         .collect();
        //     Some(messages)
        // }).await
        let keyed_messages = function.key_by(message);
        // Convert
        let messages = keyed_messages.into_iter()
            .map(Message::Keyed)
            .collect();
        Some(messages)
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }
}