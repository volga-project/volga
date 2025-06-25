use async_trait::async_trait;
use anyhow::Result;
use tokio_rayon::AsyncThreadPool;

use crate::{common::Message, runtime::{functions::key_by::{KeyByFunction, KeyByFunctionTrait}, operators::operator::{OperatorBase, OperatorTrait, OperatorType}, runtime_context::RuntimeContext}};

#[derive(Debug)]
pub struct KeyByOperator {
    base: OperatorBase,
}

impl KeyByOperator {
    pub fn new(key_by_function: KeyByFunction) -> Self {
        Self { 
            base: OperatorBase::new_with_function(key_by_function),
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
        self.base.thread_pool.spawn_fifo_async(move || {
            let keyed_messages = function.key_by(message);
            // Convert
            let messages = keyed_messages.into_iter()
                .map(Message::Keyed)
                .collect();
            Some(messages)
        }).await
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }
}