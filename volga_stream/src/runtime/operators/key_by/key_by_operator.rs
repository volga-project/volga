use std::{sync::Arc, fmt};

use async_trait::async_trait;
use anyhow::Result;
use futures::StreamExt;

use crate::{common::Message, runtime::{functions::key_by::{KeyByFunction, KeyByFunctionTrait}, operators::operator::{MessageStream, OperatorBase, OperatorConfig, OperatorPollResult, OperatorTrait, OperatorType}, runtime_context::RuntimeContext}, storage::storage::Storage};

pub struct KeyByOperator {
    base: OperatorBase,
}

impl fmt::Debug for KeyByOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyByOperator")
            .field("base", &self.base)
            .finish()
    }
}

impl KeyByOperator {
    pub fn new(config: OperatorConfig, storage: Arc<Storage>) -> Self {
        let key_by_function = match config.clone() {
            OperatorConfig::KeyByConfig(key_by_function) => key_by_function,
            _ => panic!("Expected KeyByConfig, got {:?}", config),
        };
        Self { 
            base: OperatorBase::new_with_function(key_by_function, config, storage),
        }
    }
}

#[async_trait]
impl OperatorTrait for KeyByOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    fn set_input(&mut self, input: Option<MessageStream>) {
        self.base.set_input(input);
    }

    async fn poll_next(&mut self) -> OperatorPollResult {
        // First, return any buffered messages
        if let Some(msg) = self.base.pending_messages.pop() {
            return OperatorPollResult::Ready(msg);
        }
        
        // Then process input stream
        let input_stream = self.base.input.as_mut().expect("input stream not set");
        match input_stream.next().await {
            Some(Message::Watermark(watermark)) => {
                return OperatorPollResult::Ready(Message::Watermark(watermark));
            }
            Some(message) => {
                let function = self.base.get_function_mut::<KeyByFunction>().unwrap();
                let function = function.clone();
                
                let keyed_messages = function.key_by(message);
                // Convert to Messages
                let mut messages: Vec<Message> = keyed_messages.into_iter()
                    .map(Message::Keyed)
                    .collect();
                
                if messages.is_empty() {
                    panic!("KeyBy operator produced no messages");
                }
                if messages.len() == 1 {
                    // Single message, return it directly
                    return OperatorPollResult::Ready(messages.pop().unwrap());
                } else {
                    // Multiple messages, return first and buffer the rest
                    let first_message = messages.remove(0);
                    // Add remaining messages to buffer (in reverse order since we pop from end)
                    messages.reverse();
                    self.base.pending_messages.extend(messages);
                    return OperatorPollResult::Ready(first_message);
                }
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
