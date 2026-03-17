use std::{fmt};

use async_trait::async_trait;
use anyhow::Result;

use crate::{common::Message, runtime::{functions::key_by::{KeyByFunction, KeyByFunctionTrait}, operators::operator::{MessageStream, OperatorBase, OperatorConfig, OperatorPollResult, OperatorTrait, OperatorType}, runtime_context::RuntimeContext}};

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

    fn set_input(&mut self, input: Option<MessageStream>) {
        self.base.set_input(input);
    }

    fn operator_config(&self) -> &OperatorConfig {
        self.base.operator_config()
    }

    async fn poll_next(&mut self) -> OperatorPollResult {
        if let Some(msg) = self.base.pop_pending_output() {
            return OperatorPollResult::Ready(msg);
        }

        match self.base.next_input().await {
            Some(Message::Watermark(watermark)) => OperatorPollResult::Ready(Message::Watermark(watermark)),
            Some(Message::CheckpointBarrier(barrier)) => OperatorPollResult::Ready(Message::CheckpointBarrier(barrier)),
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
