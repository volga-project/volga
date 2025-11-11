use std::sync::Arc;
use std::fmt;

use crate::{common::Message, runtime::{operators::operator::{MessageStream, OperatorBase, OperatorConfig, OperatorPollResult, OperatorTrait, OperatorType}, runtime_context::RuntimeContext}};
use async_trait::async_trait;
use anyhow::Result;
use futures::StreamExt;

pub struct JoinOperator {
    base: OperatorBase,
    left_buffer: Vec<Message>,
    right_buffer: Vec<Message>,
}

impl fmt::Debug for JoinOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinOperator")
            .field("base", &self.base)
            .field("left_buffer", &self.left_buffer)
            .field("right_buffer", &self.right_buffer)
            .finish()
    }
}

impl JoinOperator {
    pub fn new(config: OperatorConfig) -> Self {
        let join_function = match config.clone() {
            OperatorConfig::JoinConfig(join_function) => join_function,
            _ => panic!("Expected JoinConfig, got {:?}", config),
        };
        Self { 
            base: OperatorBase::new_with_function(join_function, config),
            left_buffer: Vec::new(),
            right_buffer: Vec::new(),
        }
    }
}

#[async_trait]
impl OperatorTrait for JoinOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    fn set_input(&mut self, input: Option<MessageStream>) {
        self.base.set_input(input);
    }

    async fn poll_next(&mut self) -> OperatorPollResult {
        let input_stream = self.base.input.as_mut().expect("input stream not set");
        match input_stream.next().await {
            Some(Message::Watermark(watermark)) => {
                OperatorPollResult::Ready(Message::Watermark(watermark))
            }
            Some(message) => {
                // TODO proper lookup for upstream_vertex_id position (left or right)
                if let Some(upstream_id) = message.upstream_vertex_id() {
                    if upstream_id.contains("left") {
                        self.left_buffer.push(message.clone());
                    } else {
                        self.right_buffer.push(message.clone());
                    }
                }
                OperatorPollResult::Ready(message)
            }
            None => OperatorPollResult::None,
        }
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }
}
