use crate::{common::Message, runtime::{operators::operator::{OperatorBase, OperatorTrait, OperatorType}, runtime_context::RuntimeContext}};
use async_trait::async_trait;
use anyhow::Result;

#[derive(Debug)]
pub struct JoinOperator {
    base: OperatorBase,
    left_buffer: Vec<Message>,
    right_buffer: Vec<Message>,
}

impl JoinOperator {
    pub fn new() -> Self {
        Self {
            base: OperatorBase::new(),
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

    async fn process_message(&mut self, message: Message) -> Option<Vec<Message>> {
        // TODO proper lookup for upstream_vertex_id position (left or right)
        if let Some(upstream_id) = message.upstream_vertex_id() {
            if upstream_id.contains("left") {
                self.left_buffer.push(message.clone());
            } else {
                self.right_buffer.push(message.clone());
            }
        }
        Some(vec![message])
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }
}