use std::sync::Arc;

use crate::{common::Message, runtime::{operators::operator::{OperatorBase, OperatorConfig, OperatorTrait, OperatorType}, runtime_context::RuntimeContext}, storage::storage::Storage};
use async_trait::async_trait;
use anyhow::Result;

#[derive(Debug)]
pub struct JoinOperator {
    base: OperatorBase,
    left_buffer: Vec<Message>,
    right_buffer: Vec<Message>,
}

impl JoinOperator {
    pub fn new(config: OperatorConfig, storage: Arc<Storage>) -> Self {
        let join_function = match config.clone() {
            OperatorConfig::JoinConfig(join_function) => join_function,
            _ => panic!("Expected JoinConfig, got {:?}", config),
        };
        Self { 
            base: OperatorBase::new_with_function(join_function, config, storage),
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
        self.base.operator_type()
    }
}