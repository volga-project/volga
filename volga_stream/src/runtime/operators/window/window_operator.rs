use anyhow::Result;
use async_trait::async_trait;
use crate::common::message::Message;
use crate::runtime::operators::operator::{OperatorBase, OperatorConfig, OperatorTrait, OperatorType};
use crate::runtime::runtime_context::RuntimeContext;
use super::{InputBuffer, KeyedWindowsState};

#[derive(Debug, Clone)]
pub struct WindowConfig {
    // TODO: Add window configuration fields
}

#[derive(Debug)]
pub struct WindowOperator {
    base: OperatorBase,
    input_buffer: InputBuffer,
    keyed_state: KeyedWindowsState,
}

impl WindowOperator {
    pub fn new(config: OperatorConfig) -> Self {
        let _window_config = match config.clone() {
            OperatorConfig::WindowConfig(window_config) => window_config,
            _ => panic!("Expected WindowConfig, got {:?}", config),
        };

        Self {
            base: OperatorBase::new(config),
            input_buffer: InputBuffer::new(),
            keyed_state: KeyedWindowsState::new(),
        }
    }
}

#[async_trait]
impl OperatorTrait for WindowOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    async fn process_message(&mut self, _message: Message) -> Option<Vec<Message>> {
        // TODO: Implementation
        None
    }

    async fn process_watermark(&mut self, _watermark: u64) -> Option<Vec<Message>> {
        // TODO: Implementation
        None
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }
}