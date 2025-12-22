use std::{fmt};

use crate::{common::Message, runtime::{functions::sink::{sink_function::create_sink_function, SinkFunction, SinkFunctionTrait}, operators::operator::{MessageStream, OperatorBase, OperatorConfig, OperatorPollResult, OperatorTrait, OperatorType}, runtime_context::RuntimeContext}};
use anyhow::Result;
use async_trait::async_trait;


#[derive(Clone, Debug)]
pub enum SinkConfig {
    InMemoryStorageGrpcSinkConfig(String), // server_addr
    RequestSinkConfig,
}

impl std::fmt::Display for SinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SinkConfig::InMemoryStorageGrpcSinkConfig(_) => write!(f, "InMemoryStorageGrpc"),
            SinkConfig::RequestSinkConfig => write!(f, "Request"),
        }
    }
}

pub struct SinkOperator {
    base: OperatorBase,
}

impl fmt::Debug for SinkOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SinkOperator")
            .field("base", &self.base)
            .finish()
    }
}

impl SinkOperator {
    pub fn new(config: OperatorConfig) -> Self {
        let sink_config = match config.clone() {
            OperatorConfig::SinkConfig(sink_config) => sink_config,
            _ => panic!("Expected SinkConfig, got {:?}", config),
        };
        let sink_function = create_sink_function(sink_config);
        Self {
            base: OperatorBase::new_with_function(sink_function, config),
        }
    }
}

#[async_trait]
impl OperatorTrait for SinkOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }

    fn set_input(&mut self, input: Option<MessageStream>) {
        self.base.set_input(input);
    }

    fn operator_config(&self) -> &OperatorConfig {
        self.base.operator_config()
    }

    async fn poll_next(&mut self) -> OperatorPollResult {
        match self.base.next_input().await {
            Some(Message::Watermark(watermark)) => OperatorPollResult::Ready(Message::Watermark(watermark)),
            Some(Message::CheckpointBarrier(barrier)) => OperatorPollResult::Ready(Message::CheckpointBarrier(barrier)),
            Some(message) => {
                let function = self.base.get_function_mut::<SinkFunction>().unwrap();
                function.sink(message.clone()).await.unwrap();
                OperatorPollResult::Ready(message)
            }
            None => OperatorPollResult::None,
        }
    }
}
