use std::{sync::Arc, fmt};

use crate::{common::Message, runtime::{functions::sink::{sink_function::create_sink_function, SinkFunction, SinkFunctionTrait}, operators::operator::{MessageStream, OperatorBase, OperatorConfig, OperatorPollResult, OperatorTrait, OperatorType}, runtime_context::RuntimeContext}, storage::storage::Storage};
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;


#[derive(Clone, Debug)]
pub enum SinkConfig {
    InMemoryStorageGrpcSinkConfig(String), // server_addr
    RequestSinkConfig(tokio::sync::mpsc::Sender<Message>), // response_sender
}

impl std::fmt::Display for SinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SinkConfig::InMemoryStorageGrpcSinkConfig(_) => write!(f, "InMemoryStorageGrpc"),
            SinkConfig::RequestSinkConfig(_) => write!(f, "Request"),
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
    pub fn new(config: OperatorConfig, storage: Arc<Storage>) -> Self {
        let sink_config = match config.clone() {
            OperatorConfig::SinkConfig(sink_config) => sink_config,
            _ => panic!("Expected SinkConfig, got {:?}", config),
        };
        let sink_function = create_sink_function(sink_config);
        Self {
            base: OperatorBase::new_with_function(sink_function, config, storage),
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

    async fn poll_next(&mut self) -> OperatorPollResult {
        let input_stream = self.base.input.as_mut().expect("input stream not set");
        match input_stream.next().await {
            Some(Message::Watermark(watermark)) => {
                OperatorPollResult::Ready(Message::Watermark(watermark))
            }
            Some(message) => {
                let function = self.base.get_function_mut::<SinkFunction>().unwrap();
                function.sink(message.clone()).await.unwrap();
                OperatorPollResult::Ready(message)
            }
            None => OperatorPollResult::None,
        }
    }
}
