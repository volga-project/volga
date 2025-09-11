use std::sync::Arc;

use crate::{common::Message, runtime::{functions::sink::{sink_function::create_sink_function, SinkFunction, SinkFunctionTrait}, operators::operator::{OperatorBase, OperatorConfig, OperatorTrait, OperatorType}, runtime_context::RuntimeContext}, storage::storage::Storage};
use anyhow::Result;
use async_trait::async_trait;


#[derive(Clone, Debug)]
pub enum SinkConfig {
    InMemoryStorageGrpcSinkConfig(String), // server_addr
}

impl std::fmt::Display for SinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SinkConfig::InMemoryStorageGrpcSinkConfig(_) => write!(f, "InMemoryStorageGrpc"),
        }
    }
}

#[derive(Debug)]
pub struct SinkOperator {
    base: OperatorBase,
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

    async fn process_message(&mut self, message: Message) -> Option<Vec<Message>> {
        let function = self.base.get_function_mut::<SinkFunction>().unwrap();
        function.sink(message.clone()).await.unwrap();
        Some(vec![message])
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }
}