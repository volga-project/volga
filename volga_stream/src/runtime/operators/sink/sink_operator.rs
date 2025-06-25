use crate::{common::Message, runtime::{functions::sink::{sink_function::create_sink_function, SinkFunction, SinkFunctionTrait}, operators::operator::{OperatorBase, OperatorTrait, OperatorType}, runtime_context::RuntimeContext}};
use anyhow::Result;
use async_trait::async_trait;


#[derive(Clone, Debug)]
pub enum SinkConfig {
    InMemoryStorageGrpcSinkConfig(String), // server_addr
}

#[derive(Debug)]
pub struct SinkOperator {
    base: OperatorBase,
}

impl SinkOperator {
    pub fn new(config: SinkConfig) -> Self {
        let sink_function = create_sink_function(config);
        Self {
            base: OperatorBase::new_with_function(sink_function),
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
        OperatorType::SINK
    }
}