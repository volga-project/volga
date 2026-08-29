use crate::{
    common::Message,
    runtime::{
        functions::sink::{
            parquet::ParquetSinkConfig, sink_function::create_sink_function, SinkFunction,
            SinkFunctionTrait,
        },
        operators::operator::{
            OperatorBase, OperatorConfig, OperatorTrait, OperatorType, Output, StreamOperator,
        },
        runtime_context::RuntimeContext,
    },
};
use anyhow::Result;
use async_trait::async_trait;

#[derive(Clone, Debug)]
pub enum SinkConfig {
    InMemoryStorageGrpcSinkConfig {
        server_addr: String,
        /// When non-empty, explode rows and upsert into the keyed map by these columns.
        upsert_key_columns: Vec<String>,
    },
    RequestSinkConfig,
    ParquetSinkConfig(ParquetSinkConfig),
    CountSinkConfig,
}

impl SinkConfig {
    pub fn in_memory_grpc(server_addr: impl Into<String>) -> Self {
        Self::InMemoryStorageGrpcSinkConfig {
            server_addr: server_addr.into(),
            upsert_key_columns: Vec::new(),
        }
    }

    pub fn with_upsert_key_columns(mut self, columns: Vec<String>) -> Self {
        if let Self::InMemoryStorageGrpcSinkConfig {
            upsert_key_columns, ..
        } = &mut self
        {
            *upsert_key_columns = columns;
        }
        self
    }
}

impl std::fmt::Display for SinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SinkConfig::InMemoryStorageGrpcSinkConfig { .. } => write!(f, "InMemoryStorageGrpc"),
            SinkConfig::RequestSinkConfig => write!(f, "Request"),
            SinkConfig::ParquetSinkConfig(_) => write!(f, "Parquet"),
            SinkConfig::CountSinkConfig => write!(f, "Count"),
        }
    }
}

#[derive(Debug)]
pub struct SinkOperator {
    base: OperatorBase,
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

    fn operator_config(&self) -> &OperatorConfig {
        self.base.operator_config()
    }
}

#[async_trait]
impl StreamOperator for SinkOperator {
    async fn process_data(&mut self, data: Vec<Message>, out: &mut dyn Output) -> Result<()> {
        let function = self.base.get_function_mut::<SinkFunction>().unwrap();
        for message in data {
            function.sink(message.clone()).await?;
            out.emit(message).await?;
        }
        Ok(())
    }

    async fn handle_barrier(
        &mut self,
        barrier: crate::common::message::CheckpointBarrierMessage,
        out: &mut dyn Output,
    ) -> Result<()> {
        // Barrier alignment must imply durable sink state for restore safety.
        let function = self.base.get_function_mut::<SinkFunction>().unwrap();
        function.flush().await?;
        out.emit(Message::CheckpointBarrier(barrier)).await
    }
}
