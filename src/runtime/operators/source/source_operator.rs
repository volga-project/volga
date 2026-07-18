use anyhow::Result;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use std::sync::Arc;

use crate::common::Message;
use crate::runtime::functions::source::{
    create_source_function, datagen_source::DatagenSourceConfig, kafka::KafkaSourceConfig,
    parquet::ParquetSourceConfig, word_count_source::BatchingMode, FetchResult,
    RequestSourceConfig, SourceFunction, SourceFunctionTrait,
};
use crate::runtime::operators::operator::{
    operator_config_requires_checkpoint, MessageStream, OperatorBase, OperatorConfig,
    OperatorPollResult, OperatorTrait, OperatorType,
};
use crate::runtime::runtime_context::RuntimeContext;
use super::source_handles::{SourceHandle, SourceInterrupt};

#[derive(Debug, Clone)]
pub struct VectorSourceConfig {
    pub messages: Vec<Message>,
    pub projection: Option<Vec<usize>>,
    pub projected_schema: Option<SchemaRef>,
}

#[derive(Debug, Clone)]
pub struct WordCountSourceConfig {
    pub word_length: usize,
    pub dictionary_size: usize, // Total pool of words to generate
    pub num_to_send_per_word: Option<usize>, // Optional: how many copies of each word to send
    pub run_for_s: Option<u64>,
    pub batch_size: usize,
    pub batching_mode: BatchingMode, // Controls how words are batched together
    pub projection: Option<Vec<usize>>,
    pub projected_schema: Option<SchemaRef>,
}

#[derive(Debug, Clone)]
pub enum SourceConfig {
    VectorSourceConfig(VectorSourceConfig),
    WordCountSourceConfig(WordCountSourceConfig),
    DatagenSourceConfig(DatagenSourceConfig),
    HttpRequestSourceConfig(RequestSourceConfig),
    KafkaSourceConfig(KafkaSourceConfig),
    ParquetSourceConfig(ParquetSourceConfig),
}

// TODO deprecate in favor of datagen
impl VectorSourceConfig {
    pub fn new(messages: Vec<Message>) -> Self {
        Self {
            messages,
            projection: None,
            projected_schema: None,
        }
    }

    pub fn get_projection(&self) -> (Option<Vec<usize>>, Option<SchemaRef>) {
        (self.projection.clone(), self.projected_schema.clone())
    }

    pub fn set_projection(&mut self, projection: Vec<usize>, schema: SchemaRef) {
        self.projection = Some(projection);
        self.projected_schema = Some(schema);
    }
}

// TODO deprecate in favor of datagen
impl WordCountSourceConfig {
    pub fn new(
        word_length: usize,
        dictionary_size: usize,
        num_to_send_per_word: Option<usize>,
        run_for_s: Option<u64>,
        batch_size: usize,
        batching_mode: BatchingMode,
    ) -> Self {
        Self {
            word_length,
            dictionary_size,
            num_to_send_per_word,
            run_for_s,
            batch_size,
            batching_mode,
            projection: None,
            projected_schema: None,
        }
    }

    pub fn get_projection(&self) -> (Option<Vec<usize>>, Option<SchemaRef>) {
        (self.projection.clone(), self.projected_schema.clone())
    }

    pub fn set_projection(&mut self, projection: Vec<usize>, schema: SchemaRef) {
        self.projection = Some(projection);
        self.projected_schema = Some(schema);
    }
}

impl SourceConfig {
    pub fn get_projection(&self) -> (Option<Vec<usize>>, Option<SchemaRef>) {
        match self {
            SourceConfig::VectorSourceConfig(config) => config.get_projection(),
            SourceConfig::WordCountSourceConfig(config) => config.get_projection(),
            SourceConfig::DatagenSourceConfig(config) => config.get_projection(),
            SourceConfig::HttpRequestSourceConfig(_) => (None, None), // Request source doesn't support projection
            SourceConfig::KafkaSourceConfig(config) => config.get_projection(),
            SourceConfig::ParquetSourceConfig(config) => config.get_projection(),
        }
    }

    pub fn set_projection(&mut self, projection: Vec<usize>, schema: SchemaRef) {
        match self {
            SourceConfig::VectorSourceConfig(config) => config.set_projection(projection, schema),
            SourceConfig::WordCountSourceConfig(config) => config.set_projection(projection, schema),
            SourceConfig::DatagenSourceConfig(config) => config.set_projection(projection, schema),
            SourceConfig::HttpRequestSourceConfig(_) => {}, // Request source doesn't support projection
            SourceConfig::KafkaSourceConfig(config) => config.set_projection(projection, schema),
            SourceConfig::ParquetSourceConfig(config) => config.set_projection(projection, schema),
        }
    }
}

impl std::fmt::Display for SourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceConfig::VectorSourceConfig(_) => write!(f, "Vector"),
            SourceConfig::WordCountSourceConfig(_) => write!(f, "WordCount"),
            SourceConfig::DatagenSourceConfig(_) => write!(f, "Datagen"),
            SourceConfig::HttpRequestSourceConfig(_) => write!(f, "HttpRequest"),
            SourceConfig::KafkaSourceConfig(_) => write!(f, "Kafka"),
            SourceConfig::ParquetSourceConfig(_) => write!(f, "Parquet"),
        }
    }
}

#[derive(Debug)]
pub struct SourceOperator {
    base: OperatorBase,
    handle: Option<Arc<SourceHandle>>,
}

impl SourceOperator {
    pub fn new(config: OperatorConfig) -> Self {
        let source_config = match config.clone() {
            OperatorConfig::SourceConfig(source_config) => source_config,
            _ => panic!("Expected SourceConfig, got {:?}", config),
        };
        let source_function = create_source_function(source_config);
        Self {
            base: OperatorBase::new_with_function(source_function, config),
            handle: None,
        }
    }

    /// Interrupt is only used for checkpointable sources (barrier yield).
    fn checkpoint_interrupt_arc(&self) -> Option<Arc<SourceInterrupt>> {
        if !operator_config_requires_checkpoint(self.operator_config()) {
            return None;
        }
        self.handle.as_ref().map(|h| h.interrupt.clone())
    }
}

#[async_trait]
impl OperatorTrait for SourceOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        if let Some(handles) = context.source_handles() {
            self.handle = Some(handles.register(context.vertex_id_arc()));
        }
        self.base.open(context).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::Source
    }

    fn operator_config(&self) -> &OperatorConfig {
        self.base.operator_config()
    }

    fn set_input(&mut self, input: Option<MessageStream>) {
        self.base.set_input(input);
    }

    async fn poll_next(&mut self) -> OperatorPollResult {
        if self.handle.as_ref().is_some_and(|h| h.is_stopped()) {
            return OperatorPollResult::None;
        }

        let interrupt = self.checkpoint_interrupt_arc();

        // Wake arrived between fetches: yield before pulling more data.
        if interrupt
            .as_ref()
            .is_some_and(|interrupt| interrupt.take_canceled())
        {
            return OperatorPollResult::Continue;
        }

        let function = self.base.get_function_mut::<SourceFunction>().unwrap();
        match function.fetch(interrupt.as_deref()).await {
            FetchResult::Interrupted => OperatorPollResult::Continue,
            FetchResult::Data(Message::Watermark(watermark)) => {
                OperatorPollResult::Ready(Message::Watermark(watermark))
            }
            FetchResult::Data(message) => {
                if let Some(handle) = self.handle.as_ref() {
                    match &message {
                        Message::Regular(_) | Message::Keyed(_) => {
                            handle.stats.add_records(message.num_records());
                        }
                        Message::Watermark(_) | Message::CheckpointBarrier(_) => {}
                    }
                }
                OperatorPollResult::Ready(message)
            }
            FetchResult::Idle => OperatorPollResult::None,
        }
    }

    async fn checkpoint(&mut self, _checkpoint_id: u64) -> Result<Vec<(String, Vec<u8>)>> {
        let function = self.base.get_function_mut::<SourceFunction>().unwrap();
        let pos = function.snapshot_position().await?;
        if pos.is_empty() {
            return Err(anyhow::anyhow!(
                "checkpointable source produced empty source_position blob"
            ));
        }
        Ok(vec![("source_position".to_string(), pos)])
    }

    async fn restore(&mut self, blobs: &[(String, Vec<u8>)]) -> Result<()> {
        let function = self.base.get_function_mut::<SourceFunction>().unwrap();
        let Some((_, bytes)) = blobs.iter().find(|(name, _)| name == "source_position") else {
            return Err(anyhow::anyhow!("missing source_position blob on restore"));
        };
        if bytes.is_empty() {
            return Err(anyhow::anyhow!("empty source_position blob on restore"));
        }
        function.restore_position(bytes).await?;
        if let (Some(handle), Some(n)) = (self.handle.as_ref(), function.emit_count()) {
            handle.stats.set_records_generated(n);
        }
        Ok(())
    }
}
