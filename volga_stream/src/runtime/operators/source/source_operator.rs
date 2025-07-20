use anyhow::Result;
use arrow::{array::{ArrayRef, RecordBatch}, datatypes::SchemaRef};
use async_trait::async_trait;

use crate::{common::Message, runtime::{functions::source::{create_source_function, word_count_source::BatchingMode, SourceFunction, SourceFunctionTrait}, operators::operator::{OperatorBase, OperatorTrait, OperatorType, OperatorConfig}, runtime_context::RuntimeContext}};


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
}

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
        }
    }

    pub fn set_projection(&mut self, projection: Vec<usize>, schema: SchemaRef) {
        match self {
            SourceConfig::VectorSourceConfig(config) => config.set_projection(projection, schema),
            SourceConfig::WordCountSourceConfig(config) => config.set_projection(projection, schema),
        }
    }
}

#[derive(Debug)]
pub struct SourceOperator {
    base: OperatorBase,
    projection: Option<Vec<usize>>,
    projected_schema: Option<SchemaRef>,
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
            projection: None,
            projected_schema: None,
        }
    }
}

#[async_trait]
impl OperatorTrait for SourceOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::Source
    }

    async fn fetch(&mut self) -> Option<Vec<Message>> {
        let function = self.base.get_function_mut::<SourceFunction>().unwrap();
        let msg = function.fetch().await;

        // TODO test
        // Apply projection if present
        let msg = match msg {
            Some(message) => {
                if let Some(proj) = &self.projection {
                    let batch = message.record_batch();
                    let projected_columns: Vec<ArrayRef> = proj
                        .iter()
                        .map(|&i| batch.column(i).clone())
                        .collect();
                   
                    let projected_batch = RecordBatch::try_new(
                        self.projected_schema.clone().expect("should have schema with projection"),
                        projected_columns,
                    ).unwrap();
                    
                    let projected_message = Message::new(message.upstream_vertex_id(), projected_batch, message.ingest_timestamp());
                    Some(projected_message)
                } else {
                    Some(message)
                }
            }
            None => None
        };
        if msg.is_none() {
            None
        } else {
            Some(vec![msg.unwrap()])
        }
    }
}