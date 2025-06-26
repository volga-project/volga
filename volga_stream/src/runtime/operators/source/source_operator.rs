use anyhow::Result;
use async_trait::async_trait;

use crate::{common::Message, runtime::{functions::source::{create_source_function, word_count_source::BatchingMode, SourceFunction, SourceFunctionTrait}, operators::operator::{OperatorBase, OperatorTrait, OperatorType, OperatorConfig}, runtime_context::RuntimeContext}};


#[derive(Debug, Clone)]
pub enum SourceConfig {
    VectorSourceConfig(Vec<Message>),
    WordCountSourceConfig {
        word_length: usize,
        num_words: usize, // Total pool of words to generate
        num_to_send_per_word: Option<usize>, // Optional: how many copies of each word to send
        run_for_s: Option<u64>,
        batch_size: usize,
        batching_mode: BatchingMode, // Controls how words are batched together
    },
}

#[derive(Debug)]
pub struct SourceOperator {
    base: OperatorBase,
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
        if msg.is_none() {
            None
        } else {
            Some(vec![msg.unwrap()])
        }
    }
}