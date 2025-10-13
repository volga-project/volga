use async_trait::async_trait;
use anyhow::Result;
use std::fmt;
use crate::common::message::Message;
use crate::runtime::operators::source::source_operator::SourceConfig;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use std::any::Any;
use super::vector_source::VectorSourceFunction;
use super::word_count_source::WordCountSourceFunction;
use super::datagen_source::DatagenSourceFunction;

#[async_trait]
pub trait SourceFunctionTrait: Send + Sync + fmt::Debug {
    async fn fetch(&mut self) -> Option<Message>;
}

#[derive(Debug)]
pub enum SourceFunction {
    Vector(VectorSourceFunction),
    WordCount(WordCountSourceFunction),
    Datagen(DatagenSourceFunction),
}

impl fmt::Display for SourceFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SourceFunction::Vector(_) => write!(f, "Vector"),
            SourceFunction::WordCount(_) => write!(f, "WordCount"),
            SourceFunction::Datagen(_) => write!(f, "Datagen"),
        }
    }
}

#[async_trait]
impl SourceFunctionTrait for SourceFunction {
    async fn fetch(&mut self) -> Option<Message> {
        match self {
            SourceFunction::Vector(f) => f.fetch().await,
            SourceFunction::WordCount(f) => f.fetch().await,
            SourceFunction::Datagen(f) => f.fetch().await,
        }
    }
}

#[async_trait]
impl FunctionTrait for SourceFunction {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        match self {
            SourceFunction::Vector(f) => f.open(context).await,
            SourceFunction::WordCount(f) => f.open(context).await,
            SourceFunction::Datagen(f) => f.open(context).await,
        }
    }
    
    async fn close(&mut self) -> Result<()> {
        match self {
            SourceFunction::Vector(f) => f.close().await,
            SourceFunction::WordCount(f) => f.close().await,
            SourceFunction::Datagen(f) => f.close().await,
        }
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
    
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub fn create_source_function(config: SourceConfig) -> SourceFunction {
    match config {
        SourceConfig::VectorSourceConfig(vector_config) => {
            SourceFunction::Vector(VectorSourceFunction::new(vector_config.messages))
        }
        SourceConfig::WordCountSourceConfig(word_config) => {
            SourceFunction::WordCount(WordCountSourceFunction::new(
                word_config.word_length,
                word_config.dictionary_size,
                word_config.num_to_send_per_word,
                word_config.run_for_s,
                word_config.batch_size,
                word_config.batching_mode,
            ))
        }
    }
} 