use async_trait::async_trait;
use anyhow::Result;
use std::fmt;
use crate::common::message::Message;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::function_trait::FunctionTrait;
use std::any::Any;
use super::vector_source::VectorSourceFunction;
use super::word_count_source::WordCountSourceFunction;

#[async_trait]
pub trait SourceFunctionTrait: Send + Sync + fmt::Debug {
    async fn fetch(&mut self) -> Option<Message>;
}

#[derive(Debug)]
pub enum SourceFunction {
    Vector(VectorSourceFunction),
    WordCount(WordCountSourceFunction),
}

#[async_trait]
impl SourceFunctionTrait for SourceFunction {
    async fn fetch(&mut self) -> Option<Message> {
        match self {
            SourceFunction::Vector(f) => f.fetch().await,
            SourceFunction::WordCount(f) => f.fetch().await,
        }
    }
}

#[async_trait]
impl FunctionTrait for SourceFunction {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        match self {
            SourceFunction::Vector(f) => f.open(context).await,
            SourceFunction::WordCount(f) => f.open(context).await,
        }
    }
    
    async fn close(&mut self) -> Result<()> {
        match self {
            SourceFunction::Vector(f) => f.close().await,
            SourceFunction::WordCount(f) => f.close().await,
        }
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
    
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

pub fn create_source_function(config: crate::runtime::execution_graph::SourceConfig) -> SourceFunction {
    match config {
        crate::runtime::execution_graph::SourceConfig::VectorSourceConfig(messages) => {
            SourceFunction::Vector(VectorSourceFunction::new(messages))
        }
        crate::runtime::execution_graph::SourceConfig::WordCountSourceConfig { 
            word_length, 
            num_words, 
            num_to_send_per_word,
            run_for_s, 
            batch_size,
            batching_mode,
        } => {
            SourceFunction::WordCount(WordCountSourceFunction::new(
                word_length,
                num_words,
                num_to_send_per_word,
                run_for_s,
                batch_size,
                batching_mode,
            ))
        }
    }
} 