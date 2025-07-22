use async_trait::async_trait;
use crate::runtime::functions::join::join_function::JoinFunction;
use crate::runtime::operators::chained::chained_operator::ChainedOperator;
use crate::runtime::operators::join::join_operator::JoinOperator;
use crate::runtime::operators::key_by::key_by_operator::KeyByOperator;
use crate::runtime::operators::map::map_operator::MapOperator;
use crate::runtime::operators::reduce::reduce_operator::ReduceOperator;
use crate::runtime::operators::sink::sink_operator::{SinkConfig, SinkOperator};
use crate::runtime::operators::source::source_operator::{SourceConfig, SourceOperator};
use crate::runtime::runtime_context::RuntimeContext;
use crate::common::message::Message;
use crate::common::Key;
use anyhow::Result;
use tokio_rayon::rayon::{ThreadPool, ThreadPoolBuilder};
use tokio_rayon::AsyncThreadPool;
use std::fmt;
use crate::runtime::functions::{
    function_trait::FunctionTrait,
    source::{SourceFunction, SourceFunctionTrait, create_source_function},
    sink::{SinkFunction, SinkFunctionTrait},
    sink::sink_function::create_sink_function,
    map::MapFunction,
    key_by::{KeyByFunction, KeyByFunctionTrait},
    reduce::{ReduceFunction, ReduceFunctionTrait, Accumulator, AggregationResultExtractor, AggregationResultExtractorTrait},
};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorType {
    Source,
    Sink,
    Processor,
    ChainedSourceSink,
}

#[async_trait]
pub trait OperatorTrait: Send + Sync + fmt::Debug {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
    async fn process_message(&mut self, message: Message) -> Option<Vec<Message>> {
        panic!("process_message not implemented for this operator")
    }
    fn operator_type(&self) -> OperatorType;
    async fn fetch(&mut self) -> Option<Vec<Message>> {
        panic!("fetch not implemented for this operator")
    }
}

#[derive(Debug)]
pub enum Operator {
    Map(MapOperator),
    Join(JoinOperator),
    Sink(SinkOperator),
    Source(SourceOperator),
    KeyBy(KeyByOperator),
    Reduce(ReduceOperator),
    Chained(ChainedOperator),
}

#[derive(Clone, Debug)]
pub enum OperatorConfig {
    MapConfig(MapFunction),
    JoinConfig(JoinFunction),
    SinkConfig(SinkConfig),
    SourceConfig(SourceConfig),
    KeyByConfig(KeyByFunction),
    ReduceConfig(ReduceFunction, Option<AggregationResultExtractor>),
    ChainedConfig(Vec<OperatorConfig>),
}

impl fmt::Display for OperatorConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OperatorConfig::MapConfig(map_func) => write!(f, "Map({})", map_func),
            OperatorConfig::JoinConfig(join_func) => write!(f, "Join({})", join_func),
            OperatorConfig::SinkConfig(sink_config) => write!(f, "Sink({})", sink_config),
            OperatorConfig::SourceConfig(source_config) => write!(f, "Source({})", source_config),
            OperatorConfig::KeyByConfig(key_by_func) => write!(f, "KeyBy({})", key_by_func),
            OperatorConfig::ReduceConfig(reduce_func, _) => write!(f, "Reduce({})", reduce_func),
            OperatorConfig::ChainedConfig(configs) => write!(f, "Chained({} ops)", configs.len()),
        }
    }
}

#[async_trait]
impl OperatorTrait for Operator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        match self {
            Operator::Map(op) => op.open(context).await,
            Operator::Join(op) => op.open(context).await,
            Operator::Sink(op) => op.open(context).await,
            Operator::Source(op) => op.open(context).await,
            Operator::KeyBy(op) => op.open(context).await,
            Operator::Reduce(op) => op.open(context).await,
            Operator::Chained(op) => op.open(context).await
        }
    }

    async fn close(&mut self) -> Result<()> {
        match self {
            Operator::Map(op) => op.close().await,
            Operator::Join(op) => op.close().await,
            Operator::Sink(op) => op.close().await,
            Operator::Source(op) => op.close().await,
            Operator::KeyBy(op) => op.close().await,
            Operator::Reduce(op) => op.close().await,
            Operator::Chained(op) => op.close().await
        }
    }

    async fn process_message(&mut self, message: Message) -> Option<Vec<Message>> {
        match &message {
            Message::Watermark(watermark) => {
                panic!("Watermark should not be processed for operator");
            }
            _ => {
                // Process regular messages as before
                match self {
                    Operator::Map(op) => op.process_message(message).await,
                    Operator::Join(op) => op.process_message(message).await,
                    Operator::Sink(op) => op.process_message(message).await,
                    Operator::Source(op) => op.process_message(message).await,
                    Operator::KeyBy(op) => op.process_message(message).await,
                    Operator::Reduce(op) => op.process_message(message).await,
                    Operator::Chained(op) => op.process_message(message).await,
                }
            }
        }
    }

    fn operator_type(&self) -> OperatorType {
        match self {
            Operator::Map(op) => op.operator_type(),
            Operator::Join(op) => op.operator_type(),
            Operator::Sink(op) => op.operator_type(),
            Operator::Source(op) => op.operator_type(),
            Operator::KeyBy(op) => op.operator_type(),
            Operator::Reduce(op) => op.operator_type(),
            Operator::Chained(op) => op.operator_type(),
        }
    }

    async fn fetch(&mut self) -> Option<Vec<Message>> {
        match self {
            Operator::Map(op) => op.fetch().await,
            Operator::Join(op) => op.fetch().await,
            Operator::Sink(op) => op.fetch().await,
            Operator::Source(op) => op.fetch().await,
            Operator::KeyBy(op) => op.fetch().await,
            Operator::Reduce(op) => op.fetch().await,
            Operator::Chained(op) => op.fetch().await,
        }
    }
}

#[derive(Debug)]
pub struct OperatorBase {
    pub runtime_context: Option<RuntimeContext>,
    pub function: Option<Box<dyn FunctionTrait>>,
    pub thread_pool: ThreadPool,
    pub operator_config: OperatorConfig
}

impl OperatorBase {
    pub fn new(operator_config: OperatorConfig) -> Self {
        // TODO config thread pool size
        let thread_pool = ThreadPoolBuilder::new()
            .num_threads(std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
            .build()
            .expect("Failed to create thread pool");
        Self {
            runtime_context: None,
            function: None,
            thread_pool,
            operator_config
        }
    }
    
    pub fn new_with_function<F: FunctionTrait + 'static>(function: F, operator_config: OperatorConfig) -> Self {
        
        // TODO config thread pool size
        let thread_pool = ThreadPoolBuilder::new()
            .num_threads(std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
            .build()
            .expect("Failed to create thread pool");
        
        Self {
            runtime_context: None,
            function: Some(Box::new(function)),
            thread_pool,
            operator_config
        }
    }
    
    pub fn get_function<T: 'static>(&self) -> Option<&T> {
        self.function.as_ref()
            .and_then(|f| f.as_any().downcast_ref::<T>())
    }
    
    pub fn get_function_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.function.as_mut()
            .and_then(|f| f.as_any_mut().downcast_mut::<T>())
    }
}

#[async_trait]
impl OperatorTrait for OperatorBase {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.runtime_context = Some(context.clone());
        if let Some(function) = &mut self.function {
            function.open(context).await?;
        }
        
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(function) = &mut self.function {
            function.close().await?;
        }
        
        Ok(())
    }

    fn operator_type(&self) -> OperatorType {
        get_operator_type_from_config(&self.operator_config)
    }
}

pub fn create_operator_from_config(operator_config: OperatorConfig) -> Operator {
    let operator = match operator_config {
        OperatorConfig::MapConfig(_) => Operator::Map(MapOperator::new(operator_config)),
        OperatorConfig::JoinConfig(_) => Operator::Join(JoinOperator::new(operator_config)),
        OperatorConfig::SinkConfig(_) => Operator::Sink(SinkOperator::new(operator_config)),
        OperatorConfig::SourceConfig(_) => Operator::Source(SourceOperator::new(operator_config)),
        OperatorConfig::KeyByConfig(_) => Operator::KeyBy(KeyByOperator::new(operator_config)),
        OperatorConfig::ReduceConfig(_, _) => Operator::Reduce(ReduceOperator::new(operator_config)),
        OperatorConfig::ChainedConfig(_) => Operator::Chained(ChainedOperator::new(operator_config)),
    };
    operator
}

pub fn get_operator_type_from_config(operator_config: &OperatorConfig) -> OperatorType {
    match operator_config {
        OperatorConfig::SourceConfig(_) => OperatorType::Source,
        OperatorConfig::SinkConfig(_) => OperatorType::Sink,
        OperatorConfig::ChainedConfig(configs   ) => {
            let mut has_source = false;
            let mut has_sink = false;
            for config in configs {
                match config {
                    OperatorConfig::SourceConfig(_) => {
                        has_source = true;
                    },
                    OperatorConfig::SinkConfig(_) => {
                        has_sink = true;
                    },
                    _ => {}
                }
            }

            if has_source && has_sink {
                OperatorType::ChainedSourceSink
            } else if has_source {
                OperatorType::Source
            } else if has_sink {
                OperatorType::Sink
            } else {
                OperatorType::Processor
            }
        },
        OperatorConfig::MapConfig(_) | 
        OperatorConfig::JoinConfig(_) | 
        OperatorConfig::KeyByConfig(_) | 
        OperatorConfig::ReduceConfig(_, _) => {
            OperatorType::Processor
        }
    }
}