use async_trait::async_trait;
use futures::Stream;

use crate::runtime::functions::join::join_function::JoinFunction;
use crate::runtime::operators::aggregate::aggregate_operator::{AggregateConfig, AggregateOperator};
use crate::runtime::operators::chained::chained_operator::ChainedOperator;
use crate::runtime::operators::join::join_operator::JoinOperator;
use crate::runtime::operators::key_by::key_by_operator::KeyByOperator;
use crate::runtime::operators::map::map_operator::MapOperator;
use crate::runtime::operators::reduce::reduce_operator::ReduceOperator;
use crate::runtime::operators::sink::sink_operator::{SinkConfig, SinkOperator};
use crate::runtime::operators::source::source_operator::{SourceConfig, SourceOperator};
use crate::runtime::operators::window::window_operator::{WindowOperatorConfig, WindowOperator};
use crate::runtime::operators::window::window_request_operator::WindowRequestOperatorConfig;
use crate::runtime::operators::window::WindowRequestOperator;
use crate::runtime::runtime_context::RuntimeContext;
use crate::common::message::Message;
use anyhow::Result;
use std::fmt;
use std::pin::Pin;
use crate::runtime::functions::{
    function_trait::FunctionTrait,
    map::MapFunction,
    key_by::{KeyByFunction},
    reduce::{ReduceFunction, AggregationResultExtractor},
};

pub type MessageStream = Pin<Box<dyn Stream<Item = Message> + Send + Sync>>;

#[derive(Debug, Clone)]
pub enum OperatorPollResult {
    Ready(Message),
    Continue,    
    None
}

impl OperatorPollResult {
    pub fn get_result_message(self) -> Message {
        match self {
            OperatorPollResult::Ready(msg) => msg,
            OperatorPollResult::Continue => panic!("OperatorPollResult is Continue, expected Ready"),
            OperatorPollResult::None => panic!("OperatorPollResult is None, expected Ready"),
        }
    }
}

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
    fn operator_type(&self) -> OperatorType;

    async fn poll_next(&mut self) -> OperatorPollResult {
        panic!("poll_next not implemented for this operator")
    }

    fn set_input(&mut self, _input: Option<MessageStream>) {
        panic!("set_input not implemented for this operator")
    }

    async fn checkpoint(&mut self, _checkpoint_id: u64) -> Result<Vec<(String, Vec<u8>)>> {
        Ok(vec![])
    }

    async fn restore(&mut self, _blobs: &[(String, Vec<u8>)]) -> Result<()> {
        Ok(())
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
    Aggregate(AggregateOperator),
    Window(WindowOperator),
    WindowRequest(WindowRequestOperator),
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
    AggregateConfig(AggregateConfig),
    WindowConfig(WindowOperatorConfig),
    WindowRequestConfig(WindowRequestOperatorConfig),
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
            OperatorConfig::AggregateConfig(_) => write!(f, "Aggregate"),
            OperatorConfig::WindowConfig(_) => write!(f, "Window"),
            OperatorConfig::WindowRequestConfig(_) => write!(f, "WindowRequest"),
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
            Operator::Aggregate(op) => op.open(context).await,
            Operator::Window(op) => op.open(context).await,
            Operator::WindowRequest(op) => op.open(context).await,
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
            Operator::Aggregate(op) => op.close().await,
            Operator::Window(op) => op.close().await,
            Operator::WindowRequest(op) => op.close().await,
            Operator::Chained(op) => op.close().await
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
            Operator::Aggregate(op) => op.operator_type(),
            Operator::Window(op) => op.operator_type(),
            Operator::WindowRequest(op) => op.operator_type(),
            Operator::Chained(op) => op.operator_type(),
        }
    }
    
    fn set_input(&mut self, input: Option<MessageStream>) {
        match self {
            Operator::Map(op) => op.set_input(input),
            Operator::Join(op) => op.set_input(input),
            Operator::Sink(op) => op.set_input(input),
            Operator::Source(op) => op.set_input(input),
            Operator::KeyBy(op) => op.set_input(input),
            Operator::Reduce(op) => op.set_input(input),
            Operator::Aggregate(op) => op.set_input(input),
            Operator::Window(op) => op.set_input(input),
            Operator::WindowRequest(op) => op.set_input(input),
            Operator::Chained(op) => op.set_input(input),
        }
    }
    
    async fn poll_next(&mut self) -> OperatorPollResult {
        match self {
            Operator::Map(op) => op.poll_next().await,
            Operator::Join(op) => op.poll_next().await,
            Operator::Sink(op) => op.poll_next().await,
            Operator::Source(op) => op.poll_next().await,
            Operator::KeyBy(op) => op.poll_next().await,
            Operator::Reduce(op) => op.poll_next().await,
            Operator::Aggregate(op) => op.poll_next().await,
            Operator::Window(op) => op.poll_next().await,
            Operator::WindowRequest(op) => op.poll_next().await,
            Operator::Chained(op) => op.poll_next().await,
        }
    }
}


pub struct OperatorBase {
    pub runtime_context: Option<RuntimeContext>,
    pub function: Option<Box<dyn FunctionTrait>>,
    pub operator_config: OperatorConfig,
    pub input: Option<MessageStream>,
    pub pending_messages: Vec<Message>,
}

impl fmt::Debug for OperatorBase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OperatorBase")
            .field("runtime_context", &self.runtime_context)
            .field("function", &self.function)
            .field("operator_config", &self.operator_config)
            .field("input", &"<MessageStream>")
            .field("pending_messages", &self.pending_messages)
            .finish()
    }
}

impl OperatorBase {
    pub fn new(operator_config: OperatorConfig) -> Self {
        Self {
            runtime_context: None,
            function: None,
            operator_config,
            input: None,
            pending_messages: Vec::new(),
        }
    }
    
    pub fn new_with_function<F: FunctionTrait + 'static>(function: F, operator_config: OperatorConfig) -> Self {
        Self {
            runtime_context: None,
            function: Some(Box::new(function)),
            operator_config,
            input: None,
            pending_messages: Vec::new(),
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
    
    fn set_input(&mut self, input: Option<MessageStream>) {
        self.input = input;
    }
    
    async fn poll_next(&mut self) -> OperatorPollResult {
        // OperatorBase is not a real stream operator, conform to OperatorTrait
        panic!("poll_next not implemented for OperatorBase");
    }
}


pub fn create_operator(
    operator_config: OperatorConfig
) -> Operator {
    let operator = match operator_config {
        OperatorConfig::MapConfig(_) => Operator::Map(MapOperator::new(operator_config)),
        OperatorConfig::JoinConfig(_) => Operator::Join(JoinOperator::new(operator_config)),
        OperatorConfig::SinkConfig(_) => Operator::Sink(SinkOperator::new(operator_config)),
        OperatorConfig::SourceConfig(_) => Operator::Source(SourceOperator::new(operator_config)),
        OperatorConfig::KeyByConfig(_) => Operator::KeyBy(KeyByOperator::new(operator_config)),
        OperatorConfig::ReduceConfig(_, _) => Operator::Reduce(ReduceOperator::new(operator_config)),
        OperatorConfig::AggregateConfig(_) => Operator::Aggregate(AggregateOperator::new(operator_config)),
        OperatorConfig::WindowConfig(_) => Operator::Window(WindowOperator::new(operator_config)),
        OperatorConfig::WindowRequestConfig(_) => Operator::WindowRequest(WindowRequestOperator::new(operator_config)),
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
        OperatorConfig::ReduceConfig(_, _) |
        OperatorConfig::AggregateConfig(_) |
        OperatorConfig::WindowConfig(_) |
        OperatorConfig::WindowRequestConfig(_) => {
            OperatorType::Processor
        }
    }
}