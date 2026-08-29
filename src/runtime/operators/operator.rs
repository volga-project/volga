use async_trait::async_trait;
use futures::stream::Peekable;
use futures::{FutureExt, Stream, StreamExt};

use crate::runtime::checkpoint::{SerializedCheckpoint, SerializedRestore};
use crate::runtime::functions::join::join_function::JoinFunction;
use crate::runtime::operators::aggregate::aggregate_operator::{AggregateConfig, AggregateOperator};
use crate::runtime::operators::join::join_operator::JoinOperator;
use crate::runtime::operators::key_by::key_by_operator::KeyByOperator;
use crate::runtime::operators::map::map_operator::MapOperator;
use crate::runtime::operators::sink::sink_operator::{SinkConfig, SinkOperator};
use crate::runtime::operators::source::source_operator::{SourceConfig, SourceOperator as SourceOp};
use crate::runtime::operators::window::operator::{WindowOperator, WindowOperatorConfig};
use crate::runtime::operators::window::request::WindowRequestOperatorConfig;
use crate::runtime::operators::window::WindowRequestOperator;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::functions::source::FetchResult;
use crate::runtime::operators::source::SourceInterrupt;
use crate::common::message::{CheckpointBarrierMessage, Message, WatermarkMessage};
use anyhow::Result;
use std::fmt;
use std::pin::Pin;
use crate::runtime::functions::{
    function_trait::FunctionTrait,
    map::MapFunction,
    key_by::{KeyByFunction},
};

pub type MessageStream = Pin<Box<dyn Stream<Item = Message> + Send + Sync>>;

/// Result of [`drain_ready_inputs`].
///
/// A following control or over-budget data message is left peeked on the stream.
#[derive(Debug)]
pub enum NextInputs {
    Exhausted,
    /// Watermark or checkpoint barrier.
    Control(Message),
    /// One or more data messages.
    Data(Vec<Message>),
}

/// Execution role in the topology (not the logical operator kind).
///
/// For Window / Join / Map / … see [`crate::runtime::operators::OperatorKind`]
/// via [`OperatorConfig::kind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorType {
    Source,
    Sink,
    Processor,
}

/// Cap for one processor ingest (`drain_ready_inputs`).
pub(crate) const INGEST_MAX_RECORDS: usize = 64 * 1024;

#[async_trait]
pub trait Output: Send {
    async fn emit(&mut self, msg: Message) -> Result<()>;
}

/// Collects operator output for unit tests.
#[derive(Debug, Default)]
pub struct VecOutput {
    pub messages: Vec<Message>,
}

#[async_trait]
impl Output for VecOutput {
    async fn emit(&mut self, msg: Message) -> Result<()> {
        self.messages.push(msg);
        Ok(())
    }
}

/// Lifecycle shared by sources and stream operators.
#[async_trait]
pub trait OperatorTrait: Send + Sync + fmt::Debug {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
    fn operator_type(&self) -> OperatorType;
    fn operator_config(&self) -> &OperatorConfig;

    async fn checkpoint(&mut self, _checkpoint_id: u64) -> Result<SerializedCheckpoint> {
        Ok(SerializedCheckpoint::new(Vec::new()))
    }

    async fn restore(&mut self, _restore: SerializedRestore) -> Result<()> {
        Ok(())
    }
}

/// Processor / sink: the task owns the mailbox and dispatches events.
#[async_trait]
pub trait StreamOperator: OperatorTrait {
    async fn process_data(&mut self, data: Vec<Message>, out: &mut dyn Output) -> Result<()>;

    async fn handle_watermark(&mut self, wm: WatermarkMessage, out: &mut dyn Output) -> Result<()> {
        out.emit(Message::Watermark(wm)).await
    }

    async fn handle_barrier(
        &mut self,
        barrier: CheckpointBarrierMessage,
        out: &mut dyn Output,
    ) -> Result<()> {
        out.emit(Message::CheckpointBarrier(barrier)).await
    }
}

/// Source: the task pulls via [`fetch_next`]; no mailbox.
#[async_trait]
pub trait SourceOperator: OperatorTrait {
    async fn fetch_next(&mut self, interrupt: Option<&SourceInterrupt>) -> FetchResult;
}

pub fn operator_config_requires_checkpoint(operator_config: &OperatorConfig) -> bool {
    match operator_config {
        OperatorConfig::WindowConfig(_) => true,
        OperatorConfig::SourceConfig(source_cfg) => {
            match source_cfg {
                // Only replayable datagen participates in checkpointing.
                SourceConfig::DatagenSourceConfig(cfg) => cfg.spec.replayable,
                SourceConfig::KafkaSourceConfig(_) => true,
                _ => false,
            }
        }
        _ => false,
    }
}

#[derive(Debug)]
pub enum Operator {
    Map(MapOperator),
    Join(JoinOperator),
    Sink(SinkOperator),
    Source(SourceOp),
    KeyBy(KeyByOperator),
    Aggregate(AggregateOperator),
    Window(WindowOperator),
    WindowRequest(WindowRequestOperator),
}

impl Operator {
    fn as_stream_mut(&mut self) -> &mut dyn StreamOperator {
        match self {
            Operator::Map(op) => op,
            Operator::Join(op) => op,
            Operator::Sink(op) => op,
            Operator::KeyBy(op) => op,
            Operator::Aggregate(op) => op,
            Operator::Window(op) => op,
            Operator::WindowRequest(op) => op,
            Operator::Source(_) => {
                panic!("Source operators do not implement StreamOperator; use fetch_next")
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum OperatorConfig {
    MapConfig(MapFunction),
    JoinConfig(JoinFunction),
    SinkConfig(SinkConfig),
    SourceConfig(SourceConfig),
    KeyByConfig(KeyByFunction),
    AggregateConfig(AggregateConfig),
    WindowConfig(WindowOperatorConfig),
    WindowRequestConfig(WindowRequestOperatorConfig),
}

impl fmt::Display for OperatorConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OperatorConfig::MapConfig(map_func) => write!(f, "Map({})", map_func),
            OperatorConfig::JoinConfig(join_func) => write!(f, "Join({})", join_func),
            OperatorConfig::SinkConfig(sink_config) => write!(f, "Sink({})", sink_config),
            OperatorConfig::SourceConfig(source_config) => write!(f, "Source({})", source_config),
            OperatorConfig::KeyByConfig(key_by_func) => write!(f, "KeyBy({})", key_by_func),
            OperatorConfig::AggregateConfig(_) => write!(f, "Aggregate"),
            OperatorConfig::WindowConfig(_) => write!(f, "Window"),
            OperatorConfig::WindowRequestConfig(_) => write!(f, "WindowRequest"),
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
            Operator::Aggregate(op) => op.open(context).await,
            Operator::Window(op) => op.open(context).await,
            Operator::WindowRequest(op) => op.open(context).await,
        }
    }

    async fn close(&mut self) -> Result<()> {
        match self {
            Operator::Map(op) => op.close().await,
            Operator::Join(op) => op.close().await,
            Operator::Sink(op) => op.close().await,
            Operator::Source(op) => op.close().await,
            Operator::KeyBy(op) => op.close().await,
            Operator::Aggregate(op) => op.close().await,
            Operator::Window(op) => op.close().await,
            Operator::WindowRequest(op) => op.close().await,
        }
    }

    fn operator_type(&self) -> OperatorType {
        match self {
            Operator::Map(op) => op.operator_type(),
            Operator::Join(op) => op.operator_type(),
            Operator::Sink(op) => op.operator_type(),
            Operator::Source(op) => op.operator_type(),
            Operator::KeyBy(op) => op.operator_type(),
            Operator::Aggregate(op) => op.operator_type(),
            Operator::Window(op) => op.operator_type(),
            Operator::WindowRequest(op) => op.operator_type(),
        }
    }

    fn operator_config(&self) -> &OperatorConfig {
        match self {
            Operator::Map(op) => op.operator_config(),
            Operator::Join(op) => op.operator_config(),
            Operator::Sink(op) => op.operator_config(),
            Operator::Source(op) => op.operator_config(),
            Operator::KeyBy(op) => op.operator_config(),
            Operator::Aggregate(op) => op.operator_config(),
            Operator::Window(op) => op.operator_config(),
            Operator::WindowRequest(op) => op.operator_config(),
        }
    }

    async fn checkpoint(&mut self, checkpoint_id: u64) -> Result<SerializedCheckpoint> {
        match self {
            Operator::Map(op) => op.checkpoint(checkpoint_id).await,
            Operator::Join(op) => op.checkpoint(checkpoint_id).await,
            Operator::Sink(op) => op.checkpoint(checkpoint_id).await,
            Operator::Source(op) => op.checkpoint(checkpoint_id).await,
            Operator::KeyBy(op) => op.checkpoint(checkpoint_id).await,
            Operator::Aggregate(op) => op.checkpoint(checkpoint_id).await,
            Operator::Window(op) => op.checkpoint(checkpoint_id).await,
            Operator::WindowRequest(op) => op.checkpoint(checkpoint_id).await,
        }
    }

    async fn restore(&mut self, restore: SerializedRestore) -> Result<()> {
        match self {
            Operator::Map(op) => op.restore(restore).await,
            Operator::Join(op) => op.restore(restore).await,
            Operator::Sink(op) => op.restore(restore).await,
            Operator::Source(op) => op.restore(restore).await,
            Operator::KeyBy(op) => op.restore(restore).await,
            Operator::Aggregate(op) => op.restore(restore).await,
            Operator::Window(op) => op.restore(restore).await,
            Operator::WindowRequest(op) => op.restore(restore).await,
        }
    }
}

#[async_trait]
impl StreamOperator for Operator {
    async fn process_data(&mut self, data: Vec<Message>, out: &mut dyn Output) -> Result<()> {
        self.as_stream_mut().process_data(data, out).await
    }

    async fn handle_watermark(&mut self, wm: WatermarkMessage, out: &mut dyn Output) -> Result<()> {
        self.as_stream_mut().handle_watermark(wm, out).await
    }

    async fn handle_barrier(
        &mut self,
        barrier: CheckpointBarrierMessage,
        out: &mut dyn Output,
    ) -> Result<()> {
        self.as_stream_mut().handle_barrier(barrier, out).await
    }
}

#[derive(Debug)]
pub struct OperatorBase {
    pub runtime_context: Option<RuntimeContext>,
    pub function: Option<Box<dyn FunctionTrait>>,
    pub operator_config: OperatorConfig,
}

impl OperatorBase {
    pub fn new(operator_config: OperatorConfig) -> Self {
        Self {
            runtime_context: None,
            function: None,
            operator_config,
        }
    }

    pub fn new_with_function<F: FunctionTrait + 'static>(function: F, operator_config: OperatorConfig) -> Self {
        Self {
            runtime_context: None,
            function: Some(Box::new(function)),
            operator_config,
        }
    }

    pub fn get_function_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.function.as_mut()
            .and_then(|f| f.as_any_mut().downcast_mut::<T>())
    }

    pub fn operator_config(&self) -> &OperatorConfig {
        &self.operator_config
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

    fn operator_config(&self) -> &OperatorConfig {
        &self.operator_config
    }
}


pub fn create_operator(operator_config: OperatorConfig) -> Operator {
    match operator_config {
        OperatorConfig::MapConfig(_) => Operator::Map(MapOperator::new(operator_config)),
        OperatorConfig::JoinConfig(_) => Operator::Join(JoinOperator::new(operator_config)),
        OperatorConfig::SinkConfig(_) => Operator::Sink(SinkOperator::new(operator_config)),
        OperatorConfig::SourceConfig(_) => Operator::Source(SourceOp::new(operator_config)),
        OperatorConfig::KeyByConfig(_) => Operator::KeyBy(KeyByOperator::new(operator_config)),
        OperatorConfig::AggregateConfig(_) => Operator::Aggregate(AggregateOperator::new(operator_config)),
        OperatorConfig::WindowConfig(_) => Operator::Window(WindowOperator::new(operator_config)),
        OperatorConfig::WindowRequestConfig(_) => {
            Operator::WindowRequest(WindowRequestOperator::new(operator_config))
        }
    }
}

pub fn get_operator_type_from_config(operator_config: &OperatorConfig) -> OperatorType {
    match operator_config {
        OperatorConfig::SourceConfig(_) => OperatorType::Source,
        OperatorConfig::SinkConfig(_) => OperatorType::Sink,
        OperatorConfig::MapConfig(_) |
        OperatorConfig::JoinConfig(_) |
        OperatorConfig::KeyByConfig(_) |
        OperatorConfig::AggregateConfig(_) |
        OperatorConfig::WindowConfig(_) |
        OperatorConfig::WindowRequestConfig(_) => OperatorType::Processor,
    }
}

/// Pull ready data messages until `max_records` or a control / empty / pending stop.
pub(crate) async fn drain_ready_inputs<S>(input: &mut Peekable<S>, max_records: usize) -> NextInputs
where
    S: Stream<Item = Message> + Unpin,
{
    let Some(first) = input.next().await else {
        return NextInputs::Exhausted;
    };
    if first.is_control() {
        return NextInputs::Control(first);
    }

    let mut data = vec![first];
    let mut rows = data_rows(&data[0]);

    loop {
        let n = match Pin::new(&mut *input).peek().now_or_never() {
            None | Some(None) => break,
            Some(Some(msg)) if msg.is_control() => break,
            Some(Some(msg)) => {
                let n = data_rows(msg);
                if rows.saturating_add(n) > max_records {
                    break;
                }
                n
            }
        };
        let msg = input.next().await.expect("peeked item");
        rows += n;
        data.push(msg);
    }
    NextInputs::Data(data)
}

fn data_rows(msg: &Message) -> usize {
    match msg {
        Message::Regular(m) => m.record_batch.num_rows(),
        Message::Watermark(_) | Message::CheckpointBarrier(_) => 0,
    }
}