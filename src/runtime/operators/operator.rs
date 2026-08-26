use async_trait::async_trait;
use futures::stream::Peekable;
use futures::{FutureExt, Stream, StreamExt};

use crate::runtime::checkpoint::{SerializedCheckpoint, SerializedRestore};
use crate::runtime::functions::join::join_function::JoinFunction;
use crate::runtime::operators::aggregate::aggregate_operator::{AggregateConfig, AggregateOperator};
use crate::runtime::operators::chained::chained_operator::ChainedOperator;
use crate::runtime::operators::join::join_operator::JoinOperator;
use crate::runtime::operators::key_by::key_by_operator::KeyByOperator;
use crate::runtime::operators::map::map_operator::MapOperator;
use crate::runtime::operators::sink::sink_operator::{SinkConfig, SinkOperator};
use crate::runtime::operators::source::source_operator::{SourceConfig, SourceOperator};
use crate::runtime::operators::window::operator::{WindowOperator, WindowOperatorConfig};
use crate::runtime::operators::window::request::WindowRequestOperatorConfig;
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

/// Result of [`OperatorBase::next_inputs`].
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
    ChainedSourceSink,
}

#[async_trait]
pub trait OperatorTrait: Send + Sync + fmt::Debug {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
    fn operator_type(&self) -> OperatorType;
    fn operator_config(&self) -> &OperatorConfig;

    async fn poll_next(&mut self) -> OperatorPollResult {
        panic!("poll_next not implemented for this operator")
    }

    fn set_input(&mut self, _input: Option<MessageStream>) {
        panic!("set_input not implemented for this operator")
    }

    async fn checkpoint(&mut self, _checkpoint_id: u64) -> Result<SerializedCheckpoint> {
        Ok(SerializedCheckpoint::new(Vec::new()))
    }

    async fn restore(&mut self, _restore: SerializedRestore) -> Result<()> {
        Ok(())
    }
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
        OperatorConfig::ChainedConfig(configs) => configs.iter().any(operator_config_requires_checkpoint),
        _ => false,
    }
}

#[derive(Debug)]
pub enum Operator {
    Map(MapOperator),
    Join(JoinOperator),
    Sink(SinkOperator),
    Source(SourceOperator),
    KeyBy(KeyByOperator),
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
            Operator::Aggregate(op) => op.operator_type(),
            Operator::Window(op) => op.operator_type(),
            Operator::WindowRequest(op) => op.operator_type(),
            Operator::Chained(op) => op.operator_type(),
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
            Operator::Chained(op) => op.operator_config(),
        }
    }
    
    fn set_input(&mut self, input: Option<MessageStream>) {
        match self {
            Operator::Map(op) => op.set_input(input),
            Operator::Join(op) => op.set_input(input),
            Operator::Sink(op) => op.set_input(input),
            Operator::Source(op) => op.set_input(input),
            Operator::KeyBy(op) => op.set_input(input),
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
            Operator::Aggregate(op) => op.poll_next().await,
            Operator::Window(op) => op.poll_next().await,
            Operator::WindowRequest(op) => op.poll_next().await,
            Operator::Chained(op) => op.poll_next().await,
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
            Operator::Chained(op) => op.checkpoint(checkpoint_id).await,
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
            Operator::Chained(op) => op.restore(restore).await,
        }
    }
}


pub struct OperatorBase {
    pub runtime_context: Option<RuntimeContext>,
    pub function: Option<Box<dyn FunctionTrait>>,
    pub operator_config: OperatorConfig,
    pub input: Option<Peekable<MessageStream>>,
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

    pub fn pop_pending_output(&mut self) -> Option<Message> {
        self.pending_messages.pop()
    }

    pub async fn next_input(&mut self) -> Option<Message> {
        let input_stream = self.input.as_mut().expect("input stream not set");
        input_stream.next().await
    }

    /// Wait for the first message, then take more only if already ready.
    ///
    /// `max_records` is a cap, not a fill target. The first data message is
    /// always accepted even if it exceeds the cap. Stops before watermarks /
    /// barriers (left peeked).
    pub async fn next_inputs(&mut self, max_records: usize) -> NextInputs {
        let input = self.input.as_mut().expect("input stream not set");
        drain_ready_inputs(input, max_records).await
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
    
    fn set_input(&mut self, input: Option<MessageStream>) {
        self.input = input.map(|s| s.peekable());
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
        OperatorConfig::AggregateConfig(_) |
        OperatorConfig::WindowConfig(_) |
        OperatorConfig::WindowRequestConfig(_) => {
            OperatorType::Processor
        }
    }
}

/// Pull ready data messages until `max_records` or a control / empty / pending stop.
async fn drain_ready_inputs<S>(input: &mut Peekable<S>, max_records: usize) -> NextInputs
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

#[cfg(test)]
mod next_inputs_tests {
    use super::*;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::{FutureExt, StreamExt, stream};

    use crate::common::message::WatermarkMessage;

    fn int_batch(rows: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(rows))]).unwrap()
    }

    fn data(rows: Vec<i64>) -> Message {
        Message::new(None, int_batch(rows), None, None)
    }

    fn wm(v: u64) -> Message {
        Message::Watermark(WatermarkMessage::new("src".to_string(), v, None))
    }

    fn data_len(out: &NextInputs) -> usize {
        match out {
            NextInputs::Data(msgs) => msgs.len(),
            other => panic!("expected Data, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn empty_stream() {
        let mut stream = stream::empty().peekable();
        let out = drain_ready_inputs(&mut stream, 8).await;
        assert!(matches!(out, NextInputs::Exhausted));
    }

    #[tokio::test]
    async fn does_not_block_waiting_to_fill_budget() {
        let mut stream = stream::iter(vec![data(vec![1])])
            .chain(stream::pending())
            .peekable();
        let out = tokio::time::timeout(
            Duration::from_millis(200),
            drain_ready_inputs(&mut stream, 8),
        )
        .await
        .expect("must not wait on pending input");
        assert_eq!(data_len(&out), 1);
    }

    #[tokio::test]
    async fn stops_before_watermark() {
        let mut stream = stream::iter(vec![data(vec![1]), data(vec![2]), wm(9)]).peekable();
        let out = drain_ready_inputs(&mut stream, 8).await;
        assert_eq!(data_len(&out), 2);
        match Pin::new(&mut stream).peek().now_or_never() {
            Some(Some(Message::Watermark(w))) => assert_eq!(w.watermark_value, 9),
            other => panic!("expected peeked watermark, got {other:?}"),
        }
        let again = drain_ready_inputs(&mut stream, 8).await;
        match again {
            NextInputs::Control(Message::Watermark(w)) => assert_eq!(w.watermark_value, 9),
            other => panic!("next fetch should see the watermark, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn record_limit_leaves_over_budget_message_peeked() {
        let mut stream = stream::iter(vec![
            data(vec![1, 2, 3]),
            data(vec![4, 5, 6]),
        ])
        .peekable();
        let out = drain_ready_inputs(&mut stream, 5).await;
        assert_eq!(data_len(&out), 1);
        assert!(matches!(
            Pin::new(&mut stream).peek().now_or_never(),
            Some(Some(Message::Regular(_)))
        ));
    }

    #[tokio::test]
    async fn first_message_accepted_even_when_over_record_limit() {
        let mut stream =
            stream::iter(vec![data(vec![1, 2, 3]), data(vec![4])]).peekable();
        let out = drain_ready_inputs(&mut stream, 1).await;
        assert_eq!(data_len(&out), 1);
        assert!(matches!(
            Pin::new(&mut stream).peek().now_or_never(),
            Some(Some(Message::Regular(_)))
        ));
    }
}