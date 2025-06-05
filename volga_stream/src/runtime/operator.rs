use async_trait::async_trait;
use crate::runtime::runtime_context::RuntimeContext;
use crate::common::message::{Message, WatermarkMessage};
use crate::common::Key;
use anyhow::Result;
use tokio_rayon::rayon::{ThreadPool, ThreadPoolBuilder};
use tokio_rayon::AsyncThreadPool;
use std::fmt;
use crate::runtime::execution_graph::{SourceConfig, SinkConfig};
use crate::runtime::functions::{
    function_trait::FunctionTrait,
    source::{SourceFunction, SourceFunctionTrait, create_source_function},
    sink::{SinkFunction, SinkFunctionTrait},
    sink::sink_function::create_sink_function,
    map::MapFunction,
    key_by::{KeyByFunction, KeyByFunctionTrait},
    reduce::{ReduceFunction, ReduceFunctionTrait, Accumulator, AggregationResultExtractor, AggregationResultExtractorTrait}
};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorType {
    SOURCE,
    PROCESSOR,
}

#[async_trait]
pub trait OperatorTrait: Send + Sync + fmt::Debug {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
    async fn finish(&mut self) -> Result<()>;
    async fn process_message(&mut self, message: Message) -> Result<Option<Vec<Message>>> {
        Err(anyhow::anyhow!("process_message not implemented for this operator"))
    }
    fn operator_type(&self) -> OperatorType;
    async fn fetch(&mut self) -> Result<Option<Message>> {
        Err(anyhow::anyhow!("fetch not implemented for this operator"))
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
        }
    }

    async fn finish(&mut self) -> Result<()> {
        match self {
            Operator::Map(op) => op.finish().await,
            Operator::Join(op) => op.finish().await,
            Operator::Sink(op) => op.finish().await,
            Operator::Source(op) => op.finish().await,
            Operator::KeyBy(op) => op.finish().await,
            Operator::Reduce(op) => op.finish().await,
        }
    }

    async fn process_message(&mut self, message: Message) -> Result<Option<Vec<Message>>> {
        match &message {
            Message::Watermark(watermark) => {
                // Update watermark for the input channel
                let channel_id = watermark.source_vertex_id.clone();
                let watermark_value = watermark.watermark_value;
                
                match self {
                    Operator::Map(op) => op.base.update_watermark(&channel_id, watermark_value)?,
                    Operator::Join(op) => op.base.update_watermark(&channel_id, watermark_value)?,
                    Operator::Sink(op) => op.base.update_watermark(&channel_id, watermark_value)?,
                    Operator::Source(op) => op.base.update_watermark(&channel_id, watermark_value)?,
                    Operator::KeyBy(op) => op.base.update_watermark(&channel_id, watermark_value)?,
                    Operator::Reduce(op) => op.base.update_watermark(&channel_id, watermark_value)?,
                }
                
                // Forward the watermark to downstream operators if it's new
                let current_watermark = match self {
                    Operator::Map(op) => op.base.get_current_watermark(),
                    Operator::Join(op) => op.base.get_current_watermark(),
                    Operator::Sink(op) => op.base.get_current_watermark(),
                    Operator::Source(op) => op.base.get_current_watermark(),
                    Operator::KeyBy(op) => op.base.get_current_watermark(),
                    Operator::Reduce(op) => op.base.get_current_watermark(),
                };

                let last_forwarded = match self {
                    Operator::Map(op) => op.base.last_forwarded_watermark.load(Ordering::SeqCst),
                    Operator::Join(op) => op.base.last_forwarded_watermark.load(Ordering::SeqCst),
                    Operator::Sink(op) => op.base.last_forwarded_watermark.load(Ordering::SeqCst),
                    Operator::Source(op) => op.base.last_forwarded_watermark.load(Ordering::SeqCst),
                    Operator::KeyBy(op) => op.base.last_forwarded_watermark.load(Ordering::SeqCst),
                    Operator::Reduce(op) => op.base.last_forwarded_watermark.load(Ordering::SeqCst),
                };

                if current_watermark > last_forwarded {
                    match self {
                        Operator::Map(op) => op.base.last_forwarded_watermark.store(current_watermark, Ordering::SeqCst),
                        Operator::Join(op) => op.base.last_forwarded_watermark.store(current_watermark, Ordering::SeqCst),
                        Operator::Sink(op) => op.base.last_forwarded_watermark.store(current_watermark, Ordering::SeqCst),
                        Operator::Source(op) => op.base.last_forwarded_watermark.store(current_watermark, Ordering::SeqCst),
                        Operator::KeyBy(op) => op.base.last_forwarded_watermark.store(current_watermark, Ordering::SeqCst),
                        Operator::Reduce(op) => op.base.last_forwarded_watermark.store(current_watermark, Ordering::SeqCst),
                    }

                    let runtime_context = match self {
                        Operator::Map(op) => op.base.runtime_context.as_ref().unwrap(),
                        Operator::Join(op) => op.base.runtime_context.as_ref().unwrap(),
                        Operator::Sink(op) => op.base.runtime_context.as_ref().unwrap(),
                        Operator::Source(op) => op.base.runtime_context.as_ref().unwrap(),
                        Operator::KeyBy(op) => op.base.runtime_context.as_ref().unwrap(),
                        Operator::Reduce(op) => op.base.runtime_context.as_ref().unwrap(),
                    };

                    let watermark = WatermarkMessage {
                        source_vertex_id: runtime_context.vertex_id().to_string(),
                        watermark_value: current_watermark,
                    };
                    Ok(Some(vec![Message::Watermark(watermark)]))
                } else {
                    Ok(None)
                }
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
        }
    }

    async fn fetch(&mut self) -> Result<Option<Message>> {
        match self {
            Operator::Map(op) => op.fetch().await,
            Operator::Join(op) => op.fetch().await,
            Operator::Sink(op) => op.fetch().await,
            Operator::Source(op) => op.fetch().await,
            Operator::KeyBy(op) => op.fetch().await,
            Operator::Reduce(op) => op.fetch().await,
        }
    }
}

#[derive(Debug)]
pub struct OperatorBase {
    runtime_context: Option<RuntimeContext>,
    function: Option<Box<dyn FunctionTrait>>,
    thread_pool: ThreadPool,
    // Track watermarks from each input channel
    input_watermarks: Arc<Mutex<HashMap<String, u64>>>,
    // Current watermark is the minimum of all input watermarks
    current_watermark: AtomicU64,
    last_forwarded_watermark: AtomicU64,
}

impl OperatorBase {
    pub fn new() -> Self {
        let thread_pool = ThreadPoolBuilder::new()
            .num_threads(std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
            .build()
            .expect("Failed to create thread pool");
        Self {
            runtime_context: None,
            function: None,
            thread_pool,
            input_watermarks: Arc::new(Mutex::new(HashMap::new())),
            current_watermark: AtomicU64::new(0),
            last_forwarded_watermark: AtomicU64::new(0),
        }
    }
    
    pub fn new_with_function<F: FunctionTrait + 'static>(function: F) -> Self {
        let thread_pool = ThreadPoolBuilder::new()
            .num_threads(std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
            .build()
            .expect("Failed to create thread pool");
        Self {
            runtime_context: None,
            function: Some(Box::new(function)),
            thread_pool,
            input_watermarks: Arc::new(Mutex::new(HashMap::new())),
            current_watermark: AtomicU64::new(0),
            last_forwarded_watermark: AtomicU64::new(0),
        }
    }
    
    /// Get a reference to the function as a specific type
    pub fn get_function<T: 'static>(&self) -> Option<&T> {
        self.function.as_ref()
            .and_then(|f| f.as_any().downcast_ref::<T>())
    }
    
    /// Get a mutable reference to the function as a specific type
    pub fn get_function_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.function.as_mut()
            .and_then(|f| f.as_any_mut().downcast_mut::<T>())
    }

    pub fn update_watermark(&self, channel_id: &str, watermark: u64) -> Result<()> {
        let mut watermarks = self.input_watermarks.lock().unwrap();
        watermarks.insert(channel_id.to_string(), watermark);
        
        // Update current watermark to be the minimum of all input watermarks
        let min_watermark = watermarks.values().min().copied().unwrap_or(0);
        self.current_watermark.store(min_watermark, Ordering::SeqCst);
        
        Ok(())
    }

    pub fn get_current_watermark(&self) -> u64 {
        self.current_watermark.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl OperatorTrait for OperatorBase {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.runtime_context = Some(context.clone());
        
        // If we have a function, open it
        if let Some(function) = &mut self.function {
            function.open(context).await?;
        }
        
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        // If we have a function, close it
        if let Some(function) = &mut self.function {
            function.close().await?;
        }
        
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        // If we have a function, finish it
        if let Some(function) = &mut self.function {
            function.finish().await?;
        }
        
        Ok(())
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }
}

#[derive(Debug)]
pub struct MapOperator {
    base: OperatorBase,
}

impl MapOperator {
    pub fn new(map_function: MapFunction) -> Self {
        Self { 
            base: OperatorBase::new_with_function(map_function),
        }
    }
}

#[async_trait]
impl OperatorTrait for MapOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn process_message(&mut self, message: Message) -> Result<Option<Vec<Message>>> {
        if let Some(function) = self.base.get_function_mut::<MapFunction>() {
            let function = function.clone();
            let message = message.clone();

            self.base.thread_pool.spawn_fifo_async(move || {
                let processed = function.map(message).unwrap();
                Ok(Some(vec![processed]))
            }).await
        } else {
            Err(anyhow::anyhow!("MapFunction not available"))
        }
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    async fn finish(&mut self) -> Result<()> {
        self.base.finish().await
    }
}

#[derive(Debug)]
pub struct JoinOperator {
    base: OperatorBase,
    left_buffer: Vec<Message>,
    right_buffer: Vec<Message>,
}

impl JoinOperator {
    pub fn new() -> Self {
        Self {
            base: OperatorBase::new(),
            left_buffer: Vec::new(),
            right_buffer: Vec::new(),
        }
    }
}

#[async_trait]
impl OperatorTrait for JoinOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    async fn finish(&mut self) -> Result<()> {
        self.base.finish().await
    }

    async fn process_message(&mut self, message: Message) -> Result<Option<Vec<Message>>> {
        // TODO proper lookup for upstream_vertex_id position (left or right)
        if let Some(upstream_id) = message.upstream_vertex_id() {
            if upstream_id.contains("left") {
                self.left_buffer.push(message.clone());
            } else {
                self.right_buffer.push(message.clone());
            }
        }
        Ok(Some(vec![message]))
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }
}

#[derive(Debug)]
pub struct SinkOperator {
    base: OperatorBase,
}

impl SinkOperator {
    pub fn new(config: SinkConfig) -> Self {
        let sink_function = create_sink_function(config);
        Self {
            base: OperatorBase::new_with_function(sink_function),
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

    async fn finish(&mut self) -> Result<()> {
        self.base.finish().await
    }

    async fn process_message(&mut self, message: Message) -> Result<Option<Vec<Message>>> {
        // println!("SinkOperator process_message {:?}", message);
        if let Some(function) = self.base.get_function_mut::<SinkFunction>() {
            function.sink(message.clone()).await?;
            Ok(Some(vec![message]))
            // Ok(None)
        } else {
            Err(anyhow::anyhow!("SinkFunction not available"))
        }
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }
}

#[derive(Debug)]
pub struct SourceOperator {
    base: OperatorBase,
}

impl SourceOperator {
    pub fn new(config: SourceConfig) -> Self {
        let source_function = create_source_function(config);
        Self {
            base: OperatorBase::new_with_function(source_function),
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

    async fn finish(&mut self) -> Result<()> {
        self.base.finish().await
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::SOURCE
    }

    async fn fetch(&mut self) -> Result<Option<Message>> {
        if let Some(function) = self.base.get_function_mut::<SourceFunction>() {
            function.fetch().await
        } else {
            Err(anyhow::anyhow!("SourceFunction not available"))
        }
    }
}

#[derive(Debug)]
pub struct KeyByOperator {
    base: OperatorBase,
}

impl KeyByOperator {
    pub fn new(key_by_function: KeyByFunction) -> Self {
        Self { 
            base: OperatorBase::new_with_function(key_by_function),
        }
    }
}

#[async_trait]
impl OperatorTrait for KeyByOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn process_message(&mut self, message: Message) -> Result<Option<Vec<Message>>> {
        if let Some(function) = self.base.get_function_mut::<KeyByFunction>() {
            let function = function.clone();
            let message = message.clone();

            self.base.thread_pool.spawn_fifo_async(move || {
                let keyed_messages = function.key_by(message)?;
                // Convert
                let messages = keyed_messages.into_iter()
                    .map(Message::Keyed)
                    .collect();
                Ok(Some(messages))
            }).await
        } else {
            Err(anyhow::anyhow!("KeyByFunction not available"))
        }
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    async fn finish(&mut self) -> Result<()> {
        self.base.finish().await
    }
}

#[derive(Debug)]
pub struct ReduceOperator {
    base: OperatorBase,
    accumulators: HashMap<Key, Arc<Mutex<Accumulator>>>,
    result_extractor: AggregationResultExtractor,
}

impl ReduceOperator {
    pub fn new(reduce_function: ReduceFunction, extractor: Option<AggregationResultExtractor>) -> Self {
        Self {
            base: OperatorBase::new_with_function(reduce_function),
            accumulators: HashMap::new(),
            result_extractor: extractor.unwrap_or_else(AggregationResultExtractor::all_aggregations),
        }
    }
}

#[async_trait]
impl OperatorTrait for ReduceOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    async fn finish(&mut self) -> Result<()> {
        self.base.finish().await
    }

    async fn process_message(&mut self, message: Message) -> Result<Option<Vec<Message>>> {
        println!("ReduceOperator process_message {:?}", message);
        
        // Explicitly check and handle only KeyedMessage
        match message {
            Message::Keyed(keyed_message) => {
                let key = keyed_message.key().clone();
                
                // Check if we need to create a new accumulator or update an existing one
                let acc_exists = self.accumulators.contains_key(&key);
                
                let function = self.base.get_function_mut::<ReduceFunction>()
                    .ok_or_else(|| anyhow::anyhow!("ReduceFunction not available"))?;

                if !acc_exists {
                    // Create a new accumulator
                    self.accumulators.insert(key.clone(), Arc::new(Mutex::new(function.create_accumulator()?)));
                }
                
                // Now that we've updated the accumulator, get the result
                let acc = self.accumulators.get_mut(&key)
                    .ok_or_else(|| anyhow::anyhow!("Accumulator not found"))?;
                
                let keyed_message = keyed_message.clone();
                let function = function.clone();
                let result_extractor = self.result_extractor.clone();
                let acc = acc.clone();
                
                let result = self.base.thread_pool.spawn_fifo_async(move || {
                    let mut acc = acc.lock().unwrap();
                    function.update_accumulator(&mut acc, &keyed_message)?;
                    let agg_result = function.get_result(&acc)?;
                    let result_message = result_extractor.extract_result(&key, &agg_result)?;
                    Ok::<Option<Vec<Message>>, anyhow::Error>(Some(vec![result_message]))
                }).await?;

                return Ok(result);
            },
            _ => {
                Err(anyhow::anyhow!("ReduceOperator requires KeyedMessage input"))
            }
        }
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }
}
