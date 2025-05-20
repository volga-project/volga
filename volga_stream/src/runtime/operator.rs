use async_trait::async_trait;
use crate::runtime::runtime_context::RuntimeContext;
use crate::common::data_batch::{DataBatch, KeyedDataBatch, BaseDataBatch};
use crate::common::Key;
use anyhow::Result;
use tokio_rayon::rayon::{ThreadPool, ThreadPoolBuilder};
use std::fmt;
use crate::runtime::execution_graph::{SourceConfig, SinkConfig};
use crate::runtime::functions::{
    function_trait::FunctionTrait,
    source::{SourceFunction, SourceFunctionTrait, create_source_function},
    sink::{SinkFunction, SinkFunctionTrait},
    sink::sink_function::create_sink_function,
    map::{MapFunction, MapFunctionTrait},
    key_by::{KeyByFunction, KeyByFunctionTrait},
    reduce::{ReduceFunction, ReduceFunctionTrait, Accumulator, AggregationResultExtractor, AggregationResultExtractorTrait}
};

use std::collections::HashMap;
use std::sync::Arc;
use std::any::Any;

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
    async fn process_batch(&mut self, batch: DataBatch) -> Result<Option<Vec<DataBatch>>> {
        Err(anyhow::anyhow!("process_batch not implemented for this operator"))
    }
    fn operator_type(&self) -> OperatorType;
    async fn fetch(&mut self) -> Result<Option<DataBatch>> {
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

    async fn process_batch(&mut self, batch: DataBatch) -> Result<Option<Vec<DataBatch>>> {
        match self {
            Operator::Map(op) => op.process_batch(batch).await,
            Operator::Join(op) => op.process_batch(batch).await,
            Operator::Sink(op) => op.process_batch(batch).await,
            Operator::Source(op) => op.process_batch(batch).await,
            Operator::KeyBy(op) => op.process_batch(batch).await,
            Operator::Reduce(op) => op.process_batch(batch).await,
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

    async fn fetch(&mut self) -> Result<Option<DataBatch>> {
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
}

impl OperatorBase {
    pub fn new() -> Self {
        Self {
            runtime_context: None,
            function: None,
        }
    }
    
    pub fn new_with_function<F: FunctionTrait + 'static>(function: F) -> Self {
        Self {
            runtime_context: None,
            function: Some(Box::new(function)),
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

    async fn process_batch(&mut self, batch: DataBatch) -> Result<Option<Vec<DataBatch>>> {
        if let Some(function) = self.base.get_function_mut::<MapFunction>() {
            let processed = function.map(batch).await?;
            Ok(Some(vec![processed]))
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
    left_buffer: Vec<DataBatch>,
    right_buffer: Vec<DataBatch>,
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

    async fn process_batch(&mut self, batch: DataBatch) -> Result<Option<Vec<DataBatch>>> {
        // TODO proper lookup for upstream_vertex_id position (left or right)
        if let Some(upstream_id) = batch.upstream_vertex_id() {
            if upstream_id.contains("left") {
                self.left_buffer.push(batch.clone());
            } else {
                self.right_buffer.push(batch.clone());
            }
        }
        Ok(Some(vec![batch]))
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

    async fn process_batch(&mut self, batch: DataBatch) -> Result<Option<Vec<DataBatch>>> {
        if let Some(function) = self.base.get_function_mut::<SinkFunction>() {
            function.sink(batch.clone()).await?;
            Ok(Some(vec![batch]))
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

    async fn fetch(&mut self) -> Result<Option<DataBatch>> {
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

    async fn process_batch(&mut self, batch: DataBatch) -> Result<Option<Vec<DataBatch>>> {
        if let Some(function) = self.base.get_function_mut::<KeyByFunction>() {
            let keyed_batches = function.key_by(batch).await?;
            // Convert KeyedDataBatch to DataBatch::KeyedBatch
            let batches = keyed_batches.into_iter()
                .map(DataBatch::KeyedBatch)
                .collect();
            Ok(Some(batches))
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
    accumulators: HashMap<Key, Accumulator>,
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

    async fn process_batch(&mut self, batch: DataBatch) -> Result<Option<Vec<DataBatch>>> {
        // Explicitly check and handle only KeyedDataBatch
        match batch {
            DataBatch::KeyedBatch(keyed_batch) => {
                let key = keyed_batch.key().clone();
                
                // Check if we need to create a new accumulator or update an existing one
                let acc_exists = self.accumulators.contains_key(&key);
                
                if acc_exists {
                    // Update existing accumulator
                    {
                        let acc = self.accumulators.get_mut(&key)
                            .ok_or_else(|| anyhow::anyhow!("Accumulator not found"))?;
                        
                        let function = self.base.get_function_mut::<ReduceFunction>()
                            .ok_or_else(|| anyhow::anyhow!("ReduceFunction not available"))?;
                        
                        function.update_accumulator(acc, &keyed_batch).await?;
                    }
                } else {
                    // Create a new accumulator
                    let function = self.base.get_function_mut::<ReduceFunction>()
                        .ok_or_else(|| anyhow::anyhow!("ReduceFunction not available"))?;
                    
                    let new_acc = function.create_accumulator(&keyed_batch).await?;
                    self.accumulators.insert(key.clone(), new_acc);
                }
                
                // Now that we've updated the accumulator, get the result
                let acc = self.accumulators.get(&key)
                    .ok_or_else(|| anyhow::anyhow!("Accumulator not found"))?;
                
                let function = self.base.get_function::<ReduceFunction>()
                    .ok_or_else(|| anyhow::anyhow!("ReduceFunction not available"))?;
                
                let agg_result = function.get_result(acc).await?;
                let result_batch = self.result_extractor.extract_result(&key, &agg_result).await?;
                return Ok(Some(vec![result_batch]));
            },
            _ => {
                Err(anyhow::anyhow!("ReduceOperator requires KeyedDataBatch input"))
            }
        }
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::PROCESSOR
    }
}
