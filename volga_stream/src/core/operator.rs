use async_trait::async_trait;
use crate::core::collector::Collector;
use crate::core::runtime_context::RuntimeContext;
use crate::core::record::StreamRecord;
use anyhow::{Error, Result};
use std::any::Any;
use std::collections::HashMap;

use super::{function::{Function, JoinFunction, MapFunction, SourceFunction}, record::Value, source::{SourceContextImpl, TimestampAssigner}};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorType {
    SOURCE,
    PROCESSOR,
}

#[async_trait]
pub trait Operator: Send + Sync {
    async fn open(&mut self, collectors: Vec<Box<dyn Collector>>, runtime_context: RuntimeContext) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
    async fn finish(&mut self) -> Result<()>;
    async fn process_batch(&mut self, records: Vec<StreamRecord>, stream_id: Option<usize>) -> Result<()>;
    fn operator_type(&self) -> OperatorType;
    fn as_any(&self) -> &dyn Any;

    // Default implementation for non-source operators
    async fn fetch(&mut self) -> Result<()> {
        Err(Error::new("Not a source operator"))
    }
}

// Base operator
pub struct BaseOperator<F: Function> {
    pub func: F,
    pub collectors: Vec<Box<dyn Collector>>,
    pub runtime_context: Option<RuntimeContext>,
}

impl<F: Function> BaseOperator<F> {
    pub fn new(func: F) -> Self {
        Self {
            func,
            collectors: Vec::new(),
            runtime_context: None,
        }
    }

    async fn collect_batch(&mut self, records: Vec<StreamRecord>) -> Result<()> {
        for collector in &mut self.collectors {
            let records_clone = records.clone();
            collector.collect_batch(records_clone).await?;
        }
        Ok(())
    }
}

// MapOperator
pub struct MapOperator<F: MapFunction> {
    base: BaseOperator<F>,
}

impl<F: MapFunction> MapOperator<F> {
    pub fn new(func: F) -> Self {
        Self {
            base: BaseOperator::new(func),
        }
    }
}

#[async_trait]
impl<F: MapFunction> Operator for MapOperator<F> {
    async fn open(&mut self, collectors: Vec<Box<dyn Collector>>, runtime_context: RuntimeContext) -> Result<()> {
        self.base.collectors = collectors;
        self.base.runtime_context = Some(runtime_context.clone());
        self.base.func.open(runtime_context).await
    }

    async fn close(&mut self) -> Result<()> {
        self.base.func.close().await
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_batch(&mut self, records: Vec<StreamRecord>, stream_id: Option<usize>) -> Result<()> {
        if stream_id != Some(0) {
            return Err(Error::new("MapOperator only accepts input from stream 0"));
        }

        let map_futures: Vec<_> = records.iter()
            .map(|record| self.base.func.map(record.value()))
            .collect();
        
        let mapped_values = tokio::join!(map_futures.into_iter())
            .into_iter()
            .collect::<Result<Vec<Value>>>()?;
        
        let new_records: Vec<_> = records.iter()
            .zip(mapped_values)
            .map(|(record, value)| record.with_new_value(value))
            .collect();
        
        self.base.collect_batch(new_records).await
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::ONE_INPUT
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// JoinOperator
pub struct JoinOperator<F: JoinFunction> {
    base: BaseOperator<F>,
    left_records: HashMap<Value, Vec<StreamRecord>>,
    right_records: HashMap<Value, Vec<StreamRecord>>,
}

impl<F: JoinFunction> JoinOperator<F> {
    pub fn new(func: F) -> Self {
        Self {
            base: BaseOperator::new(func),
            left_records: HashMap::new(),
            right_records: HashMap::new(),
        }
    }
}

#[async_trait]
impl<F: JoinFunction> Operator for JoinOperator<F> {
    async fn open(&mut self, collectors: Vec<Box<dyn Collector>>, runtime_context: RuntimeContext) -> Result<()> {
        self.base.collectors = collectors;
        self.base.runtime_context = Some(runtime_context.clone());
        self.base.func.open(runtime_context).await
    }

    async fn close(&mut self) -> Result<()> {
        self.base.func.close().await
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_batch(&mut self, records: Vec<StreamRecord>, stream_id: Option<usize>) -> Result<()> {
        let stream_id = stream_id.ok_or_else(|| Error::new("JoinOperator requires a stream ID"))?;
        
        let mut keyed_records: HashMap<Value, Vec<StreamRecord>> = HashMap::new();
        for record in records {
            let key = record.key().ok_or_else(|| Error::new("JoinOperator expects a keyed record"))?;
            keyed_records.entry(key.clone()).or_default().push(record);
        }

        match stream_id {
            0 => { // Left stream
                for (key, left_batch) in keyed_records {
                    self.left_records.entry(key.clone()).or_default().extend(left_batch.clone());
                    
                    if let Some(right_records) = self.right_records.get(&key) {
                        let mut join_futures = Vec::new();
                        let mut record_pairs = Vec::new();
                        
                        for left_record in &left_batch {
                            for right_record in right_records {
                                join_futures.push(self.base.func.join(
                                    left_record.value(),
                                    right_record.value()
                                ));
                                record_pairs.push((left_record, right_record));
                            }
                        }
                        
                        let joined_values = tokio::join!(join_futures.into_iter())
                            .into_iter()
                            .collect::<Result<Vec<Value>>>()?;
                        
                        let new_records: Vec<_> = record_pairs.iter()
                            .zip(joined_values)
                            .map(|((left_record, _), value)| (*left_record).with_new_value(value))
                            .collect();
                        
                        self.base.collect_batch(new_records).await?;
                    }
                }
            }
            1 => { // Right stream - similar pattern
                // ... similar implementation for right stream ...
            }
            _ => return Err(Error::new("Invalid stream ID for JoinOperator")),
        }
        Ok(())
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::TWO_INPUT
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct SourceOperator<F: SourceFunction> {
    base: BaseOperator<F>,
    source_context: Option<SourceContextImpl>,
}

impl<F: SourceFunction> SourceOperator<F> {
    pub fn new(func: F) -> Self {
        Self {
            base: BaseOperator::new(func),
            source_context: None,
        }
    }

    pub fn set_timestamp_assigner(&mut self, assigner: Box<dyn TimestampAssigner>) {
        if let Some(ctx) = &mut self.source_context {
            ctx.timestamp_assigner = Some(assigner);
        }
    }
}

#[async_trait]
impl<F: SourceFunction> Operator for SourceOperator<F> {
    async fn open(&mut self, collectors: Vec<Box<dyn Collector>>, runtime_context: RuntimeContext) -> Result<()> {
        self.base.collectors = collectors.clone();
        self.base.runtime_context = Some(runtime_context.clone());
        self.base.func.open(runtime_context.clone()).await?;

        self.source_context = Some(SourceContextImpl::new(
            collectors,
            runtime_context,
            None,
            None,
        ));

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.base.func.close().await
    }

    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_batch(&mut self, _records: Vec<StreamRecord>, _stream_id: Option<usize>) -> Result<()> {
        Err(Error::new("SourceOperator does not process input records"))
    }

    async fn fetch(&mut self) -> Result<()> {
        if let Some(ctx) = &mut self.source_context {
            self.base.func.fetch(ctx).await
        } else {
            Err(Error::new("SourceContext not initialized"))
        }
    }

    fn operator_type(&self) -> OperatorType {
        OperatorType::SOURCE
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
