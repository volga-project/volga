use crate::core::{collector::Collector, record::StreamRecord, runtime_context::RuntimeContext};
use anyhow::{Error, Result};
use async_trait::async_trait;
use serde_json::Value;

#[async_trait]
pub trait SourceContext: Send + Sync {
    async fn collect_batch(&mut self, records: Vec<StreamRecord>) -> Result<()>;
    fn get_runtime_context(&self) -> &RuntimeContext;
}

pub struct SourceContextImpl {
    pub collectors: Vec<Box<dyn Collector>>,
    pub runtime_context: RuntimeContext,
    pub timestamp_assigner: Option<Box<dyn TimestampAssigner>>,
    pub num_records: Option<i64>,
    pub num_fetched_records: i64,
    pub finished: bool,
}

impl SourceContextImpl {
    pub fn new(
        collectors: Vec<Box<dyn Collector>>,
        runtime_context: RuntimeContext,
        timestamp_assigner: Option<Box<dyn TimestampAssigner>>,
        num_records: Option<i64>,
    ) -> Self {
        Self {
            collectors,
            runtime_context,
            timestamp_assigner,
            num_records,
            num_fetched_records: 0,
            finished: false,
        }
    }
}

#[async_trait]
impl SourceContext for SourceContextImpl {
    async fn collect_batch(&mut self, records: Vec<StreamRecord>) -> Result<()> {
        if self.finished {
            return Ok(());
        }

        let records = if let Some(assigner) = &self.timestamp_assigner {
            records.into_iter()
                .map(|record| assigner.assign_timestamp(record))
                .collect::<Result<Vec<_>>>()?
        } else {
            records
        };

        for collector in &mut self.collectors {
            collector.collect_batch(records.clone()).await?;
        }

        self.num_fetched_records += records.len() as i64;
        self.throughput_stats.increment(records.len() as i64);

        if let Some(num_records) = self.num_records {
            if self.num_fetched_records >= num_records {
                self.finished = true;
            }
        }

        Ok(())
    }

    fn get_runtime_context(&self) -> &RuntimeContext {
        &self.runtime_context
    }
}

#[async_trait]
pub trait TimestampAssigner: Send + Sync {
    async fn assign_timestamp(&self, record: StreamRecord) -> Result<StreamRecord>;
}

pub struct EventTimeAssigner<F> 
where 
    F: Fn(&StreamRecord) -> Result<i64> + Send + Sync 
{
    func: F,
}

impl<F> EventTimeAssigner<F>
where 
    F: Fn(&StreamRecord) -> Result<i64> + Send + Sync 
{
    pub fn new(func: F) -> Self {
        Self { func }
    }
}

#[async_trait]
impl<F> TimestampAssigner for EventTimeAssigner<F>
where 
    F: Fn(&StreamRecord) -> Result<i64> + Send + Sync 
{
    async fn assign_timestamp(&self, mut record: StreamRecord) -> Result<StreamRecord> {
        let ts = (self.func)(&record)?;
        match record {
            StreamRecord::Record(ref mut r) => {
                r.base.event_time = Some(ts);
            }
            StreamRecord::KeyedRecord(ref mut kr) => {
                kr.base.event_time = Some(ts);
            }
        }
        Ok(record)
    }
}

pub struct DefaultEventTimeAssigner {
    timestamp_field: String,
}

impl DefaultEventTimeAssigner {
    pub fn new(timestamp_field: String) -> Self {
        Self { timestamp_field }
    }

    fn extract_timestamp(&self, value: &Value) -> Result<i64> {
        match value {
            Value::String(s) => {
                s.parse::<i64>()
                    .map_err(|_| Error::msg("Failed to parse timestamp from string"))
            }
            _ => Err(Error::msg("Value is not a string"))
        }
    }
}

#[async_trait]
impl TimestampAssigner for DefaultEventTimeAssigner {
    async fn assign_timestamp(&self, mut record: StreamRecord) -> Result<StreamRecord> {
        let ts = self.extract_timestamp(record.value())?;
        match record {
            StreamRecord::Record(ref mut r) => {
                r.base.event_time = Some(ts);
            }
            StreamRecord::KeyedRecord(ref mut kr) => {
                kr.base.event_time = Some(ts);
            }
        }
        Ok(record)
    }
}