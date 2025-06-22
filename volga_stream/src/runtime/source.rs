use crate::runtime::{collector::Collector, runtime_context::RuntimeContext};
use crate::common::{data_batch::DataBatch, message::{Message, WatermarkMessage}};
use anyhow::{Error, Result};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
// use arrow::record_batch::RecordBatch;

#[async_trait]
pub trait SourceContext: Send + Sync {
    async fn collect_batch(&mut self, batch: DataBatch) -> Result<()>;
    fn get_runtime_context(&self) -> &RuntimeContext;
}

pub struct SourceContextImpl {
    pub collectors: Vec<Box<Collector>>,
    pub runtime_context: RuntimeContext,
    pub timestamp_assigner: Option<Box<dyn TimestampAssigner>>,
    pub watermark_generator: Option<Box<dyn WatermarkGenerator>>,
    pub num_records: Option<i64>,
    pub num_fetched_records: i64,
    pub finished: bool,
}

impl SourceContextImpl {
    pub fn new(
        collectors: Vec<Box<Collector>>,
        runtime_context: RuntimeContext,
        timestamp_assigner: Option<Box<dyn TimestampAssigner>>,
        watermark_generator: Option<Box<dyn WatermarkGenerator>>,
        num_records: Option<i64>,
    ) -> Self {
        Self {
            collectors,
            runtime_context,
            timestamp_assigner,
            watermark_generator,
            num_records,
            num_fetched_records: 0,
            finished: false,
        }
    }
}

#[async_trait]
impl SourceContext for SourceContextImpl {
    async fn collect_batch(&mut self, batch: DataBatch) -> Result<()> {
        if self.finished {
            return Ok(());
        }

        let mut batch = batch;
        
        // Apply timestamp assigner if present
        if let Some(assigner) = &self.timestamp_assigner {
            assigner.assign_timestamp(&mut batch);
        }

        // Collect batch through all collectors
        for collector in &mut self.collectors {
            collector.collect_batch(batch.clone(), None).await?;
        }

        self.num_fetched_records += batch.record_batch().num_rows() as i64;

        if let Some(num_records) = self.num_records {
            if self.num_fetched_records >= num_records {
                self.finished = true;
                // Send EOS watermark to all downstream operators
                self.send_eos_watermark().await?;
            }
        }

        // Update watermark if generator is present
        if let Some(generator) = &self.watermark_generator {
            if let Some(watermark) = generator.generate_watermark(&batch) {
                // TODO: Update watermark in runtime context
            }
        }

        Ok(())
    }

    async fn send_eos_watermark(&mut self) -> Result<()> {
        // Create EOS watermark
        let eos_watermark = Message::Watermark(WatermarkMessage::new_eos(
            self.runtime_context.vertex_id.clone()
        ));
        
        // Send EOS watermark to all downstream operators
        for collector in &mut self.collectors {
            collector.collect_batch(DataBatch::new(eos_watermark.clone()), None).await?;
        }
        
        println!("Source {} sent EOS watermark to all downstream operators", self.runtime_context.vertex_id);
        Ok(())
    }

    fn get_runtime_context(&self) -> &RuntimeContext {
        &self.runtime_context
    }
}

pub trait TimestampAssigner: Send + Sync {
    fn assign_timestamp(&self, batch: &mut DataBatch);
}

pub trait WatermarkGenerator: Send + Sync {
    fn generate_watermark(&self, batch: &DataBatch) -> Option<u64>;
}

pub struct EventTimeAssigner<F> 
where 
    F: Fn(&DataBatch) -> Result<i64> + Send + Sync 
{
    func: F,
}

impl<F> EventTimeAssigner<F>
where 
    F: Fn(&DataBatch) -> Result<i64> + Send + Sync 
{
    pub fn new(func: F) -> Self {
        Self { func }
    }
}

impl<F> TimestampAssigner for EventTimeAssigner<F>
where 
    F: Fn(&DataBatch) -> Result<i64> + Send + Sync 
{
    fn assign_timestamp(&self, batch: &mut DataBatch) {
        if let Ok(ts) = (self.func)(batch) {
            // TODO: Update timestamp in the batch
            // This will require modifying the RecordBatch to add/update timestamp column
        }
    }
}

pub struct DefaultEventTimeAssigner {
    timestamp_field: String,
}

impl DefaultEventTimeAssigner {
    pub fn new(timestamp_field: String) -> Self {
        Self { timestamp_field }
    }

    fn extract_timestamp(&self, batch: &DataBatch) -> Result<i64> {
        // TODO: Extract timestamp from the specified column in the RecordBatch
        // This will require accessing the column by name and converting to i64
        Err(Error::msg("Not implemented"))
    }
}

impl TimestampAssigner for DefaultEventTimeAssigner {
    fn assign_timestamp(&self, batch: &mut DataBatch) {
        if let Ok(ts) = self.extract_timestamp(batch) {
            // TODO: Update timestamp in the batch
            // This will require modifying the RecordBatch to add/update timestamp column
        }
    }
}