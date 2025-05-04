use async_trait::async_trait;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::transport::transport_client::DataWriter;
use crate::runtime::partition::Partition;
use tokio::task::JoinSet;
use crate::common::data_batch::DataBatch;
use std::any::Any;

#[async_trait]
pub trait Collector: Send + Sync + Any {
    async fn collect_batch(&mut self, batch: DataBatch) -> Result<()>;
}

// Collection collector
pub struct CollectionCollector {
    collectors: Vec<Box<dyn Collector>>,
}

impl CollectionCollector {
    pub fn new(collectors: Vec<Box<dyn Collector>>) -> Self {
        Self { collectors }
    }
}

#[async_trait]
impl Collector for CollectionCollector {
    async fn collect_batch(&mut self, batch: DataBatch) -> Result<()> {
        let mut all_futures = Vec::new();
        for collector in &mut self.collectors {
            let batch_clone = batch.clone();
            all_futures.push(collector.collect_batch(batch_clone));
        }

        futures::future::try_join_all(all_futures).await?;
        Ok(())
    }
}

// Output collector
pub struct OutputCollector {
    data_writer: Arc<Mutex<DataWriter>>,
    output_channel_ids: Vec<String>,
    partition: Box<dyn Partition>,
}

impl OutputCollector {
    pub fn new(
        data_writer: Arc<Mutex<DataWriter>>,
        partition: Box<dyn Partition>,
    ) -> Self {
        Self {
            data_writer,
            output_channel_ids: Vec::new(),
            partition,
        }
    }

    pub fn register_output_channel_id(&mut self, channel_id: String) {
        self.output_channel_ids.push(channel_id);
    }
}

#[async_trait]
impl Collector for OutputCollector {
    async fn collect_batch(&mut self, batch: DataBatch) -> Result<()> {
        if batch.record_batch().is_empty() {
            return Ok(());
        }

        let num_partitions = self.output_channel_ids.len();
        let mut partitioned_batches: Vec<DataBatch> = vec![DataBatch::new(None, batch.record_batch().clone()); num_partitions];
        
        // Partition the batch based on the key
        if let Ok(key) = batch.key() {
            let partitions = self.partition.partition(&batch, num_partitions)?;
            for partition_idx in partitions {
                partitioned_batches[partition_idx] = batch.clone();
            }
        }

        let mut join_set = JoinSet::new();
        for (partition_idx, partition_batch) in partitioned_batches.into_iter().enumerate() {
            if partition_batch.record_batch().is_empty() {
                continue;
            }
            
            let channel_id = self.output_channel_ids[partition_idx].clone();
            let data_writer = self.data_writer.clone();
            
            join_set.spawn(async move {
                let mut writer = data_writer.lock().await;
                writer.write_batch(&channel_id, partition_batch).await
            });
        }

        while let Some(result) = join_set.join_next().await {
            result??;
        }

        Ok(())
    }
}