use async_trait::async_trait;
use crate::core::record::StreamRecord;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::network::data_writer::DataWriter;
use crate::core::partition::Partition;
use tokio::task::{JoinSet};

#[async_trait]
pub trait Collector: Send + Sync {
    async fn collect_batch(&mut self, records: Vec<StreamRecord>) -> Result<()>;
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
    async fn collect_batch(&mut self, records: Vec<StreamRecord>) -> Result<()> {
        let mut all_futures = Vec::new();
        for collector in &mut self.collectors {
            let records_clone = records.clone();
            all_futures.push(collector.collect_batch(records_clone));
        }

        futures::future::try_join_all(all_futures).await?;
        Ok(())
    }
}

// Output collector
pub struct OutputCollector {
    data_writer: Arc<Mutex<Box<dyn DataWriter>>>,
    output_channel_ids: Vec<String>,
    partition: Box<dyn Partition>,
}

impl OutputCollector {
    pub fn new(
        data_writer: Arc<Mutex<Box<dyn DataWriter>>>,
        output_channel_ids: Vec<String>,
        partition: Box<dyn Partition>,
    ) -> Self {
        Self {
            data_writer,
            output_channel_ids,
            partition,
        }
    }
}

#[async_trait]
impl Collector for OutputCollector {
    async fn collect_batch(&mut self, records: Vec<StreamRecord>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let num_partitions = self.output_channel_ids.len();
        let mut partitioned_records: Vec<Vec<StreamRecord>> = vec![Vec::new(); num_partitions];
        
        for record in records {
            let partitions = self.partition.partition(&record, num_partitions)?;
            for partition_idx in partitions {
                partitioned_records[partition_idx].push(record.clone());
            }
        }

        let mut join_set = JoinSet::new();
        for (partition_idx, partition_records) in partitioned_records.into_iter().enumerate() {
            if partition_records.is_empty() {
                continue;
            }
            
            let channel_id = self.output_channel_ids[partition_idx].clone();
            let data_writer = self.data_writer.clone();
            
            join_set.spawn(async move {
                let mut writer = data_writer.lock().await;
                writer.write_batch(&channel_id, partition_records).await
            });
        }

        while let Some(result) = join_set.join_next().await {
            result??;
        }

        Ok(())
    }
}