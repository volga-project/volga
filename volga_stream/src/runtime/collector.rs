use anyhow::Result;
use crate::common::data_batch::DataBatch;
use std::collections::HashMap;
use crate::transport::transport_client::DataWriter;
use crate::runtime::partition::Partition;
use crate::runtime::partition::PartitionTrait;
use futures::future::join_all;

#[derive(Clone)]
pub struct Collector {
    data_writer: DataWriter,
    output_channel_ids: Vec<String>,
    partition: Partition,
}

impl Collector {
    pub fn new(
        data_writer: DataWriter,
        partition: Partition,
    ) -> Self {
        Self {
            data_writer,
            output_channel_ids: Vec::new(),
            partition,
        }
    }

    pub fn add_output_channel_id(&mut self, channel_id: String) {
        if self.output_channel_ids.contains(&channel_id) {
            panic!("Output channel id already exists");
        }
        self.output_channel_ids.push(channel_id);
    }

    pub fn output_channel_ids(&self) -> Vec<String> {
        self.output_channel_ids.clone()
    }

    pub async fn collect_batch(&mut self, batch: DataBatch, channel_ids_to_send: Option<Vec<String>>) -> Result<Vec<String>> {
        let num_partitions = self.output_channel_ids.len();
        let mut partitioned_batches: Vec<DataBatch> = vec![DataBatch::new(None, batch.record_batch().clone()); num_partitions];
        
        let partitions = self.partition.partition(&batch, num_partitions)?;
        for partition_idx in partitions {
            partitioned_batches[partition_idx] = batch.clone();
        }

        // Create channel to partition mapping
        let channel_to_partition: HashMap<_, _> = self.output_channel_ids.iter()
            .enumerate()
            .map(|(idx, channel_id)| (channel_id.clone(), idx))
            .collect();

        // Use provided channel IDs or default to all output channels
        let channels_to_send = channel_ids_to_send.unwrap_or_else(|| self.output_channel_ids.clone());

        // Create futures for parallel writes
        let mut write_futures = Vec::new();
        for channel_id in channels_to_send {
            if let Some(&partition_idx) = channel_to_partition.get(&channel_id) {
                let partition_batch = partitioned_batches[partition_idx].clone();
                if partition_batch.record_batch().num_rows() == 0 {
                    continue;
                }
                
                let mut writer = self.data_writer.clone();
                let channel_id_clone = channel_id.clone();
                write_futures.push(async move {
                    match writer.write_batch(&channel_id_clone, partition_batch).await {
                        Ok(_) => Ok(channel_id_clone),
                        Err(_) => Err(anyhow::anyhow!("Failed to write batch"))
                    }
                });
            }
        }

        // Execute all writes in parallel
        let results = join_all(write_futures).await;
        
        // Collect successful channel IDs
        let successful_channels: Vec<String> = results.into_iter()
            .filter_map(|result| result.ok())
            .collect();

        Ok(successful_channels)
    }
}