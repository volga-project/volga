use crate::common::message::Message;
use std::collections::HashMap;
use crate::transport::transport_client::DataWriter;
use crate::runtime::partition::Partition;
use crate::runtime::partition::PartitionTrait;
use futures::future::join_all;

// Routes messages to multiple channels based on the partition strategy
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
        // println!("Adding output channel id {:?}", channel_id);
        if self.output_channel_ids.contains(&channel_id) {
            panic!("Output channel id already exists");
        }
        self.output_channel_ids.push(channel_id);
    }

    pub fn output_channel_ids(&self) -> Vec<String> {
        self.output_channel_ids.clone()
    }

    // generate which channels the message goes to
    // Note: calling this updates partition state - do not call for the same message twice
    pub fn gen_partitioned_channel_ids(&mut self, message: Message) -> Vec<String> {
        let num_partitions = self.output_channel_ids.len();
        
        // Use BroadcastPartition for watermark messages, otherwise use the configured partition strategy
        let partitions = if let Message::Watermark(_) = &message {
            crate::runtime::partition::BroadcastPartition::new().partition(&message, num_partitions)
        } else {
            self.partition.partition(&message, num_partitions)
        };

        partitions.iter().map(|partition_idx| self.output_channel_ids[*partition_idx].clone()).collect()
    }

    async fn write_message_to_channels(&mut self, message: Message, channel_ids_to_send: Vec<String>) -> HashMap<String, (bool, u32)> {
        
        // parallel write
        let mut write_futures = Vec::new();
        for channel_id in channel_ids_to_send.clone() {
            let partition_message = message.clone();
            
            let mut writer = self.data_writer.clone();
            let channel_id_clone = channel_id.clone();
            write_futures.push(async move {
                return writer.write_message(&channel_id_clone, partition_message).await;
            });
        }
        let results = join_all(write_futures).await;
        
        let mut channel_results = HashMap::new();
        for (i, (success, backpressure_time_ms)) in results.into_iter().enumerate() {
            channel_results.insert(channel_ids_to_send[i].clone(), (success, backpressure_time_ms));
        }
        channel_results
    }

    pub async fn write_message_to_operators(
        collectors: &mut HashMap<String, Collector>,
        message: Message,
        channels_per_operator: HashMap<String, Vec<String>>
    ) -> HashMap<String, HashMap<String, (bool, u32)>> {
        let mut futures = Vec::new();
        for (operator_id, collector) in collectors.iter_mut() {
            let message_clone = message.clone();
            let operator_id = operator_id.clone();
            let channels = channels_per_operator.get(&operator_id).unwrap().clone();
            
            futures.push(async move {
                (operator_id, collector.write_message_to_channels(message_clone, channels).await)
            });
        }
        
        let results = join_all(futures).await;
        
        let mut res = HashMap::new();
        for (operator_id, write_results) in results {
            res.insert(operator_id, write_results);
        }
        
        res
    }
}