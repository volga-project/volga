use crate::common::message::Message;
use crate::transport::channel::Channel;
use std::collections::HashMap;
use crate::transport::transport_client::DataWriter;
use crate::runtime::partition::Partition;
use crate::runtime::partition::PartitionTrait;
use futures::future::join_all;
use tokio::sync::mpsc;

// Routes messages to multiple channels based on the partition strategy
#[derive(Clone)]
pub struct Collector {
    data_writer: DataWriter,
    output_channels: Vec<Channel>,
    partition: Partition,
}

impl Collector {
    pub fn new(
        data_writer: DataWriter,
        partition: Partition,
    ) -> Self {
        Self {
            data_writer,
            output_channels: Vec::new(),
            partition,
        }
    }

    pub async fn start(&mut self) {
        self.data_writer.start().await
    }

    pub async fn flush_and_close(&mut self) -> Result<(), mpsc::error::SendError<Message>> {
        self.data_writer.flush_and_close().await
    }

    pub fn add_output_channel(&mut self, channel: Channel) {
        if self.output_channels.contains(&channel) {
            panic!("Output channel already exists");
        }
        self.output_channels.push(channel);
        // Dest index is the target task_index (KeyBy / RequestRoute). Sort
        // numerically so dest 10 is task 10, not lex-sorted `_10` before `_2`.
        self.output_channels.sort_by(|a, b| {
            a.target_task_index()
                .cmp(&b.target_task_index())
                .then_with(|| a.get_channel_id().cmp(&b.get_channel_id()))
        });
    }

    pub fn output_channels(&self) -> Vec<Channel> {
        self.output_channels.clone()
    }

    // generate which channels the message goes to
    // Note: calling this updates partition state - do not call for the same message twice
    // TODO what if it is a retried message?
    pub fn gen_partitioned_channels(&mut self, message: &Message) -> Vec<Channel> {
        let num_partitions = self.output_channels.len();
        
        // Use BroadcastPartition for control messages, otherwise use the configured partition strategy
        let partitions = if matches!(message, Message::Watermark(_) | Message::CheckpointBarrier(_)) {
            crate::runtime::partition::BroadcastPartition::new().partition(message, num_partitions)
        } else {
            self.partition.partition(message, num_partitions)
        };

        partitions.iter().map(|partition_idx| self.output_channels[*partition_idx].clone()).collect()
    }

    async fn write_message_to_channels(
        &mut self,
        message: &Message,
        channels_to_send: Vec<Channel>,
    ) -> HashMap<Channel, bool> {
        // parallel write (BP time is recorded inside DataWriter / BatchSender)
        let mut write_futures = Vec::new();
        for channel in channels_to_send.clone() {
            let mut writer = self.data_writer.clone();
            write_futures.push(async move { writer.write_message(&channel, message).await });
        }
        let results = join_all(write_futures).await;

        let mut channel_results = HashMap::new();
        for (i, success) in results.into_iter().enumerate() {
            channel_results.insert(channels_to_send[i].clone(), success);
        }
        channel_results
    }

    pub async fn write_message_to_operators(
        collectors: &mut HashMap<String, Collector>,
        message: &Message,
        channels_per_operator: HashMap<String, Vec<Channel>>,
    ) -> HashMap<String, HashMap<Channel, bool>> {
        let mut futures = Vec::new();
        for (operator_id, collector) in collectors.iter_mut() {
            let operator_id = operator_id.clone();
            let channels = channels_per_operator.get(&operator_id).unwrap().clone();
            
            futures.push(async move {
                (operator_id, collector.write_message_to_channels(message, channels).await)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::message::TARGET_SUBTASK_EXTRA;
    use crate::runtime::health::WorkerHealth;
    use crate::runtime::partition::PartitionType;
    use crate::transport::channel::Channel;
    use crate::transport::transport_client::DataWriter;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn dummy_writer() -> DataWriter {
        DataWriter::new(
            Arc::<str>::from("src_0"),
            HashMap::new(),
            None,
            Arc::new(WorkerHealth::new()),
        )
    }

    fn int_message(dest: usize) -> Message {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
        let extras = HashMap::from([(TARGET_SUBTASK_EXTRA.to_string(), dest.to_string())]);
        Message::new(None, batch, None, Some(extras))
    }

    #[test]
    fn dest_10_routes_to_task_10_not_lex_channel() {
        let mut collector = Collector::new(dummy_writer(), PartitionType::Hash.create());
        for i in (0..12).rev() {
            collector.add_output_channel(Channel::new_local(
                "keyby_0".to_string(),
                format!("window_{i}"),
            ));
        }
        let channels = collector.output_channels();
        assert_eq!(channels[2].get_target_vertex_id(), "window_2");
        assert_eq!(channels[10].get_target_vertex_id(), "window_10");

        let routed = collector.gen_partitioned_channels(&int_message(10));
        assert_eq!(routed.len(), 1);
        assert_eq!(routed[0].get_target_vertex_id(), "window_10");
    }
}
