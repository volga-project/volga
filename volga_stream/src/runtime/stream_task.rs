use crate::runtime::{runtime_context::RuntimeContext, collector::Collector, execution_graph::{ExecutionVertex, OperatorConfig}, execution_graph::ExecutionGraph, partition::{Partition, PartitionType}};
use anyhow::Result;
use crate::transport::transport_client::TransportClient;
use crate::runtime::operator::{Operator, OperatorTrait, MapOperator, JoinOperator, SinkOperator, SourceOperator};
use crate::common::data_batch::DataBatch;
use std::collections::HashMap;
use futures::future::join_all;
use std::fmt;

pub struct StreamTask {
    vertex_id: String,
    operator: Operator,
    runtime_context: RuntimeContext,
    pub transport_client: TransportClient,
    collectors: HashMap<String, Collector>,
    running: bool,
}

impl StreamTask {
    pub fn new(
        vertex_id: String,
        operator_config: OperatorConfig,
        runtime_context: RuntimeContext,
    ) -> Result<Self> {
        let operator = match operator_config {
            OperatorConfig::MapConfig(_) => Operator::Map(MapOperator::new()),
            OperatorConfig::JoinConfig(_) => Operator::Join(JoinOperator::new()),
            OperatorConfig::SinkConfig(config) => Operator::Sink(SinkOperator::new(config)),
            OperatorConfig::SourceConfig(config) => Operator::Source(SourceOperator::new(config)),
        };
        let transport_client = TransportClient::new(vertex_id.clone());
        
        Ok(Self {
            vertex_id,
            operator,
            runtime_context,
            transport_client,
            collectors: HashMap::new(),
            running: true,
        })
    }
    
    pub fn create_or_update_collector(
        &mut self,
        channel_id: String,
        partition_type: PartitionType,
        target_operator_id: String,
    ) -> Result<()> {
        let writer = self.transport_client.writer.as_ref().ok_or_else(|| anyhow::anyhow!("Writer not initialized"))?;
        let partition = partition_type.create();

        let collector = self.collectors.entry(target_operator_id).or_insert_with(|| {
            Collector::new(
                writer.clone(),
                partition,
            )
        });

        collector.add_output_channel_id(channel_id);
        Ok(())
    }

    async fn collect_batch_parallel(
        &mut self, 
        batch: DataBatch,
        channels_to_send: Option<HashMap<String, Vec<String>>>
    ) -> Result<HashMap<String, Vec<String>>> {
        let mut futures = Vec::new();
        for (collector_id, collector) in self.collectors.iter_mut() {
            let batch_clone = batch.clone();
            let channels = channels_to_send.as_ref()
                .and_then(|map| map.get(collector_id).cloned());
            futures.push(async move {
                let result = collector.collect_batch(batch_clone, channels).await?;
                Ok::<_, anyhow::Error>((collector_id.clone(), result))
            });
        }
        let results = join_all(futures).await;
        let mut successful_channels = HashMap::new();
        for result in results {
            let (collector_id, channels) = result?;
            successful_channels.insert(collector_id, channels);
        }
        Ok(successful_channels)
    }

    pub async fn run(&mut self) -> Result<()> {
        while self.running {
            let batch = match self.operator.operator_type() {
                crate::runtime::operator::OperatorType::SOURCE => self.operator.fetch().await?,
                _ => {
                    let reader = self.transport_client.reader.as_mut().expect("Reader should be initialized for non-SOURCE operator");
                    if let Some(batch) = reader.read_batch().await? {
                        println!("StreamTask {:?} received batch", self.vertex_id);
                        Some(self.operator.process_batch(batch).await?)
                    } else {
                        None
                    }
                }
            };

            if let Some(batch) = batch {
                // TODO set vertex_id in the batch
                let mut channels_to_retry = HashMap::new();
                for (collector_id, collector) in &self.collectors {
                    channels_to_retry.insert(collector_id.clone(), collector.output_channel_ids());
                }

                while self.running && !channels_to_retry.is_empty() {
                    let successful_channels = self.collect_batch_parallel(batch.clone(), Some(channels_to_retry.clone())).await?;
                    channels_to_retry.clear();
                    for (collector_id, successful) in successful_channels {
                        if let Some(collector) = self.collectors.get(&collector_id) {
                            let unsuccessful: Vec<String> = collector.output_channel_ids()
                                .into_iter()
                                .filter(|channel| !successful.contains(channel))
                                .collect();
                            if !unsuccessful.is_empty() {
                                channels_to_retry.insert(collector_id, unsuccessful);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn open(&mut self) -> Result<()> {
        // Open the operator with runtime context
        self.operator.open(&self.runtime_context).await?;
        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        // TODO store in-fligh tasks
        println!("StreamTask {:?} closing", self.vertex_id);
        self.running = false;
        self.operator.close().await?;
        println!("StreamTask {:?} closed", self.vertex_id);
        Ok(())
    }

    pub async fn process_batch(&mut self, batch: DataBatch) -> Result<Option<DataBatch>> {
        Ok(Some(self.operator.process_batch(batch).await?))
    }
}

impl fmt::Debug for StreamTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamTask")
            .field("vertex_id", &self.vertex_id)
            .field("operator_type", &self.operator.operator_type())
            .field("num_collectors", &self.collectors.len())
            .field("collector_targets", &self.collectors.keys().collect::<Vec<_>>())
            .field("running", &self.running)
            .finish()
    }
}