use crate::{runtime::{
    collector::Collector, 
    execution_graph::{ExecutionGraph, ExecutionVertex, OperatorConfig}, 
    partition::{PartitionTrait, PartitionType}, 
    runtime_context::RuntimeContext,
    functions::{
        map::MapFunction,
        key_by::KeyByFunction,
        reduce::{ReduceFunction, AggregationResultExtractor},
        sink::SinkFunction,
        source::SourceFunction,
    }
}, transport::{DataReader, DataWriter}};
use anyhow::Result;
use tokio::{sync::mpsc::{Receiver, Sender, channel}, task::JoinHandle, time::Instant};
use crate::transport::transport_client::TransportClient;
use crate::runtime::operator::{Operator, OperatorTrait, MapOperator, JoinOperator, SinkOperator, SourceOperator, KeyByOperator, ReduceOperator};
use crate::common::data_batch::DataBatch;
use std::{collections::HashMap, sync::{atomic::{AtomicBool, Ordering}, Arc}, time::{Duration, SystemTime, UNIX_EPOCH}};
use futures::future::join_all;
use std::fmt;
// Helper function to get current timestamp
fn timestamp() -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0));
    format!("[{}.{:03}]", now.as_secs(), now.subsec_millis())
}
pub enum StreamTaskConfigurationRequest {
    CreateOrUpdateCollector {
        channel_id: String,
        partition_type: PartitionType,
        target_operator_id: String,
    },
    RegisterReaderReceiver {
        channel_id: String,
        receiver: Receiver<DataBatch>,
    },
    RegisterWriterSender {  
        channel_id: String,
        sender: Sender<DataBatch>,
    },
}

pub struct StreamTask {
    vertex_id: String,
    runtime_context: RuntimeContext,
    running: Arc<AtomicBool>,
    run_loop_handle: Option<JoinHandle<Result<()>>>,
    configuration_sender: Sender<StreamTaskConfigurationRequest>,
    configuration_receiver: Option<Receiver<StreamTaskConfigurationRequest>>,
    operator_config: OperatorConfig,
}

impl StreamTask {
    pub fn new(
        vertex_id: String,
        operator_config: OperatorConfig,
        runtime_context: RuntimeContext,
    ) -> Result<Self> {
        let (sender, receiver) = channel::<StreamTaskConfigurationRequest>(100);
        
        Ok(Self {
            vertex_id,
            runtime_context,
            running: Arc::new(AtomicBool::new(true)),
            run_loop_handle: None,
            configuration_sender: sender,
            configuration_receiver: Some(receiver),
            operator_config,
        })
    }
    
    pub async fn create_or_update_collector(
        &mut self,
        channel_id: String,
        partition_type: PartitionType,
        target_operator_id: String,
    ) -> Result<()> {
        self.configuration_sender.send(StreamTaskConfigurationRequest::CreateOrUpdateCollector {
            channel_id,
            partition_type,
            target_operator_id,
        }).await.map_err(|e| anyhow::anyhow!("Failed to send configuration request: {}", e))?;
        
        Ok(())
    }

    pub async fn register_reader_receiver(&mut self, channel_id: String, receiver: Receiver<DataBatch>) -> Result<()> {
        self.configuration_sender.send(StreamTaskConfigurationRequest::RegisterReaderReceiver {
            channel_id,
            receiver,
        }).await.map_err(|e| anyhow::anyhow!("Failed to send configuration request: {}", e))?;
        
        Ok(())
    }

    pub async fn register_writer_sender(&mut self, channel_id: String, sender: Sender<DataBatch>) -> Result<()> {
        self.configuration_sender.send(StreamTaskConfigurationRequest::RegisterWriterSender {
            channel_id,
            sender,
        }).await.map_err(|e| anyhow::anyhow!("Failed to send configuration request: {}", e))?;
        
        Ok(())
    }

    async fn process_configuration_requests(
        config_receiver: &mut Receiver<StreamTaskConfigurationRequest>,
        transport_client: &mut TransportClient,
        collectors: &mut HashMap<String, Collector>,
    ) -> Result<()> {
        while let Ok(request) = config_receiver.try_recv() {
            match request {
                StreamTaskConfigurationRequest::CreateOrUpdateCollector { 
                    channel_id, 
                    partition_type, 
                    target_operator_id 
                } => {
                    let writer = transport_client.writer.as_ref()
                        .ok_or_else(|| anyhow::anyhow!("Writer not initialized"))?;
                    
                    let partition = partition_type.create();
                    
                    let collector = collectors.entry(target_operator_id).or_insert_with(|| {
                        Collector::new(
                            writer.clone(),
                            partition,
                        )
                    });
                    
                    collector.add_output_channel_id(channel_id);
                },
                StreamTaskConfigurationRequest::RegisterReaderReceiver { channel_id, receiver } => {
                    if let Some(reader) = transport_client.reader.as_mut() {
                        reader.register_receiver(channel_id, receiver);
                    }
                },
                StreamTaskConfigurationRequest::RegisterWriterSender { channel_id, sender } => {
                    if let Some(writer) = transport_client.writer.as_mut() {
                        writer.register_sender(channel_id, sender);
                    }
                },
            }
        }
        
        Ok(())
    }

    async fn collect_batch_parallel(
        collectors: &mut HashMap<String, Collector>,
        batch: DataBatch,
        channels_to_send: Option<HashMap<String, Vec<String>>>
    ) -> Result<HashMap<String, Vec<String>>> {
        let mut futures = Vec::new();
        for (collector_id, collector) in collectors.iter_mut() {
            let batch_clone = batch.clone();
            let collector_id = collector_id.clone();
            let channels = channels_to_send.as_ref()
                .and_then(|map| map.get(&collector_id).cloned());
            
            futures.push(async move {
                let result = collector.collect_batch(batch_clone, channels).await?;
                Ok::<_, anyhow::Error>((collector_id, result))
            });
        }
        
        let results = join_all(futures).await;
        
        let mut successful_channels = HashMap::new();
        for result in results {
            match result {
                Ok((id, channels)) => {
                    successful_channels.insert(id, channels);
                },
                Err(e) => {
                    println!("Error collecting batch: {:?}", e);
                }
            }
        }
        
        Ok(successful_channels)
    }

    pub async fn run(&mut self) -> Result<()> {
        let vertex_id = self.vertex_id.clone();
        let runtime_context = self.runtime_context.clone();
        let running = self.running.clone();
        let operator_config = self.operator_config.clone();
        
        // Move receiver, nulling property on streamtask struct and removing ownership
        let mut receiver = self.configuration_receiver.take().unwrap();
        
        // Main stream task lifecycle loop
        let run_loop_handle = tokio::spawn(async move {
            let mut operator = match operator_config {
                OperatorConfig::MapConfig(map_function) => Operator::Map(MapOperator::new(map_function)),
                OperatorConfig::JoinConfig(_) => Operator::Join(JoinOperator::new()),
                OperatorConfig::SinkConfig(config) => Operator::Sink(SinkOperator::new(config)),
                OperatorConfig::SourceConfig(config) => Operator::Source(SourceOperator::new(config)),
                OperatorConfig::KeyByConfig(key_by_function) => Operator::KeyBy(KeyByOperator::new(key_by_function)),
                OperatorConfig::ReduceConfig(reduce_function, extractor) => Operator::Reduce(ReduceOperator::new(reduce_function, extractor)),
            };
            
            let mut transport_client = TransportClient::new(vertex_id.clone());
            let mut collectors: HashMap<String, Collector> = HashMap::new();
            
            operator.open(&runtime_context).await?;
            println!("{:?} Operator {:?} opened", timestamp(), vertex_id);
            
            while running.load(Ordering::SeqCst) {
                Self::process_configuration_requests(
                    &mut receiver,
                    &mut transport_client,
                    &mut collectors
                ).await?;
                
                let batches = match operator.operator_type() {
                    crate::runtime::operator::OperatorType::SOURCE => {
                        match operator.fetch().await? {
                            Some(batch) => Some(vec![batch]),
                            None => None,
                        }
                    }
                    _ => {
                        let reader = transport_client.reader.as_mut()
                            .expect("Reader should be initialized for non-SOURCE operator");
                        if let Some(batch) = reader.read_batch().await? {
                            println!("StreamTask {:?} received batch", vertex_id);
                            match operator.process_batch(batch).await? {
                                Some(batches) => Some(batches),
                                None => None,
                            }
                        } else {
                            None
                        }
                    }
                };

                if batches.is_none() {
                    continue;
                }
                
                let batches = batches.unwrap();
                
                for batch in batches {
                    let mut channels_to_retry = HashMap::new();
                    for (collector_id, collector) in &collectors {
                        channels_to_retry.insert(collector_id.clone(), collector.output_channel_ids());
                    }
                
                    let mut retries_before_close = 3;
                    while !channels_to_retry.is_empty() {
                        if !running.load(Ordering::SeqCst) && retries_before_close <= 0 {
                            break;
                        }
                        
                        let successful_channels = Self::collect_batch_parallel(
                            &mut collectors, 
                            batch.clone(), 
                            Some(channels_to_retry.clone())
                        ).await?;
                        
                        channels_to_retry.clear();
                        for (collector_id, successful) in successful_channels {
                            if let Some(collector) = collectors.get(&collector_id) {
                                let unsuccessful: Vec<String> = collector.output_channel_ids()
                                    .into_iter()
                                    .filter(|channel| !successful.contains(channel))
                                    .collect();
                                    
                                if !unsuccessful.is_empty() {
                                    channels_to_retry.insert(collector_id, unsuccessful);
                                }
                            }
                        }
                        
                        retries_before_close -= 1;
                    }
                }
            }
            
            operator.close().await?;
            println!("{:?} Operator {:?} closed", timestamp(), vertex_id);
            
            Ok(())
        });
        
        self.run_loop_handle = Some(run_loop_handle);
        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        println!("StreamTask {:?} closing", self.vertex_id);
        let start = Instant::now();
        self.running.store(false, Ordering::SeqCst);
        
        if let Some(handle) = self.run_loop_handle.take() {
            match handle.await {
                Ok(result) => {
                    println!("Run loop {:?} finished with result: {:?}", self.vertex_id, result);
                },
                Err(e) => {
                    println!("Run loop {:?} failed: {:?}", self.vertex_id, e);
                }
            }
        }
        
        let elapsed = start.elapsed();
        println!("StreamTask {:?} closed in {:?}", self.vertex_id, elapsed);
        Ok(())
    }
}

impl fmt::Debug for StreamTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamTask")
            .field("vertex_id", &self.vertex_id)
            .field("running", &self.running)
            .finish()
    }
}