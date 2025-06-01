use crate::{common::MAX_WATERMARK_VALUE, runtime::{
    collector::Collector, execution_graph::{ExecutionGraph, ExecutionVertex, OperatorConfig}, functions::{
        key_by::KeyByFunction, map::MapFunction, reduce::{AggregationResultExtractor, ReduceFunction}, sink::SinkFunction, source::SourceFunction
    }, partition::{PartitionTrait, PartitionType}, runtime_context::RuntimeContext
}, transport::{DataReader, DataWriter}};
use anyhow::Result;
use tokio::{sync::mpsc::{Receiver, Sender, channel}, task::JoinHandle, time::Instant};
use crate::transport::transport_client::TransportClient;
use crate::runtime::operator::{Operator, OperatorTrait, MapOperator, JoinOperator, SinkOperator, SourceOperator, KeyByOperator, ReduceOperator};
use crate::common::message::{Message, WatermarkMessage};
use std::{collections::HashMap, mem, sync::{atomic::{AtomicBool, Ordering, AtomicU8, AtomicU32}, Arc}, time::{Duration, SystemTime, UNIX_EPOCH}};
use futures::future::join_all;
use std::fmt;
use std::collections::HashSet;
use std::sync::Mutex;
// Helper function to get current timestamp
fn timestamp() -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0));
    format!("[{}.{:03}]", now.as_secs(), now.subsec_millis())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamTaskStatus {
    Created = 0,
    Running = 1,
    Closing = 2,
    Closed = 3,
}

#[derive(Debug, Clone)]
pub struct StreamTaskState {
    pub vertex_id: String,
    pub status: StreamTaskStatus,
}

impl From<u8> for StreamTaskStatus {
    fn from(value: u8) -> Self {
        match value {
            0 => StreamTaskStatus::Created,
            1 => StreamTaskStatus::Running,
            2 => StreamTaskStatus::Closing,
            3 => StreamTaskStatus::Closed,
            _ => panic!("Invalid task status value"),
        }
    }
}

pub enum StreamTaskConfigurationRequest {
    CreateOrUpdateCollector {
        channel_id: String,
        partition_type: PartitionType,
        target_operator_id: String,
    },
    RegisterReaderReceiver {
        channel_id: String,
        receiver: Receiver<Message>,
    },
    RegisterWriterSender {  
        channel_id: String,
        sender: Sender<Message>,
    },
}

#[derive(Debug)]
pub struct StreamTask {
    vertex_id: String,
    runtime_context: RuntimeContext,
    status: Arc<AtomicU8>,
    run_loop_handle: Option<JoinHandle<Result<()>>>,
    configuration_sender: Sender<StreamTaskConfigurationRequest>,
    configuration_receiver: Option<Receiver<StreamTaskConfigurationRequest>>,
    operator_config: OperatorConfig,
    execution_graph: ExecutionGraph,
    max_watermark_source_ids: Arc<Mutex<HashSet<String>>>, // tracks which source have finished
}

impl StreamTask {
    pub fn new(
        vertex_id: String,
        operator_config: OperatorConfig,
        runtime_context: RuntimeContext,
        execution_graph: ExecutionGraph,
    ) -> Result<Self> {
        let (sender, receiver) = channel::<StreamTaskConfigurationRequest>(100);
        
        Ok(Self {
            vertex_id,
            runtime_context,
            status: Arc::new(AtomicU8::new(StreamTaskStatus::Created as u8)),
            run_loop_handle: None,
            configuration_sender: sender,
            configuration_receiver: Some(receiver),
            operator_config,
            execution_graph,
            max_watermark_source_ids: Arc::new(Mutex::new(HashSet::new())),
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

    pub async fn register_reader_receiver(&mut self, channel_id: String, receiver: Receiver<Message>) -> Result<()> {
        self.configuration_sender.send(StreamTaskConfigurationRequest::RegisterReaderReceiver {
            channel_id,
            receiver,
        }).await.map_err(|e| anyhow::anyhow!("Failed to send configuration request: {}", e))?;
        
        Ok(())
    }

    pub async fn register_writer_sender(&mut self, channel_id: String, sender: Sender<Message>) -> Result<()> {
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

    async fn collect_message_parallel(
        collectors: &mut HashMap<String, Collector>,
        message: Message,
        channels_to_send: Option<HashMap<String, Vec<String>>>
    ) -> Result<HashMap<String, Vec<String>>> {
        let mut futures = Vec::new();
        for (collector_id, collector) in collectors.iter_mut() {
            let message_clone = message.clone();
            let collector_id = collector_id.clone();
            let channels = channels_to_send.as_ref()
                .and_then(|map| map.get(&collector_id).cloned());
            
            futures.push(async move {
                let result = collector.collect_message(message_clone, channels).await?;
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
                    println!("Error collecting message: {:?}", e);
                }
            }
        }
        
        Ok(successful_channels)
    }

    pub fn get_status(&self) -> StreamTaskStatus {
        StreamTaskStatus::from(self.status.load(Ordering::SeqCst))
    }

    async fn handle_watermark(
        watermark: WatermarkMessage,
        status: Arc<AtomicU8>,
        max_watermark_source_ids: Arc<Mutex<HashSet<String>>>,
        num_source_vertices: u32,
        vertex_id: String,
    ) -> Result<()> {
        if watermark.watermark_value == MAX_WATERMARK_VALUE {
            let source_id = watermark.source_vertex_id.clone();
            let mut done_sources = max_watermark_source_ids.lock().unwrap();
            done_sources.insert(source_id);
            
            // If we've received watermarks from all sources, initiate shutdown
            if done_sources.len() as u32 >= num_source_vertices {
                println!("{} Received max watermarks from all sources ({}), initiating shutdown", 
                    timestamp(), done_sources.len());
                Self::mark_closing(status, vertex_id);
            }
        }
        
        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        let vertex_id = self.vertex_id.clone();
        let runtime_context = self.runtime_context.clone();
        let status = self.status.clone();
        let operator_config = self.operator_config.clone();
        let execution_graph = self.execution_graph.clone();
        let received_source_ids = self.max_watermark_source_ids.clone();
        let num_source_vertices = execution_graph.get_source_vertices().len() as u32;
        
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
            
            status.store(StreamTaskStatus::Running as u8, Ordering::SeqCst);
            
            while status.load(Ordering::SeqCst) == StreamTaskStatus::Running as u8 {
                Self::process_configuration_requests(
                    &mut receiver,
                    &mut transport_client,
                    &mut collectors
                ).await?;
                
                let messages = match operator.operator_type() {
                    crate::runtime::operator::OperatorType::SOURCE => {
                        match operator.fetch().await? {
                            Some(message) => Some(vec![message]),
                            None => None,
                        }
                    }
                    _ => {
                        let reader = transport_client.reader.as_mut()
                            .expect("Reader should be initialized for non-SOURCE operator");
                        if let Some(message) = reader.read_message().await? {
                            println!("StreamTask {:?} received message", vertex_id);
                            match message {
                                Message::Watermark(watermark) => {
                                    Self::handle_watermark(
                                        watermark.clone(),
                                        status.clone(),
                                        received_source_ids.clone(),
                                        num_source_vertices,
                                        vertex_id.clone(),
                                    ).await?;
                                    Some(vec![Message::Watermark(watermark)])
                                }
                                _ => match operator.process_message(message).await.unwrap() {
                                    Some(messages) => Some(messages),
                                    None => None,
                                }
                            }
                        } else {
                            None
                        }
                    }
                };

                if messages.is_none() {
                    continue;
                }
                
                let messages = messages.unwrap();
                
                for message in messages {
                    let mut channels_to_retry = HashMap::new();
                    for (collector_id, collector) in &collectors {
                        channels_to_retry.insert(collector_id.clone(), collector.output_channel_ids());
                    }
                
                    let mut retries_before_close = 3;
                    while !channels_to_retry.is_empty() {
                        if status.load(Ordering::SeqCst) != StreamTaskStatus::Running as u8 && retries_before_close <= 0 {
                            break;
                        }
                        
                        let successful_channels = Self::collect_message_parallel(
                            &mut collectors, 
                            message.clone(), 
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

            status.store(StreamTaskStatus::Closed as u8, Ordering::SeqCst);
            println!("{:?} StreamTask {:?} closed", timestamp(), vertex_id);
            
            Ok(())
        });
        
        self.run_loop_handle = Some(run_loop_handle);
        Ok(())
    }

    fn mark_closing(status: Arc<AtomicU8>, vertex_id: String) {
        println!("StreamTask {:?} closing", vertex_id);
        status.store(StreamTaskStatus::Closing as u8, Ordering::SeqCst);
    }

    pub async fn close_and_wait(&mut self) -> Result<()> {
        if self.status.load(Ordering::SeqCst) != StreamTaskStatus::Running as u8 {
            return Ok(());
        }

        Self::mark_closing(self.status.clone(), self.vertex_id.clone());
        
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
        Ok(())
    }

    pub fn vertex_id(&self) -> &str {
        &self.vertex_id
    }
}