use crate::{common::MAX_WATERMARK_VALUE, runtime::{
    collector::Collector, execution_graph::{ExecutionGraph, OperatorConfig}, runtime_context::RuntimeContext
}, transport::transport_client::TransportClientConfig};
use anyhow::Result;
use tokio::task::JoinHandle;
use crate::transport::transport_client::TransportClient;
use crate::runtime::operator::{Operator, OperatorTrait, MapOperator, JoinOperator, SinkOperator, SourceOperator, KeyByOperator, ReduceOperator};
use crate::common::message::{Message, WatermarkMessage};
use std::{collections::HashMap, sync::{atomic::{Ordering, AtomicU8}, Arc}, time::{Duration, SystemTime, UNIX_EPOCH}};
use futures::future::join_all;
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

#[derive(Debug)]
pub struct StreamTask {
    vertex_id: String,
    runtime_context: RuntimeContext,
    status: Arc<AtomicU8>,
    run_loop_handle: Option<JoinHandle<Result<()>>>,
    operator_config: OperatorConfig,
    transport_client_config: Option<TransportClientConfig>,
    execution_graph: ExecutionGraph,
    max_watermark_source_ids: Arc<Mutex<HashSet<String>>>, // tracks which source have finished
}

impl StreamTask {
    pub fn new(
        vertex_id: String,
        operator_config: OperatorConfig,
        transport_client_config: TransportClientConfig,
        runtime_context: RuntimeContext,
        execution_graph: ExecutionGraph,
    ) -> Self {
        
        Self {
            vertex_id,
            runtime_context,
            status: Arc::new(AtomicU8::new(StreamTaskStatus::Created as u8)),
            run_loop_handle: None,
            operator_config,
            transport_client_config: Some(transport_client_config),
            execution_graph,
            max_watermark_source_ids: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    // TODO retrying collect() updates partition state - fix this
    async fn collect_message_parallel(
        collectors: &mut HashMap<String, Collector>,
        message: Message,
        channels_to_send: Option<HashMap<String, Vec<String>>>
    ) -> HashMap<String, Vec<String>> {
        let mut futures = Vec::new();
        for (collector_id, collector) in collectors.iter_mut() {
            let message_clone = message.clone();
            let collector_id = collector_id.clone();
            let channels = channels_to_send.as_ref()
                .and_then(|map| map.get(&collector_id).cloned());
            
            futures.push(async move {
                let result = collector.collect_message(message_clone, channels).await;
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
        
        successful_channels
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

    pub async fn run(&mut self) {
        let vertex_id = self.vertex_id.clone();
        let runtime_context = self.runtime_context.clone();
        let status = self.status.clone();
        let operator_config = self.operator_config.clone();
        let execution_graph = self.execution_graph.clone();
        let received_source_ids = self.max_watermark_source_ids.clone();
        let num_source_vertices = execution_graph.get_source_vertices().len() as u32;

        let transport_client_config = self.transport_client_config.take().unwrap();
        
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
            
            let mut transport_client = TransportClient::new(vertex_id.clone(), transport_client_config);
            
            let mut collectors: HashMap<String, Collector> = HashMap::new();

            let (_, output_edges) = execution_graph.get_edges_for_vertex(&vertex_id).unwrap();

            for edge in output_edges {
                let channel_id = edge.channel.get_channel_id();
                let partition_type = edge.partition_type.clone();
                let target_operator_id = edge.target_operator_id.clone();

                let writer = transport_client.writer.as_ref()
                    .unwrap_or_else(|| panic!("Writer not initialized"));
            
                let partition = partition_type.create();
                
                let collector = collectors.entry(target_operator_id).or_insert_with(|| {
                    Collector::new(
                        writer.clone(),
                        partition,
                    )
                });
                
                collector.add_output_channel_id(channel_id.clone());
            }

            // for collector in collectors.values() {
            //     println!("Collector {:?} output channel ids: {:?}", vertex_id, collector.output_channel_ids());
            //     println!("Writer senders {:?}", collector.data_writer.senders.keys());
            // }

            // panic!("Stop here");
            
            
            operator.open(&runtime_context).await?;
            println!("{:?} Operator {:?} opened", timestamp(), vertex_id);
            
            status.store(StreamTaskStatus::Running as u8, Ordering::SeqCst);
            
            while status.load(Ordering::SeqCst) == StreamTaskStatus::Running as u8 {
                let messages = match operator.operator_type() {
                    crate::runtime::operator::OperatorType::SOURCE => {
                        if let Some(message) = operator.fetch().await {
                            match message {
                                Message::Watermark(watermark) => {
                                    println!("StreamTask {:?} received watermark {:?}", vertex_id, watermark.source_vertex_id);
                                    Self::handle_watermark(
                                        watermark.clone(),
                                        status.clone(),
                                        received_source_ids.clone(),
                                        num_source_vertices,
                                        vertex_id.clone(),
                                    ).await?;
                                    Some(vec![Message::Watermark(watermark)])
                                }
                                _ => {
                                    // println!("StreamTask {:?} received message", vertex_id);
                                    Some(vec![message])
                                }
                            }
                        } else {
                            None
                        }
                    }
                    _ => {
                        let reader = transport_client.reader.as_mut()
                            .expect("Reader should be initialized for non-SOURCE operator");
                        if let Some(message) = reader.read_message().await? {
                            match message {
                                Message::Watermark(watermark) => {
                                    println!("StreamTask {:?} received watermark from {:?}", vertex_id, watermark.source_vertex_id);
                                    Self::handle_watermark(
                                        watermark.clone(),
                                        status.clone(),
                                        received_source_ids.clone(),
                                        num_source_vertices,
                                        vertex_id.clone(),
                                    ).await?;
                                    Some(vec![Message::Watermark(watermark)])
                                }
                                _ => match operator.process_message(message).await {
                                    Some(messages) => {
                                        // println!("StreamTask {:?} received message", vertex_id);
                                        Some(messages)
                                    }
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

                if collectors.len() == 0 {
                    // TODO assert it is sink
                    // println!("StreamTask {:?} no collectors, skipping message", vertex_id);
                    continue;
                }
                
                let messages = messages.unwrap();
                
                for message in messages {
                    let mut channels_to_retry = HashMap::new();
                    for (collector_id, collector) in &collectors {
                        channels_to_retry.insert(collector_id.clone(), collector.output_channel_ids());
                    }
                

                    // TODO retrying collect() updates partition state - fix this

                    let mut retries_before_close = 3;
                    // if vertex_id.contains("sink") {
                    //     println!("StreamTask {:?} retries before close {:?}", vertex_id, retries_before_close);
                    // }
                    while !channels_to_retry.is_empty() {
                        if status.load(Ordering::SeqCst) != StreamTaskStatus::Running as u8 && retries_before_close <= 0 {
                            break;
                        }
                        
                        let successful_channels = Self::collect_message_parallel(
                            &mut collectors, 
                            message.clone(), 
                            Some(channels_to_retry.clone())
                        ).await;
                        
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
                        // println!("StreamTask {:?} retries before close {:?}", vertex_id, retries_before_close);
                    }
                }
            }
            
            operator.close().await?;

            status.store(StreamTaskStatus::Closed as u8, Ordering::SeqCst);
            println!("{:?} StreamTask {:?} closed", timestamp(), vertex_id);
            
            Ok(())
        });
        
        self.run_loop_handle = Some(run_loop_handle);
    }

    fn mark_closing(status: Arc<AtomicU8>, vertex_id: String) {
        println!("StreamTask {:?} closing", vertex_id);
        status.store(StreamTaskStatus::Closing as u8, Ordering::SeqCst);
    }

    pub async fn close_and_wait(&mut self) {
        if self.status.load(Ordering::SeqCst) != StreamTaskStatus::Running as u8 {
            return;
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
    }

    pub fn vertex_id(&self) -> &str {
        &self.vertex_id
    }
}