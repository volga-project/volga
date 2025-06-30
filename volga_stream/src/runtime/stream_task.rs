use crate::{common::MAX_WATERMARK_VALUE, runtime::{
    collector::Collector, execution_graph::ExecutionGraph, operators::operator::{create_operator_from_config, OperatorConfig, OperatorTrait, OperatorType}, runtime_context::RuntimeContext
}, transport::transport_client::TransportClientConfig};
use anyhow::Result;
use tokio::{task::JoinHandle, sync::Mutex, sync::watch};
use crate::transport::transport_client::TransportClient;
use crate::common::message::{Message, WatermarkMessage};
use std::{collections::HashMap, sync::{atomic::{AtomicBool, AtomicU8, Ordering}, Arc}, time::{Duration, SystemTime, UNIX_EPOCH}};
use std::collections::HashSet;
use serde::{Serialize, Deserialize};


// Helper function to get current timestamp
fn timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamTaskStatus {
    Created = 0,
    Opened = 1,
    Running = 2,
    Finished = 3,
    Closed = 4,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamTaskMetrics {
    pub vertex_id: String,
    pub latency_histogram: Vec<u64>, // Simple histogram: [0-1ms, 1-10ms, 10-100ms, 100ms-1s, >1s]
    pub num_messages: u64,
    pub num_records: u64,
}

impl StreamTaskMetrics {
    pub fn new(vertex_id: String) -> Self {
        Self {
            vertex_id,
            latency_histogram: vec![0, 0, 0, 0, 0], // 5 buckets
            num_messages: 0,
            num_records: 0,
        }
    }

    pub fn update_latency(&mut self, latency_ms: u64) {
        let bucket = match latency_ms {
            0..=1 => 0,
            2..=10 => 1,
            11..=100 => 2,
            101..=1000 => 3,
            _ => 4,
        };
        self.latency_histogram[bucket] += 1;
    }

    pub fn increment_messages(&mut self) {
        self.num_messages += 1;
    }

    pub fn add_records(&mut self, count: usize) {
        self.num_records += count as u64;
    }
}

#[derive(Debug, Clone)]
pub struct StreamTaskState {
    pub vertex_id: String,
    pub status: StreamTaskStatus,
    pub metrics: StreamTaskMetrics,
}

impl From<u8> for StreamTaskStatus {
    fn from(value: u8) -> Self {
        match value {
            0 => StreamTaskStatus::Created,
            1 => StreamTaskStatus::Opened,
            2 => StreamTaskStatus::Running,
            3 => StreamTaskStatus::Finished,
            4 => StreamTaskStatus::Closed,
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
    finished_upstream_ids: Arc<Mutex<HashSet<String>>>, // tracks which upstream vertices have finished
    metrics: Arc<Mutex<StreamTaskMetrics>>,
    run_signal_sender: Option<watch::Sender<bool>>,
    close_signal_sender: Option<watch::Sender<bool>>,
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
            vertex_id: vertex_id.clone(),
            runtime_context,
            status: Arc::new(AtomicU8::new(StreamTaskStatus::Created as u8)),
            run_loop_handle: None,
            operator_config,
            transport_client_config: Some(transport_client_config),
            execution_graph,
            finished_upstream_ids: Arc::new(Mutex::new(HashSet::new())),
            metrics: Arc::new(Mutex::new(StreamTaskMetrics::new(vertex_id.clone()))),
            run_signal_sender: None,
            close_signal_sender: None,
        }
    }

    async fn handle_watermark(
        is_source: bool,
        watermark: WatermarkMessage,
        status: Arc<AtomicU8>,
        finished_upstream_ids: Arc<Mutex<HashSet<String>>>,
        upstream_vertices: Vec<String>,
        vertex_id: String
    ) -> Option<WatermarkMessage> {
        if watermark.watermark_value == MAX_WATERMARK_VALUE {
            if is_source {
                // println!("source vertex_id {:?} received max watermark, initiating shutdown", 
                //     vertex_id);
                status.store(StreamTaskStatus::Finished as u8, Ordering::SeqCst);
                return Some(WatermarkMessage::new(vertex_id, MAX_WATERMARK_VALUE, watermark.ingest_timestamp));
            }

            let upstream_vertex_id = watermark.upstream_vertex_id.clone();
            
            let mut finished_upstreams = finished_upstream_ids.lock().await;
            finished_upstreams.insert(upstream_vertex_id);
            // println!("vertex_id {:?}, finished upstreams {:?}", vertex_id, finished_upstreams);
            
            // If we've received max watermarks from all upstreams, initiate shutdown
            let upstream_vertices_set: HashSet<String> = upstream_vertices.iter().cloned().collect();
            if finished_upstreams.len() == upstream_vertices_set.len() && 
               upstream_vertices_set.iter().all(|id| finished_upstreams.contains(id)) {
                // println!("vertex_id {:?} Received max watermarks from all upstreams {:?}, initiating shutdown", 
                //     vertex_id, finished_upstreams);
                status.store(StreamTaskStatus::Finished as u8, Ordering::SeqCst);
                return Some(WatermarkMessage::new(vertex_id, MAX_WATERMARK_VALUE, watermark.ingest_timestamp));
            }
        }
        None
    }

    async fn update_metrics(
        metrics: Arc<Mutex<StreamTaskMetrics>>,
        message: Message,
    ) {
        let is_watermark = matches!(message, Message::Watermark(_));
        
        if !is_watermark {
            let mut metrics_guard = metrics.lock().await;
            metrics_guard.increment_messages();
            metrics_guard.add_records(message.record_batch().num_rows());
        
            
            // Update latency if available
            let ingest_timestamp= message.ingest_timestamp().unwrap();
            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let latency = current_time.saturating_sub(ingest_timestamp);
            metrics_guard.update_latency(latency);
        }
    }

    pub async fn start(&mut self) {
        let vertex_id = self.vertex_id.clone();
        let runtime_context = self.runtime_context.clone();
        let status = self.status.clone();
        let operator_config = self.operator_config.clone();
        let execution_graph = self.execution_graph.clone();
        let finished_upstream_ids = self.finished_upstream_ids.clone();
        let metrics = self.metrics.clone();
        
        let upstream_vertices: Vec<String> = execution_graph.get_edges_for_vertex(&vertex_id)
            .map(|(input_edges, _)| input_edges.iter().map(|e| e.source_vertex_id.clone()).collect())
            .unwrap_or_default();

        let transport_client_config = self.transport_client_config.take().unwrap();
        
        let (run_sender, run_receiver) = watch::channel(false);
        self.run_signal_sender = Some(run_sender);

        let (close_sender, close_receiver) = watch::channel(false);
        self.close_signal_sender = Some(close_sender);
        
        // Main stream task lifecycle loop
        let run_loop_handle = tokio::spawn(async move {
            let mut operator = create_operator_from_config(operator_config);
            
            let mut transport_client = TransportClient::new(vertex_id.clone(), transport_client_config);
            
            let mut collectors_per_target_operator: HashMap<String, Collector> = HashMap::new();

            let (_, output_edges) = execution_graph.get_edges_for_vertex(&vertex_id).unwrap();

            for edge in output_edges {
                let channel_id = edge.channel.get_channel_id();
                let partition_type = edge.partition_type.clone();
                let target_operator_id = edge.target_operator_id.clone();

                let writer = transport_client.writer.as_ref()
                    .unwrap_or_else(|| panic!("Writer not initialized"));
            
                let partition = partition_type.create();
                
                let collector = collectors_per_target_operator.entry(target_operator_id).or_insert_with(|| {
                    Collector::new(
                        writer.clone(),
                        partition,
                    )
                });
                
                collector.add_output_channel_id(channel_id.clone());
            }

            operator.open(&runtime_context).await?;
            println!("{:?} Operator {:?} opened", timestamp(), vertex_id);

            // if task is not finished/closed early, mark as opened
            if status.load(Ordering::SeqCst) != StreamTaskStatus::Finished as u8 && status.load(Ordering::SeqCst) != StreamTaskStatus::Closed as u8 {
                status.store(StreamTaskStatus::Opened as u8, Ordering::SeqCst);
            }
            
            // Wait for signal to start processing
            println!("{:?} Task {:?} waiting for run signal", timestamp(), vertex_id);
            Self::wait_for_signal(run_receiver, status.clone(), true).await;
            println!("{:?} Task {:?} received run signal, starting processing", timestamp(), vertex_id);
            

            // if task is not finished/closed early, mark as runnning
            if status.load(Ordering::SeqCst) != StreamTaskStatus::Finished as u8 && status.load(Ordering::SeqCst) != StreamTaskStatus::Closed as u8 {
                status.store(StreamTaskStatus::Running as u8, Ordering::SeqCst);
            }
            
            // processing loop
            while status.load(Ordering::SeqCst) == StreamTaskStatus::Running as u8 {
                let operator_type = operator.operator_type();
                let is_source = operator_type == OperatorType::Source || operator_type == OperatorType::ChainedSourceSink;
                let mut processed_messages: Option<Vec<Message>> = None;
                
                if is_source {
                    if let Some(fetched_msgs) = operator.fetch().await {
                        let mut filtered = vec![]; // remove watermarks if needed, etc.
                        for mut message in fetched_msgs {
                            // source should set ingest timestamp
                            message.set_ingest_timestamp(SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64);

                            Self::update_metrics(metrics.clone(), message.clone()).await;
                            match message {
                                Message::Watermark(watermark) => {
                                    println!("StreamTask {:?} received watermark {:?}", vertex_id, watermark.upstream_vertex_id);
                                    let wm = Self::handle_watermark(
                                        is_source,
                                        watermark.clone(),
                                        status.clone(),
                                        finished_upstream_ids.clone(),
                                        upstream_vertices.clone(),
                                        vertex_id.clone()
                                    ).await;
                                    if let Some(wm) = wm {
                                        filtered.push(Message::Watermark(wm));
                                    }
                                }
                                _ => {
                                    filtered.push(message)
                                }
                            }
                        }
                        if filtered.len() != 0 {
                            processed_messages = Some(filtered)
                        }
                    }
                } else { 
                    let reader = transport_client.reader.as_mut()
                        .expect("Reader should be initialized for non-SOURCE operator");
                    if let Some(message) = reader.read_message().await? {
                        Self::update_metrics(metrics.clone(), message.clone()).await;

                        match message {
                            Message::Watermark(watermark) => {
                                println!("StreamTask {:?} received watermark from {:?}", vertex_id, watermark.upstream_vertex_id);
                                let wm = Self::handle_watermark(
                                    is_source,
                                    watermark.clone(),
                                    status.clone(),
                                    finished_upstream_ids.clone(),
                                    upstream_vertices.clone(),
                                    vertex_id.clone()
                                ).await;
                                if let Some(wm) = wm {
                                    processed_messages = Some(vec![Message::Watermark(wm)])
                                }
                            }
                            _ => {
                                processed_messages = operator.process_message(message).await;
                            }
                        }
                    } 
                }

                if processed_messages.is_none() {
                    continue;
                }

                if collectors_per_target_operator.len() == 0 {
                    // TODO assert it is sink
                    continue;
                }
                
                let messages = processed_messages.unwrap();
                
                let mut retries_before_close = 3; // shared between all messages
                for mut message in messages {
                    // set upstream vertex id for all messages before sending downstream
                    message.set_upstream_vertex_id(vertex_id.clone());

                    let mut channels_to_send_per_operator = HashMap::new();
                    for (target_operator_id, collector) in &mut collectors_per_target_operator {
                        let partitioned_channel_ids = collector.gen_partitioned_channel_ids(message.clone());
                        channels_to_send_per_operator.insert(target_operator_id.clone(), partitioned_channel_ids);
                    }
                    
                    // send message to all destinations until no backpressure
                    // TODO track backpreessure stats
                    while status.load(Ordering::SeqCst) == StreamTaskStatus::Running as u8 ||
                        status.load(Ordering::SeqCst) == StreamTaskStatus::Finished as u8 {

                        if status.load(Ordering::SeqCst) == StreamTaskStatus::Finished as u8 {
                            if retries_before_close == 0 {
                                break;
                            }
                            retries_before_close -= 1;
                        }

                        let write_results = Collector::write_message_to_operators(
                            &mut collectors_per_target_operator, 
                            message.clone(), 
                            channels_to_send_per_operator.clone()
                        ).await;
                        
                        channels_to_send_per_operator.clear();
                        for (target_operator_id, write_res) in write_results {
                            let mut resend_channels = vec![];
                            for channel_id in write_res.keys() {
                                let (success, backpressure_time_ms) = write_res.get(channel_id).unwrap();
                                if !success {
                                    resend_channels.push(channel_id.clone());
                                }
                            }
                            if !resend_channels.is_empty() {
                                channels_to_send_per_operator.insert(target_operator_id.clone(), resend_channels);
                            }
                        }
                        if channels_to_send_per_operator.len() == 0 {
                            break;
                        }
                    }
                }
            }
            
            println!("{:?} Task {:?} waiting for close signal", timestamp(), vertex_id);
            Self::wait_for_signal(close_receiver, status.clone(), false).await;
            println!("{:?} Task {:?} received close signal, performing close", timestamp(), vertex_id);

            operator.close().await?;

            status.store(StreamTaskStatus::Closed as u8, Ordering::SeqCst);
            println!("{:?} StreamTask {:?} closed", timestamp(), vertex_id);
            
            Ok(())
        });
        
        self.run_loop_handle = Some(run_loop_handle);
    }

    pub async fn get_state(&self) -> StreamTaskState {
        StreamTaskState {
            vertex_id: self.vertex_id.clone(),
            status: StreamTaskStatus::from(self.status.load(Ordering::SeqCst)),
            metrics: self.metrics.lock().await.clone(),
        }
    }

    pub fn signal_to_run(&mut self) {
        let run_signal_sender = self.run_signal_sender.as_ref().unwrap();
        let _ = run_signal_sender.send(true);
    }

    pub fn signal_to_close(&mut self) {
        let close_signal_sender = self.close_signal_sender.as_ref().unwrap();
        let _ = close_signal_sender.send(true);
    }

    async fn wait_for_signal(mut receiver: watch::Receiver<bool>, status: Arc<AtomicU8>, skip_on_finished: bool) {
        let timeout = Duration::from_millis(5000);
        let start_time = SystemTime::now();
        
        loop {
            if skip_on_finished {
                let current_status = status.load(Ordering::SeqCst);
                if current_status == StreamTaskStatus::Finished as u8 || current_status == StreamTaskStatus::Closed as u8 {
                    println!("{:?} Task status is {:?}, stopping wait for signal", timestamp(), StreamTaskStatus::from(current_status));
                    return;
                }
            }

            if start_time.elapsed().unwrap_or_default() > timeout {
                panic!("Timeout waiting for signal after {:?}", timeout);
            }

            match tokio::time::timeout(Duration::from_millis(50), receiver.changed()).await {
                Ok(_) => {
                    // Signal received
                    return;
                }
                Err(_) => {
                    // Timeout, continue loop to check status
                    continue;
                }
            }
        }
    }
}