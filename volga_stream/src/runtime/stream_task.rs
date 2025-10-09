use crate::{common::MAX_WATERMARK_VALUE, runtime::{
    collector::Collector, execution_graph::ExecutionGraph, metrics::{get_stream_task_metrics, init_metrics, StreamTaskMetrics, LABEL_VERTEX_ID, METRIC_STREAM_TASK_BYTES_RECV, METRIC_STREAM_TASK_BYTES_SENT, METRIC_STREAM_TASK_LATENCY, METRIC_STREAM_TASK_MESSAGES_RECV, METRIC_STREAM_TASK_MESSAGES_SENT, METRIC_STREAM_TASK_RECORDS_RECV, METRIC_STREAM_TASK_RECORDS_SENT}, operators::operator::{create_operator, OperatorConfig, OperatorTrait, OperatorType}, runtime_context::RuntimeContext
}, storage::storage::Storage, transport::transport_client::TransportClientConfig};
use anyhow::Result;
use metrics::{counter, histogram};
use tokio::{task::JoinHandle, sync::Mutex, sync::watch};
use crate::transport::transport_client::TransportClient;
use crate::common::message::{Message, WatermarkMessage};
use std::{collections::HashMap, sync::{atomic::{AtomicU8, AtomicU64, Ordering}, Arc}, time::{Duration, SystemTime, UNIX_EPOCH}};
use serde::{Serialize, Deserialize};

// Latency histogram bucket configuration
// pub const LATENCY_BUCKET_BOUNDARIES: [u64; 5] = [1, 10, 100, 1000, u64::MAX];

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
    run_signal_sender: Option<watch::Sender<bool>>,
    close_signal_sender: Option<watch::Sender<bool>>,
    upstream_watermarks: Arc<Mutex<HashMap<String, u64>>>, // upstream_vertex_id -> watermark_value
    current_watermark: Arc<AtomicU64>,
    storage: Arc<Storage>,
}

impl StreamTask {
    pub fn new(
        vertex_id: String,
        operator_config: OperatorConfig,
        transport_client_config: TransportClientConfig,
        runtime_context: RuntimeContext,
        execution_graph: ExecutionGraph,
        storage: Arc<Storage>,
    ) -> Self {
        init_metrics();
        Self {
            vertex_id: vertex_id.clone(),
            runtime_context,
            status: Arc::new(AtomicU8::new(StreamTaskStatus::Created as u8)),
            run_loop_handle: None,
            operator_config,
            transport_client_config: Some(transport_client_config),
            execution_graph,
            // metrics: Arc::new(Mutex::new(StreamTaskMetrics::new(vertex_id.clone()))),
            run_signal_sender: None,
            close_signal_sender: None,
            upstream_watermarks: Arc::new(Mutex::new(HashMap::new())),
            current_watermark: Arc::new(AtomicU64::new(0)),
            storage,
        }
    }

    async fn advance_watermark(
        watermark: WatermarkMessage,
        upstream_vertices: &[String],
        upstream_watermarks: Arc<Mutex<HashMap<String, u64>>>,
        current_watermark: Arc<AtomicU64>,
        status: Arc<AtomicU8>,
        _vertex_id: String,
    ) -> Option<u64> { // Returns new_watermark if advanced
        // For sources (no incoming edges), always advance
        if upstream_vertices.is_empty() {
            let current_wm = current_watermark.load(Ordering::SeqCst);
            if watermark.watermark_value > current_wm {
                current_watermark.store(watermark.watermark_value, Ordering::SeqCst);
                if watermark.watermark_value == MAX_WATERMARK_VALUE {
                    status.store(StreamTaskStatus::Finished as u8, Ordering::SeqCst);
                }
                return Some(watermark.watermark_value);
            }
            return None;
        }
        
        // For non-sources, handle upstream watermark tracking
        let upstream_vertex_id = watermark.metadata.upstream_vertex_id.clone()
            .expect("Non-source tasks must have upstream_vertex_id in watermark metadata");
        
        let mut upstream_wms = upstream_watermarks.lock().await;
        
        // Assert that watermark increases for given upstream
        if let Some(&previous_watermark) = upstream_wms.get(&upstream_vertex_id) {
            assert!(
                watermark.watermark_value >= previous_watermark,
                "Watermark must not decrease: received {} but previous was {} from upstream {}",
                watermark.watermark_value, previous_watermark, upstream_vertex_id
            );
        }
        
        // Update watermark for this upstream
        upstream_wms.insert(upstream_vertex_id, watermark.watermark_value);
        
        // Only advance when we have watermarks from ALL upstreams
        if upstream_wms.len() < upstream_vertices.len() {
            return None; // Haven't received watermarks from all upstreams yet
        }
        
        // Calculate minimum watermark across all upstreams
        let min_watermark = upstream_wms.values().min().copied().unwrap_or(0);
        drop(upstream_wms); // Release lock
        
        let current_wm = current_watermark.load(Ordering::SeqCst);
        if min_watermark > current_wm {
            current_watermark.store(min_watermark, Ordering::SeqCst);
            if min_watermark == MAX_WATERMARK_VALUE {
                status.store(StreamTaskStatus::Finished as u8, Ordering::SeqCst);
            }
            Some(min_watermark)
        } else {
            None // Watermark didn't advance
        }
    }

    fn record_metrics(
        vertex_id: String,
        message: &Message,
        recv_or_send: bool,
        is_sink: bool
    ) {
        let is_watermark = matches!(message, Message::Watermark(_));
        
        if !is_watermark {
            if recv_or_send {
                counter!(METRIC_STREAM_TASK_MESSAGES_RECV, LABEL_VERTEX_ID => vertex_id.clone()).increment(1);
                counter!(METRIC_STREAM_TASK_RECORDS_RECV, LABEL_VERTEX_ID => vertex_id.clone()).increment(message.num_records() as u64);
                counter!(METRIC_STREAM_TASK_BYTES_RECV, LABEL_VERTEX_ID => vertex_id.clone()).increment(message.get_memory_size() as u64);
                if is_sink {
                    // record latency for sink only
                    let ingest_timestamp= message.ingest_timestamp().unwrap();
                    let current_time = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let latency = current_time.saturating_sub(ingest_timestamp);
                    histogram!(METRIC_STREAM_TASK_LATENCY, LABEL_VERTEX_ID => vertex_id.clone()).record(latency as f64);
                }

            } else {
                counter!(METRIC_STREAM_TASK_MESSAGES_SENT, LABEL_VERTEX_ID => vertex_id.clone()).increment(1);
                counter!(METRIC_STREAM_TASK_RECORDS_SENT, LABEL_VERTEX_ID => vertex_id.clone()).increment(message.num_records() as u64);
                counter!(METRIC_STREAM_TASK_BYTES_SENT, LABEL_VERTEX_ID => vertex_id.clone()).increment(message.get_memory_size() as u64);
            }
        }
    }

    pub async fn start(&mut self) {
        let vertex_id = self.vertex_id.clone();
        let runtime_context = self.runtime_context.clone();
        let status = self.status.clone();
        let operator_config = self.operator_config.clone();
        let execution_graph = self.execution_graph.clone();
        // let metrics = self.metrics.clone();
        let upstream_watermarks = self.upstream_watermarks.clone();
        let current_watermark = self.current_watermark.clone();
        let storage = self.storage.clone();
        
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
            let mut operator = create_operator(operator_config, storage.clone());
            
            let mut transport_client = TransportClient::new(vertex_id.clone(), transport_client_config);
            
            let mut collectors_per_target_operator: HashMap<String, Collector> = HashMap::new();

            let (_, output_edges) = execution_graph.get_edges_for_vertex(&vertex_id).unwrap();

            for edge in output_edges {
                let channel = edge.get_channel();
                // let channel_id = channel.get_channel_id();
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
                
                collector.add_output_channel(channel);
            }

            // start collectors
            for (_, collector) in collectors_per_target_operator.iter_mut() {
                collector.start().await;
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
                let is_sink = operator_type == OperatorType::Sink || operator_type == OperatorType::ChainedSourceSink;
                let mut produced_messages: Option<Vec<Message>> = None;
                
                if is_source {
                    if let Some(fetched_msgs) = operator.fetch().await {
                        let mut msgs = vec![]; // remove watermarks if needed, etc.
                        for mut message in fetched_msgs {
                            // source should set ingest timestamp
                            message.set_ingest_timestamp(SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64);
                            Self::record_metrics(vertex_id.clone(), &message, true, false);
                            match message {
                                Message::Watermark(watermark) => {
                                    println!("StreamTask {:?} received watermark {:?}", vertex_id, watermark.metadata.upstream_vertex_id);
                                    let new_wm = Self::advance_watermark(
                                        watermark.clone(),
                                        &upstream_vertices,
                                        upstream_watermarks.clone(),
                                        current_watermark.clone(),
                                        status.clone(),
                                        vertex_id.clone(),
                                    ).await;
                                    if let Some(wm_value) = new_wm {
                                        let mut wm = watermark.clone();
                                        wm.watermark_value = wm_value;
                                        wm.metadata.upstream_vertex_id = Some(vertex_id.clone());
                                        msgs.push(Message::Watermark(wm));
                                    }
                                }
                                _ => {
                                    msgs.push(message)
                                }
                            }
                        }
                        if msgs.len() != 0 {
                            produced_messages = Some(msgs)
                        }
                    }
                } else { 
                    let reader = transport_client.reader.as_mut()
                        .expect("Reader should be initialized for non-SOURCE operator");
                    if let Some(message) = reader.read_message().await? {
                        Self::record_metrics(vertex_id.clone(), &message, true, false);  
                        match message {
                            Message::Watermark(watermark) => {
                                println!("StreamTask {:?} received watermark from {:?}", vertex_id, watermark.metadata.upstream_vertex_id);
                                if let Some(new_watermark) = Self::advance_watermark(
                                    watermark.clone(),
                                    &upstream_vertices,
                                    upstream_watermarks.clone(),
                                    current_watermark.clone(),
                                    status.clone(),
                                    vertex_id.clone(),
                                ).await {
                                    // Watermark advanced, notify operator
                                    let msgs = operator.process_watermark(new_watermark).await;
                                    
                                    let wm = WatermarkMessage::new(
                                        vertex_id.clone(), 
                                        new_watermark, 
                                        watermark.metadata.ingest_timestamp
                                    );
                                    
                                    if let Some(mut msgs_vec) = msgs {
                                        msgs_vec.push(Message::Watermark(wm));
                                        produced_messages = Some(msgs_vec);
                                    } else {
                                        produced_messages = Some(vec![Message::Watermark(wm)]);
                                    }
                                }
                            }
                            _ => {
                                produced_messages = operator.process_message(message.clone()).await;
                            }
                        }
                    } 
                }

                if produced_messages.is_none() {
                    continue;
                }

                if collectors_per_target_operator.len() == 0 {
                    // TODO assert it is sink
                    continue;
                }
                
                let messages = produced_messages.unwrap();
                
                for mut message in messages {
                    // set upstream vertex id for all messages before sending downstream
                    message.set_upstream_vertex_id(vertex_id.clone());
                    Self::record_metrics(vertex_id.clone(), &message, false, is_sink);

                    let mut channels_to_send_per_operator = HashMap::new();
                    for (target_operator_id, collector) in &mut collectors_per_target_operator {
                        let partitioned_channels = collector.gen_partitioned_channels(&message);

                        channels_to_send_per_operator.insert(target_operator_id.clone(), partitioned_channels);
                    }

                    // TODO should retires be inside transport?
                    let mut retries_before_close = 3; // per - messages
                
                    // send message to all destinations until no backpressure
                    // TODO track backpreessure stats
                    while status.load(Ordering::SeqCst) == StreamTaskStatus::Running as u8 ||
                        status.load(Ordering::SeqCst) == StreamTaskStatus::Finished as u8 {
                        
                        if status.load(Ordering::SeqCst) == StreamTaskStatus::Finished as u8 {
                            if retries_before_close == 0 {
                                panic!("StreamTask {:?} gave up on writing message {:?}", vertex_id, message);
                            }
                            retries_before_close -= 1;
                        }

                        let write_results = Collector::write_message_to_operators(
                            &mut collectors_per_target_operator, 
                            &message, 
                            channels_to_send_per_operator.clone()
                        ).await;
                        
                        channels_to_send_per_operator.clear();
                        for (target_operator_id, write_res) in write_results {
                            let mut resend_channels = vec![];
                            for channel in write_res.keys() {
                                let (success, _backpressure_time_ms) = write_res.get(channel).unwrap();
                                if !success {
                                    resend_channels.push(channel.clone());
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

            // flush all buffered messages and close collector
            // start collectors
            for (_, collector) in collectors_per_target_operator.iter_mut() {
                collector.flush_and_close().await.unwrap(); // TODO proper error handle
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
            metrics: get_stream_task_metrics(self.vertex_id.clone()),
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