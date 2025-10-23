use crate::{common::MAX_WATERMARK_VALUE, runtime::{
    collector::Collector, execution_graph::ExecutionGraph, metrics::{get_stream_task_metrics, init_metrics, StreamTaskMetrics, LABEL_VERTEX_ID, METRIC_STREAM_TASK_BYTES_RECV, METRIC_STREAM_TASK_BYTES_SENT, METRIC_STREAM_TASK_LATENCY, METRIC_STREAM_TASK_MESSAGES_RECV, METRIC_STREAM_TASK_MESSAGES_SENT, METRIC_STREAM_TASK_RECORDS_RECV, METRIC_STREAM_TASK_RECORDS_SENT}, operators::operator::{create_operator, OperatorConfig, OperatorTrait, OperatorType, OperatorPollResult, MessageStream}, runtime_context::RuntimeContext
}, storage::storage::Storage, transport::transport_client::TransportClientConfig};
use anyhow::Result;
use futures::StreamExt;
use async_stream::stream;
use metrics::{counter, histogram};
use tokio::{task::JoinHandle, sync::Mutex, sync::watch};
use crate::transport::transport_client::TransportClient;
use crate::common::message::{Message, WatermarkMessage};
use std::{collections::HashMap, sync::{atomic::{AtomicU8, AtomicU64, Ordering}, Arc}, time::{Duration, SystemTime, UNIX_EPOCH}};
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
    ) -> Option<u64> { // Returns new_watermark if advanced
        // For sources (no incoming edges), always advance
        if upstream_vertices.is_empty() {
            let current_wm = current_watermark.load(Ordering::SeqCst);
            if watermark.watermark_value > current_wm {
                current_watermark.store(watermark.watermark_value, Ordering::SeqCst);
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

    // Create preprocessed input stream that handles watermark advancement and metrics
    fn create_preprocessed_input_stream(
        input_stream: MessageStream,
        vertex_id: String,
        upstream_vertices: Vec<String>,
        upstream_watermarks: Arc<Mutex<HashMap<String, u64>>>,
        current_watermark: Arc<AtomicU64>,
        status: Arc<AtomicU8>,
    ) -> MessageStream {
        
        Box::pin(stream! {
            let mut input_stream = input_stream;
            
            while let Some(message) = input_stream.next().await {
                
                Self::record_metrics(vertex_id.clone(), &message, true, false);
                
                match &message {
                    Message::Watermark(watermark) => {
                        println!("StreamTask {:?} received watermark from {:?}", vertex_id, watermark.metadata.upstream_vertex_id);
                        
                        // Advance watermark
                        if let Some(new_watermark) = Self::advance_watermark(
                            watermark.clone(),
                            &upstream_vertices,
                            upstream_watermarks.clone(),
                            current_watermark.clone(),
                        ).await {
                            // Create new watermark message with advanced value
                            let mut wm = watermark.clone();
                            wm.watermark_value = new_watermark;
                            wm.metadata.upstream_vertex_id = Some(vertex_id.clone());
                            let advanced_wm = Message::Watermark(wm);
                            
                            yield advanced_wm;
                        }
                        // If watermark didn't advance, skip it (don't yield anything)
                    }
                    _ => {
                        // Regular message, pass through
                        yield message;
                    }
                }
            }
        })
    }

    // Helper function to send message to collectors (similar to original stream_task.rs)
    async fn send_to_collectors_if_needed(
        mut collectors_per_target_operator: &mut HashMap<String, Collector>,
        message: Message,
        vertex_id: String,
        status: Arc<AtomicU8>
    ) {
        if collectors_per_target_operator.is_empty() {
            // Sink operator - no downstream collectors
            return;
        }

        let mut channels_to_send_per_operator = HashMap::new();
        for (target_operator_id, collector) in collectors_per_target_operator.iter_mut() {
            let partitioned_channels = collector.gen_partitioned_channels(&message);

            channels_to_send_per_operator.insert(target_operator_id.clone(), partitioned_channels);
        }

        // TODO should retires be inside transport?
        let mut retries_before_close = 3; // per - messages
    
        // send message to all destinations until no backpressure
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

    pub async fn start(&mut self) {
        let vertex_id = self.vertex_id.clone();
        let runtime_context = self.runtime_context.clone();
        let status = self.status.clone();
        let operator_config = self.operator_config.clone();
        let execution_graph = self.execution_graph.clone();
        
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

            let operator_type = operator.operator_type();
            let is_source = operator_type == OperatorType::Source || operator_type == OperatorType::ChainedSourceSink;
            let is_sink = operator_type == OperatorType::Sink || operator_type == OperatorType::ChainedSourceSink;
            
            if !is_source {
                // Pre-process input stream for non-source operators
                let reader = transport_client.reader.take()
                    .expect("Reader should be initialized for non-SOURCE operator");
                let input_stream = reader.message_stream();
                
                // Create preprocessing stream (advance watermarks, logging)
                let preprocessed_stream = Self::create_preprocessed_input_stream(
                    input_stream,
                    vertex_id.clone(),
                    upstream_vertices.clone(),
                    upstream_watermarks.clone(),
                    current_watermark.clone(),
                    status.clone()
                );
                operator.set_input(Some(preprocessed_stream))
            };

            while status.load(Ordering::SeqCst) == StreamTaskStatus::Running as u8 {
                // TODO do we need timeout here?
                match operator.poll_next().await {
                    OperatorPollResult::Ready(mut message) => {
                        if is_source {
                            message.set_ingest_timestamp(SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64);
                        }
                        if let Message::Watermark(ref watermark) = message {
                            if is_source {
                                // Advance watermark for source, since it does not have preprocessed input like other operators
                                Self::advance_watermark(
                                    watermark.clone(),
                                    &upstream_vertices,
                                    upstream_watermarks.clone(),
                                    current_watermark.clone(),
                                ).await;
                            }

                            // If operator outputs max watermark, finish task
                            if watermark.watermark_value == MAX_WATERMARK_VALUE {
                                status.store(StreamTaskStatus::Finished as u8, Ordering::SeqCst);
                            }
                        }
                        
                        // Set upstream vertex id for all messages before sending downstream
                        message.set_upstream_vertex_id(vertex_id.clone());
                        Self::record_metrics(vertex_id.clone(), &message, false, is_sink);

                        // Send to collectors (same logic as original)
                        Self::send_to_collectors_if_needed(
                            &mut collectors_per_target_operator,
                            message,
                            vertex_id.clone(),
                            status.clone()
                        ).await;
                    }
                    OperatorPollResult::None => {
                        panic!("Message stream has None");
                    }
                    OperatorPollResult::Continue => {
                        // No output, continue to next iteration
                        continue;
                    }
                }
            }
            
            // Flush and close collectors
            for (_, collector) in collectors_per_target_operator.iter_mut() {
                collector.flush_and_close().await.unwrap();
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
                panic!("Timeout waiting for signal after {:?}, current status is {:?}", timeout, StreamTaskStatus::from(status.load(Ordering::SeqCst)));
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