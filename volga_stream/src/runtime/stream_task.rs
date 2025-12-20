use crate::{common::MAX_WATERMARK_VALUE, runtime::{
    collector::Collector, execution_graph::ExecutionGraph, metrics::{get_stream_task_metrics, init_metrics, StreamTaskMetrics, LABEL_VERTEX_ID, METRIC_STREAM_TASK_BYTES_RECV, METRIC_STREAM_TASK_BYTES_SENT, METRIC_STREAM_TASK_LATENCY, METRIC_STREAM_TASK_MESSAGES_RECV, METRIC_STREAM_TASK_MESSAGES_SENT, METRIC_STREAM_TASK_RECORDS_RECV, METRIC_STREAM_TASK_RECORDS_SENT}, operators::operator::{create_operator, OperatorConfig, OperatorTrait, OperatorType, OperatorPollResult, MessageStream}, runtime_context::RuntimeContext
}, transport::transport_client::TransportClientConfig};
use anyhow::Result;
use futures::StreamExt;
use async_stream::stream;
use metrics::{counter, histogram};
use tokio::{task::JoinHandle, sync::Mutex, sync::watch, sync::mpsc};
use crate::transport::transport_client::DataReaderControl;
use std::collections::HashSet;
use crate::transport::transport_client::TransportClient;
use crate::common::message::{Message, WatermarkMessage};
use std::{collections::HashMap, sync::{atomic::{AtomicU8, AtomicU64, Ordering}, Arc}, time::{Duration, SystemTime, UNIX_EPOCH}};
use serde::{Serialize, Deserialize};
use crate::runtime::master_server::master_service::master_service_client::MasterServiceClient;

#[derive(Debug)]
struct CheckpointAligner {
    upstream_count: usize,
    current_checkpoint: Option<u64>,
    seen_upstreams: HashSet<String>,
    reader_control: DataReaderControl,
}

impl CheckpointAligner {
    fn new(upstream_vertices: &[String], reader_control: DataReaderControl) -> Self {
        Self {
            upstream_count: upstream_vertices.len(),
            current_checkpoint: None,
            seen_upstreams: HashSet::new(),
            reader_control,
        }
    }

    fn on_barrier(&mut self, barrier: &crate::common::message::CheckpointBarrierMessage) -> Option<u64> {
        let upstream_vertex_id = barrier
            .metadata
            .upstream_vertex_id
            .clone()
            .expect("Barrier must have upstream vertex id");

        let checkpoint_id = barrier.checkpoint_id;
        if self.current_checkpoint.is_none() {
            self.current_checkpoint = Some(checkpoint_id);
        }
        if self.current_checkpoint != Some(checkpoint_id) {
            // Ignore unexpected checkpoint for MVP
            return None;
        }

        self.reader_control.block_upstream(&upstream_vertex_id);
        self.seen_upstreams.insert(upstream_vertex_id);

        if self.seen_upstreams.len() == self.upstream_count {
            self.current_checkpoint = None;
            self.seen_upstreams.clear();
            return Some(checkpoint_id);
        }
        None
    }

    // Unblocking is done in StreamTask after checkpointing.
}

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
    checkpoint_trigger_sender: Option<mpsc::UnboundedSender<u64>>,
    upstream_watermarks: Arc<Mutex<HashMap<String, u64>>>, // upstream_vertex_id -> watermark_value
    current_watermark: Arc<AtomicU64>,
    master_addr: Option<String>,
    restore_checkpoint_id: Option<u64>,
}

impl StreamTask {
    pub fn new(
        vertex_id: String,
        operator_config: OperatorConfig,
        transport_client_config: TransportClientConfig,
        runtime_context: RuntimeContext,
        execution_graph: ExecutionGraph,
    ) -> Self {
        init_metrics();
        let master_addr = runtime_context
            .job_config()
            .get("master_addr")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let restore_checkpoint_id = runtime_context
            .job_config()
            .get("restore_checkpoint_id")
            .and_then(|v| v.as_u64());
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
            checkpoint_trigger_sender: None,
            upstream_watermarks: Arc::new(Mutex::new(HashMap::new())),
            current_watermark: Arc::new(AtomicU64::new(0)),
            master_addr,
            restore_checkpoint_id,
        }
    }

    async fn restore_from_master_if_configured(
        operator: &mut dyn OperatorTrait,
        master_client: &mut Option<MasterServiceClient<tonic::transport::Channel>>,
        restore_checkpoint_id: Option<u64>,
        vertex_id: &str,
        task_index: i32,
    ) -> Result<()> {
        let Some(restore_checkpoint_id) = restore_checkpoint_id else {
            return Ok(());
        };
        let Some(client) = master_client.as_mut() else {
            return Ok(());
        };

        println!(
            "[CHECKPOINT] {} task_index={} restoring from checkpoint_id={}",
            vertex_id, task_index, restore_checkpoint_id
        );

        let req = crate::runtime::master_server::master_service::GetTaskCheckpointRequest {
            checkpoint_id: restore_checkpoint_id,
            vertex_id: vertex_id.to_string(),
            task_index,
        };
        let resp = client
            .get_task_checkpoint(tonic::Request::new(req))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get task checkpoint: {}", e))?
            .into_inner();

        if resp.success && !resp.blobs.is_empty() {
            println!(
                "[CHECKPOINT] {} restore found {} blobs",
                vertex_id,
                resp.blobs.len()
            );
            let blobs = resp
                .blobs
                .into_iter()
                .map(|b| (b.name, b.bytes))
                .collect::<Vec<_>>();
            operator.restore(&blobs).await?;
            println!("[CHECKPOINT] {} restore applied", vertex_id);
        } else {
            println!(
                "[CHECKPOINT] {} restore had no blobs (success={})",
                vertex_id, resp.success
            );
        }
        Ok(())
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
    ) {
        let is_control_message = matches!(message, Message::Watermark(_) | Message::CheckpointBarrier(_));
        
        if !is_control_message {
            if recv_or_send {
                counter!(METRIC_STREAM_TASK_MESSAGES_RECV, LABEL_VERTEX_ID => vertex_id.clone()).increment(1);
                counter!(METRIC_STREAM_TASK_RECORDS_RECV, LABEL_VERTEX_ID => vertex_id.clone()).increment(message.num_records() as u64);
                counter!(METRIC_STREAM_TASK_BYTES_RECV, LABEL_VERTEX_ID => vertex_id.clone()).increment(message.get_memory_size() as u64);
                
                let ingest_timestamp= message.ingest_timestamp().unwrap();
                let current_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let latency = current_time.saturating_sub(ingest_timestamp);
                histogram!(METRIC_STREAM_TASK_LATENCY, LABEL_VERTEX_ID => vertex_id.clone()).record(latency as f64);

            } else {
                counter!(METRIC_STREAM_TASK_MESSAGES_SENT, LABEL_VERTEX_ID => vertex_id.clone()).increment(1);
                counter!(METRIC_STREAM_TASK_RECORDS_SENT, LABEL_VERTEX_ID => vertex_id.clone()).increment(message.num_records() as u64);
                counter!(METRIC_STREAM_TASK_BYTES_SENT, LABEL_VERTEX_ID => vertex_id.clone()).increment(message.get_memory_size() as u64);
            }
        }
    }

    // Create preprocessed input stream that handles watermark advancement + barrier alignment (blocks upstreams).
    // It never yields checkpoint barriers to the operator; instead it notifies the run loop via `aligned_checkpoint_sender`.
    fn create_preprocessed_input_stream(
        input_stream: MessageStream,
        vertex_id: String,
        upstream_vertices: Vec<String>,
        upstream_watermarks: Arc<Mutex<HashMap<String, u64>>>,
        current_watermark: Arc<AtomicU64>,
        _status: Arc<AtomicU8>,
        mut aligner: CheckpointAligner,
    ) -> MessageStream {
        Box::pin(stream! {
            let mut input_stream = input_stream;

            while let Some(message) = input_stream.next().await {
                Self::record_metrics(vertex_id.clone(), &message, true);

                match &message {
                    Message::Watermark(watermark) => {
                        // Advance watermark and only forward to operator if advanced.
                        if let Some(new_watermark) = Self::advance_watermark(
                            watermark.clone(),
                            &upstream_vertices,
                            upstream_watermarks.clone(),
                            current_watermark.clone(),
                        ).await {
                            let mut wm = watermark.clone();
                            wm.watermark_value = new_watermark;
                            wm.metadata.upstream_vertex_id = Some(vertex_id.clone());
                            yield Message::Watermark(wm);
                        }
                    }
                    Message::CheckpointBarrier(barrier) => {
                        if let Some(checkpoint_id) = aligner.on_barrier(barrier) {
                            // Once fully aligned, yield a single barrier downstream through the operator.
                            // StreamTask will intercept this barrier, do synchronous checkpointing, and forward it.
                            yield Message::CheckpointBarrier(crate::common::message::CheckpointBarrierMessage::new(
                                vertex_id.clone(),
                                checkpoint_id,
                                None,
                            ));
                        }
                    }
                    _ => {
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
            // Edge operator - no downstream collectors
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
        let master_addr = self.master_addr.clone();
        let restore_checkpoint_id = self.restore_checkpoint_id;
        
        let upstream_watermarks = self.upstream_watermarks.clone();
        let current_watermark = self.current_watermark.clone();
        
        let upstream_vertices: Vec<String> = execution_graph.get_edges_for_vertex(&vertex_id)
            .map(|(input_edges, _)| input_edges.iter().map(|e| e.source_vertex_id.clone()).collect())
            .unwrap_or_default();

        let transport_client_config = self.transport_client_config.take().unwrap();
        
        let (run_sender, run_receiver) = watch::channel(false);
        self.run_signal_sender = Some(run_sender);

        let (close_sender, close_receiver) = watch::channel(false);
        self.close_signal_sender = Some(close_sender);

        let (checkpoint_sender, mut checkpoint_receiver) = mpsc::unbounded_channel::<u64>();
        self.checkpoint_trigger_sender = Some(checkpoint_sender);
        
        // Main stream task lifecycle loop
        let run_loop_handle = tokio::spawn(async move {
            let mut operator = create_operator(operator_config);
            
            let mut transport_client = TransportClient::new(vertex_id.clone(), transport_client_config);
            
            let mut collectors_per_target_operator: HashMap<String, Collector> = HashMap::new();

            let (_, output_edges) = execution_graph.get_edges_for_vertex(&vertex_id).unwrap();
            let num_output_edges = output_edges.len();

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

            // Optional master client (used for restore + checkpoint reporting)
            let mut master_client: Option<MasterServiceClient<tonic::transport::Channel>> = if let Some(master_addr) = master_addr {
                let endpoint = format!("http://{}", master_addr);
                Some(
                    MasterServiceClient::connect(endpoint)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to connect to master service: {}", e))?,
                )
            } else {
                None
            };

            operator.open(&runtime_context).await?;
            println!("{:?} Operator {:?} opened with {} output edges", timestamp(), vertex_id, num_output_edges);

            // Optional restore hook (after open, before run).
            // Important: some operators (e.g. sources) initialize internal state in open();
            // restoring before open would be overwritten by that initialization.
            Self::restore_from_master_if_configured(
                &mut operator,
                &mut master_client,
                restore_checkpoint_id,
                &vertex_id,
                runtime_context.task_index(),
            )
            .await?;

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
            let mut data_reader_control: Option<crate::transport::transport_client::DataReaderControl> = None;
            
            if !is_source {
                // Pre-process input stream for non-source operators
                let reader = transport_client.reader.take()
                    .expect("Reader should be initialized for non-SOURCE operator");
                let (input_stream, reader_control) = reader.message_stream_with_control();
                data_reader_control = Some(reader_control.clone());

                let checkpoint_aligner = CheckpointAligner::new(&upstream_vertices, reader_control);

                let preprocessed_stream = Self::create_preprocessed_input_stream(
                    input_stream,
                    vertex_id.clone(),
                    upstream_vertices.clone(),
                    upstream_watermarks.clone(),
                    current_watermark.clone(),
                    status.clone(),
                    checkpoint_aligner,
                );

                operator.set_input(Some(preprocessed_stream))
            };

            while status.load(Ordering::SeqCst) == StreamTaskStatus::Running as u8 {
                // For sources: if checkpoint is triggered, synthesize a CheckpointBarrier message and treat it exactly
                // like a poll_next() output (same code path, no duplication).
                let produced = if is_source && crate::runtime::operators::operator::operator_config_requires_checkpoint(operator.operator_config()) {
                    match checkpoint_receiver.try_recv() {
                        Ok(checkpoint_id) => {
                            println!(
                                "[CHECKPOINT] {} received trigger for checkpoint_id={}",
                                vertex_id, checkpoint_id
                            );
                            Some(Message::CheckpointBarrier(
                                crate::common::message::CheckpointBarrierMessage::new(
                                    vertex_id.clone(),
                                    checkpoint_id,
                                    None,
                                ),
                            ))
                        }
                        Err(mpsc::error::TryRecvError::Empty) => None,
                        Err(mpsc::error::TryRecvError::Disconnected) => None,
                    }
                } else {
                    None
                };

                let poll_res = if let Some(msg) = produced {
                    OperatorPollResult::Ready(msg)
                } else {
                    operator.poll_next().await
                };

                match poll_res {
                    OperatorPollResult::Ready(mut message) => {
                        if let Message::CheckpointBarrier(ref barrier) = message {
                            let checkpoint_id = barrier.checkpoint_id;

                            if crate::runtime::operators::operator::operator_config_requires_checkpoint(operator.operator_config()) {
                                println!(
                                    "[CHECKPOINT] {} checkpointing checkpoint_id={}",
                                    vertex_id, checkpoint_id
                                );
                                let blobs = operator.checkpoint(checkpoint_id).await?;
                                println!(
                                    "[CHECKPOINT] {} checkpointed {} blobs, reporting...",
                                    vertex_id,
                                    blobs.len()
                                );

                                master_client
                                    .as_mut()
                                    .expect("Master client not initialized")
                                    .report_checkpoint(tonic::Request::new(
                                        crate::runtime::master_server::master_service::ReportCheckpointRequest {
                                            checkpoint_id,
                                            vertex_id: vertex_id.clone(),
                                            task_index: runtime_context.task_index(),
                                            blobs: blobs
                                                .into_iter()
                                                .map(|(name, bytes)| crate::runtime::master_server::master_service::StateBlob { name, bytes })
                                                .collect(),
                                        },
                                    ))
                                    .await?;
                                println!(
                                    "[CHECKPOINT] {} reported checkpoint_id={}",
                                    vertex_id, checkpoint_id
                                );
                            }

                            // Resume input after checkpointing
                            if let Some(ctrl) = &data_reader_control {
                                ctrl.unblock_all();
                            }
                        }

                        // if vertex_id == "Window_1_2" {
                        //     // todo remove after debugging
                        //     println!("StreamTask {:?} produced message {:?}", vertex_id, message);
                        // }
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
                            println!("StreamTask {:?} produced watermark {:?}", vertex_id, watermark.watermark_value);
                        }
                        
                        // Set upstream vertex id for all messages before sending downstream
                        message.set_upstream_vertex_id(vertex_id.clone());
                        Self::record_metrics(vertex_id.clone(), &message, false);

                        // Send to collectors
                        Self::send_to_collectors_if_needed(
                            &mut collectors_per_target_operator,
                            message,
                            vertex_id.clone(),
                            status.clone()
                        ).await;
                    }
                    OperatorPollResult::None => panic!("Message stream has None"),
                    OperatorPollResult::Continue => { /* no output */ }
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

    pub fn signal_trigger_checkpoint(&mut self, checkpoint_id: u64) {
        let sender = self.checkpoint_trigger_sender.as_ref().expect("checkpoint trigger sender not set");
        let _ = sender.send(checkpoint_id);
    }

    async fn wait_for_signal(mut receiver: watch::Receiver<bool>, status: Arc<AtomicU8>, skip_on_finished: bool) {
        let timeout = Duration::from_millis(50000);
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