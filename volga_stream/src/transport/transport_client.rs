use anyhow::Result;
use metrics::gauge;
use std::collections::HashMap;
use crate::{common::message::Message, runtime::metrics::{LABEL_TARGET_VERTEX_ID, LABEL_VERTEX_ID, METRIC_STREAM_TASK_BACKPRESSURE_RATIO, METRIC_STREAM_TASK_BYTES_RECV, METRIC_STREAM_TASK_BYTES_SENT, METRIC_STREAM_TASK_MESSAGES_RECV, METRIC_STREAM_TASK_MESSAGES_SENT, METRIC_STREAM_TASK_RECORDS_RECV, METRIC_STREAM_TASK_RECORDS_SENT, METRIC_STREAM_TASK_TX_QUEUE_REM, METRIC_STREAM_TASK_TX_QUEUE_SIZE}, transport::{batch_channel::{BatchReceiver, BatchSender}, channel::Channel}};
use std::time::Duration;
use tokio::{sync::mpsc::error::SendError, time};
use crate::transport::batcher::{Batcher, BatcherConfig};

#[derive(Debug)]
pub struct TransportClientConfig {
    pub vertex_id: String,
    pub reader_receivers: Option<HashMap<String, BatchReceiver>>,
    pub writer_senders: Option<HashMap<String, BatchSender>>,
}

impl TransportClientConfig {
    pub fn new(vertex_id: String) -> Self {
        Self {
            vertex_id,
            reader_receivers: None,
            writer_senders: None,
        }
    }

    pub fn add_reader_receiver(&mut self, channel_id: String, receiver: BatchReceiver) {
        if self.reader_receivers.is_none() {
            self.reader_receivers = Some(HashMap::new());
        }
        self.reader_receivers.as_mut().unwrap().insert(channel_id, receiver);
    }

    pub fn add_writer_sender(&mut self, channel_id: String, sender: BatchSender) {
        if self.writer_senders.is_none() {
            self.writer_senders = Some(HashMap::new());
        }
        self.writer_senders.as_mut().unwrap().insert(channel_id, sender);
    }
}

#[derive(Debug)]
pub struct DataReader {
    vertex_id: String,
    receivers: HashMap<String, BatchReceiver>,
    default_timeout: Duration,
    default_retries: usize,
}

impl DataReader {
    pub fn new(vertex_id: String, receivers: HashMap<String, BatchReceiver>) -> Self {
        Self {
            vertex_id,
            receivers,
            default_timeout: Duration::from_millis(100),
            default_retries: 0,
        }
    }

    pub async fn read_message(&mut self) -> Result<Option<Message>> {
        self.read_message_with_params(None, None).await
    }

    async fn read_message_with_params(
        &mut self,
        timeout_duration: Option<Duration>,
        retries: Option<usize>
    ) -> Result<Option<Message>> {
        let timeout_duration = timeout_duration.unwrap_or(self.default_timeout);
        let retries = retries.unwrap_or(self.default_retries);
        let mut attempts = 0;

        while attempts <= retries {
            if self.receivers.is_empty() {
                panic!("Attempted to read message from DataReader {} with no channels registered", self.vertex_id);
            }

            // Create a future that completes when any channel has data
            let mut futures = Vec::new();
            for (channel_id, receiver) in self.receivers.iter_mut() {
                // let vertex_id = self.vertex_id.clone();
                futures.push(Box::pin(async move {
                    match time::timeout(timeout_duration, receiver.recv()).await {
                        Ok(Some(message)) => Some((channel_id.clone(), message)),
                        Ok(None) => {
                            panic!("DataReader channel {} closed", channel_id);
                        },
                        Err(_) => {
                            // println!("DataReader {:?} timeout", vertex_id);
                            None
                        },
                    }
                }));
            }

            // Wait for the first channel to have data
            let result = tokio::select! {
                result = futures::future::select_all(futures) => {
                    match result.0 {
                        Some((channel_id, message)) => Some(message),
                        None => None,
                    }
                }
            };

            if result.is_some() {
                return Ok(result);
            }
            attempts += 1;
        }
        Ok(None)
    }
}

#[derive(Debug, Clone)]
pub struct DataWriter {
    pub vertex_id: String,
    pub senders: HashMap<String, BatchSender>,
    // batcher: Batcher,
    default_timeout: Duration,
    default_retries: usize,
    batching_config: BatcherConfig,
}

impl DataWriter {
    pub fn new(vertex_id: String, senders: HashMap<String, BatchSender>) -> Self {
        let batching_config = BatcherConfig::default();
        // let batcher = Batcher::new(batching_config.clone(), senders.clone());
        
        Self {
            vertex_id,
            senders,
            // batcher,
            default_timeout: Duration::from_millis(5000),
            default_retries: 10,
            batching_config,
        }
    }

    pub async fn start(&mut self) {
        // self.batcher.start().await
    }

    pub async fn flush_and_close(&mut self) -> Result<(), SendError<Message>> {
        // self.batcher.flush_and_close().await
        Ok(())
    }

    pub async fn write_message(&mut self, channel: &Channel, message: &Message) -> (bool, u32) {
        // match self.batcher.write_message(channel_id, message.clone()).await {
        //     Ok(()) => (true, 0), // Success, no latency for batching
        //     Err(_) => (false, 0)
        // }
        self.write_message_with_params(channel, message, self.default_timeout, self.default_retries).await
    }

    async fn write_message_with_params(
        &mut self,
        channel: &Channel,
        message: &Message,
        timeout_duration: Duration,
        retries: usize
    ) -> (bool, u32) {
        let mut attempts = 0;
        let start_time = std::time::Instant::now();

        while attempts <= retries {
            if self.senders.is_empty() {
                panic!("DataWriter {:?} no channels registered", self.vertex_id);
            }
            let channel_id = channel.get_channel_id();
            if let Some(sender) = self.senders.get(&channel_id) {
                let queue_size = sender.size();
                let queue_remaining = sender.capacity();
                let target_vertex_id = channel.get_target_vertex_id();
                
                gauge!(METRIC_STREAM_TASK_TX_QUEUE_SIZE, LABEL_VERTEX_ID => self.vertex_id.clone(), LABEL_TARGET_VERTEX_ID => target_vertex_id.clone()).set(queue_size);
                gauge!(METRIC_STREAM_TASK_TX_QUEUE_REM, LABEL_VERTEX_ID => self.vertex_id.clone(), LABEL_TARGET_VERTEX_ID => target_vertex_id.clone()).set(queue_remaining);
                let backpressure = 1.0 - (queue_remaining as f64 + 1.0) / (queue_size as f64 + 1.0);
                gauge!(METRIC_STREAM_TASK_BACKPRESSURE_RATIO, LABEL_VERTEX_ID => self.vertex_id.clone(), LABEL_TARGET_VERTEX_ID => target_vertex_id.clone()).set(backpressure);
                match time::timeout(timeout_duration, sender.send(message.clone())).await {
                    Ok(Ok(())) => {
                        return (true, start_time.elapsed().as_millis() as u32)
                    }
                    Ok(Err(_)) => {
                        panic!("DataWriter {:?} channel {} closed", self.vertex_id, channel_id);
                    }
                    Err(_) => {
                        println!("DataWriter {:?} timeout", self.vertex_id);
                        attempts += 1;
                        continue;
                    }
                }
            } else {
                panic!("DataWriter {:?} channel {} not found", self.vertex_id, channel_id);
            }
        }
    
        (false, start_time.elapsed().as_millis() as u32)
    }

    pub fn get_queue_size_and_capacity(&self, channel_id: &str) -> Option<(u32, u32)> {
        self.senders.get(channel_id).map(|sender| (sender.size(), sender.capacity()))
    }
}

#[derive(Debug)]
pub struct TransportClient {
    vertex_id: String,
    pub reader: Option<DataReader>,
    pub writer: Option<DataWriter>,
}

impl TransportClient {
    pub fn new(vertex_id: String, config: TransportClientConfig) -> Self {
        let mut reader: Option<DataReader> = None;
        let mut writer: Option<DataWriter> = None;

        if let Some(receivers) = config.reader_receivers {
            reader = Some(DataReader::new(vertex_id.clone(), receivers));
        }
        if let Some(senders) = config.writer_senders {
            writer = Some(DataWriter::new(vertex_id.clone(), senders));
        }

        Self {
            vertex_id: vertex_id.clone(),
            reader,
            writer,
        }
    }

    pub fn vertex_id(&self) -> &str {
        &self.vertex_id
    }
}