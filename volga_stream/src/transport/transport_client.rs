use anyhow::{Result, anyhow};
use arrow::array::StringArray;
use ordered_float::Float;
use std::collections::HashMap;
use tokio::sync::mpsc;
use crate::common::message::Message;
use std::time::Duration;
use tokio::time;

#[derive(Debug)]
pub struct TransportClientConfig {
    pub vertex_id: String,
    pub reader_receivers: Option<HashMap<String, mpsc::Receiver<Message>>>,
    pub writer_senders: Option<HashMap<String, mpsc::Sender<Message>>>,
}

impl TransportClientConfig {
    pub fn new(vertex_id: String) -> Self {
        Self {
            vertex_id,
            reader_receivers: None,
            writer_senders: None,
        }
    }

    pub fn add_reader_receiver(&mut self, channel_id: String, receiver: mpsc::Receiver<Message>) {
        if self.reader_receivers.is_none() {
            self.reader_receivers = Some(HashMap::new());
        }
        self.reader_receivers.as_mut().unwrap().insert(channel_id, receiver);
    }

    pub fn add_writer_sender(&mut self, channel_id: String, sender: mpsc::Sender<Message>) {
        if self.writer_senders.is_none() {
            self.writer_senders = Some(HashMap::new());
        }
        self.writer_senders.as_mut().unwrap().insert(channel_id, sender);
    }
}

#[derive(Debug)]
pub struct DataReader {
    vertex_id: String,
    receivers: HashMap<String, mpsc::Receiver<Message>>,
    default_timeout: Duration,
    default_retries: usize,
}

impl DataReader {
    pub fn new(vertex_id: String, receivers: HashMap<String, mpsc::Receiver<Message>>) -> Self {
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

    pub async fn read_message_with_params(
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
                let vertex_id = self.vertex_id.clone();
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
                let message = result.clone().unwrap();
                let mut source_vertex_id = None;
                let is_watermark = match message.clone() {
                    Message::Watermark(watermark) => {
                        source_vertex_id = Some(watermark.upstream_vertex_id);
                        true
                    }
                    _ => false,
                };

                // TODO config to enable/disable per-message printing
                // if !is_watermark {
                //     let value = message.record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0);
            
                //     println!("DataReader {:?} read message: {:?}", self.vertex_id, value);
                // } else {
                //     println!("DataReader {:?} read watermark: {:?}", self.vertex_id, source_vertex_id.unwrap());
                // }
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
    pub senders: HashMap<String, mpsc::Sender<Message>>,
    default_timeout: Duration,
    default_retries: usize,
}

impl DataWriter {
    pub fn new(vertex_id: String, senders: HashMap<String, mpsc::Sender<Message>>) -> Self {
        Self {
            vertex_id,
            senders,
            default_timeout: Duration::from_millis(5000),
            default_retries: 10,
        }
    }

    pub async fn write_message(&mut self, channel_id: &str, message: Message) -> (bool, u32) {
        self.write_message_with_params(channel_id, message, self.default_timeout, self.default_retries).await
    }

    pub async fn write_message_with_params(
        &mut self,
        channel_id: &str,
        message: Message,
        timeout_duration: Duration,
        retries: usize
    ) -> (bool, u32) {
        let mut attempts = 0;
        let start_time = std::time::Instant::now();

        while attempts <= retries {
            if self.senders.is_empty() {
                panic!("DataWriter {:?} no channels registered", self.vertex_id);
            }
            
            if let Some(sender) = self.senders.get(channel_id) {
                match time::timeout(timeout_duration, sender.send(message.clone())).await {
                    Ok(Ok(())) => {
                        let mut source_vertex_id = None;
                        let is_watermark = match message.clone() {
                            Message::Watermark(watermark) => {
                                source_vertex_id = Some(watermark.upstream_vertex_id);
                                true
                            }
                            _ => false,
                        };
                        
                        // TODO config to enable/disable per-message printing
                        // if !is_watermark {
                        //     let value = message.record_batch().column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0);
                        //     // if self.vertex_id.contains("map") || self.vertex_id.contains("key_by") {
                        //     println!("DataWriter {:?} wrote message: {:?}", self.vertex_id, value);
                        //     // }
                        // } else {
                        //     println!("DataWriter {:?} wrote watermark: {:?}", self.vertex_id, source_vertex_id.unwrap());
                        // }
                        return (true, start_time.elapsed().as_millis() as u32)
                    }
                    Ok(Err(_)) => {
                        panic!("DataWriter {:?} channel {} closed", self.vertex_id, channel_id);
                    }
                    Err(_) => {
                        // println!("DataWriter {:?} timeout", self.vertex_id);
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