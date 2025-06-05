use anyhow::{Result, anyhow};
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

    // pub fn register_receiver(&mut self, channel_id: String, receiver: mpsc::Receiver<Message>) {
    //     self.receivers.insert(channel_id, receiver);
    // }

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
                futures.push(Box::pin(async move {
                    match time::timeout(timeout_duration, receiver.recv()).await {
                        Ok(Some(message)) => Some((channel_id.clone(), message)),
                        Ok(None) => {
                            panic!("DataReader channel {} closed", channel_id);
                        },
                        Err(_) => None,
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
                // println!("{} DataReader {:?} read message: {:?}", timestamp(), self.vertex_id, result.clone());
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
    senders: HashMap<String, mpsc::Sender<Message>>,
    default_timeout: Duration,
    default_retries: usize,
}

impl DataWriter {
    pub fn new(vertex_id: String, senders: HashMap<String, mpsc::Sender<Message>>) -> Self {
        Self {
            vertex_id,
            senders,
            default_timeout: Duration::from_millis(100),
            default_retries: 0,
        }
    }

    // pub fn register_sender(&mut self, channel_id: String, sender: mpsc::Sender<Message>) {
    //     self.senders.insert(channel_id, sender);
    // }

    pub async fn write_message(&mut self, channel_id: &str, message: Message) -> Result<()> {
        self.write_message_with_params(channel_id, message, None, None).await
    }

    pub async fn write_message_with_params(
        &mut self,
        channel_id: &str,
        message: Message,
        timeout_duration: Option<Duration>,
        retries: Option<usize>
    ) -> Result<()> {
        let timeout_duration = timeout_duration.unwrap_or(self.default_timeout);
        let retries = retries.unwrap_or(self.default_retries);
        let mut attempts = 0;

        while attempts <= retries {
            if self.senders.is_empty() {
                println!("DataWriter {:?} no channels registered", self.vertex_id);
                return Err(anyhow!("Attempted to write message to DataWriter with no channels registered"));
            }
            
            if let Some(sender) = self.senders.get(channel_id) {
                match time::timeout(timeout_duration, sender.send(message.clone())).await {
                    Ok(Ok(())) => {
                        // println!("{} DataWriter {:?} wrote message: {:?}", timestamp(), self.vertex_id, message);
                        
                        // println!("DataWriter {:?} wrote message: {:?}", self.vertex_id, message);
                        return Ok(());
                    }
                    Ok(Err(_)) => {
                        println!("DataWriter {:?} channel {} closed", self.vertex_id, channel_id);
                        return Err(anyhow!("Channel {} closed", channel_id));
                    }
                    Err(_) => {
                        println!("DataWriter {:?} timeout", self.vertex_id);
                        attempts += 1;
                        continue;
                    }
                }
            } else {
                return Err(anyhow!("Channel {} not found", channel_id));
            }
        }
        Err(anyhow!("Failed to write message after {} retries", retries))
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

        // Self {
        //     vertex_id: vertex_id.clone(),
        //     reader: config.reader_receivers.map(|receivers| DataReader::new(vertex_id.clone(), receivers)),
        //     writer: config.writersenders.map(|senders| DataWriter::new(vertex_id.clone(), senders)),
        // }
    }

    pub fn vertex_id(&self) -> &str {
        &self.vertex_id
    }

    // pub async fn register_receiver(&mut self, channel_id: String, receiver: mpsc::Receiver<Message>) -> Result<()> {
    //     if let Some(reader) = &mut self.reader {
    //         reader.register_receiver(channel_id, receiver);
    //         Ok(())
    //     } else {
    //         Err(anyhow!("Reader not initialized"))
    //     }
    // }

    // pub async fn register_sender(&mut self, channel_id: String, sender: mpsc::Sender<Message>) -> Result<()> {
    //     if let Some(writer) = &mut self.writer {
    //         writer.register_sender(channel_id, sender);
    //         Ok(())
    //     } else {
    //         Err(anyhow!("Writer not initialized"))
    //     }
    // }
}