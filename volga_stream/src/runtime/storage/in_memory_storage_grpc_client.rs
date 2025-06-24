use tonic::transport::Channel;
use std::collections::HashMap;
use crate::common::message::Message;
use anyhow::Result;
use tokio::time::{Duration, sleep};

pub mod in_memory_storage_service {
    tonic::include_proto!("in_memory_storage_service");
}

use in_memory_storage_service::{
    in_memory_storage_service_client::InMemoryStorageServiceClient,
    AppendRequest,
    AppendManyRequest,
    InsertRequest,
    InsertKeyedManyRequest,
    GetVectorRequest,
    GetMapRequest,
};

/// Client for the InMemoryStorageService
#[derive(Debug)]
pub struct InMemoryStorageClient {
    client: InMemoryStorageServiceClient<Channel>,
}

impl InMemoryStorageClient {
    /// Create a new client connected to the specified address with retry logic
    pub async fn new(addr: String) -> Result<Self> {
        const MAX_RETRIES: u32 = 5;
        const RETRY_DELAY_MS: u64 = 1000;
        
        let mut last_error = None;
        
        for attempt in 0..MAX_RETRIES {
            match InMemoryStorageServiceClient::connect(addr.clone()).await {
                Ok(client) => {
                    println!("[IN_MEMORY_STORAGE_CLIENT] Successfully connected to {} on attempt {}", addr, attempt + 1);
                    return Ok(Self { client });
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    println!("[IN_MEMORY_STORAGE_CLIENT] Connection attempt {} failed: {}", attempt + 1, e);
                    
                    // Don't sleep on the last attempt
                    if attempt < MAX_RETRIES - 1 {
                        sleep(Duration::from_millis(RETRY_DELAY_MS * (attempt + 1) as u64)).await;
                    }
                }
            }
        }
        
        Err(anyhow::anyhow!(
            "Failed to connect to {} after {} attempts. Last error: {:?}", 
            addr, 
            MAX_RETRIES, 
            last_error
        ))
    }

    /// Append a single message to vector storage
    pub async fn append(&mut self, message: Message) -> Result<bool> {
        let message_bytes = message.to_bytes();
        let request = tonic::Request::new(AppendRequest { message_bytes });
        
        let response = self.client.append(request).await?;
        let response = response.into_inner();
        
        if !response.success {
            return Err(anyhow::anyhow!("Append failed: {}", response.error_message));
        }
        
        Ok(true)
    }

    /// Append multiple messages to vector storage
    pub async fn append_many(&mut self, messages: Vec<Message>) -> Result<bool> {
        let mut messages_bytes = Vec::new();
        for message in messages {
            messages_bytes.push(message.to_bytes());
        }
        
        let request = tonic::Request::new(AppendManyRequest { messages_bytes });
        
        let response = self.client.append_many(request).await?;
        let response = response.into_inner();
        
        if !response.success {
            return Err(anyhow::anyhow!("AppendMany failed: {}", response.error_message));
        }
        
        Ok(true)
    }

    /// Insert a message with a key into map storage
    pub async fn insert(&mut self, key: String, message: Message) -> Result<bool> {
        let message_bytes = message.to_bytes();
        let request = tonic::Request::new(InsertRequest { key, message_bytes });
        
        let response = self.client.insert(request).await?;
        let response = response.into_inner();
        
        if !response.success {
            return Err(anyhow::anyhow!("Insert failed: {}", response.error_message));
        }
        
        Ok(true)
    }

    /// Insert multiple keyed messages into map storage
    pub async fn insert_keyed_many(&mut self, keyed_messages: HashMap<String, Message>) -> Result<bool> {
        let mut keyed_messages_bytes = HashMap::new();
        for (key, message) in keyed_messages {
            keyed_messages_bytes.insert(key, message.to_bytes());
        }
        
        let request = tonic::Request::new(InsertKeyedManyRequest { keyed_messages: keyed_messages_bytes });
        
        let response = self.client.insert_keyed_many(request).await?;
        let response = response.into_inner();
        
        if !response.success {
            return Err(anyhow::anyhow!("InsertKeyedMany failed: {}", response.error_message));
        }
        
        Ok(true)
    }

    /// Get all messages from vector storage
    pub async fn get_vector(&mut self) -> Result<Vec<Message>> {
        let request = tonic::Request::new(GetVectorRequest {});
        
        let response = self.client.get_vector(request).await?;
        let response = response.into_inner();
        
        if !response.success {
            return Err(anyhow::anyhow!("GetVector failed: {}", response.error_message));
        }
        
        let mut messages = Vec::new();
        for message_bytes in response.messages_bytes {
            let message = Message::from_bytes(&message_bytes);
            messages.push(message);
        }
        
        Ok(messages)
    }

    /// Get all messages from map storage
    pub async fn get_map(&mut self) -> Result<HashMap<String, Message>> {
        let request = tonic::Request::new(GetMapRequest {});
        
        let response = self.client.get_map(request).await?;
        let response = response.into_inner();
        
        if !response.success {
            return Err(anyhow::anyhow!("GetMap failed: {}", response.error_message));
        }
        
        let mut keyed_messages = HashMap::new();
        for (key, message_bytes) in response.keyed_messages {
            let message = Message::from_bytes(&message_bytes);
            keyed_messages.insert(key, message);
        }
        
        Ok(keyed_messages)
    }
} 