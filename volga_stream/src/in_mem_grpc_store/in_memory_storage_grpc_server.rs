use tonic::{Request, Response, Status};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::common::message::Message;

pub mod in_memory_storage_service {
    tonic::include_proto!("in_memory_storage_service");
}

use in_memory_storage_service::{
    in_memory_storage_service_server::InMemoryStorageService,
    AppendRequest, AppendResponse,
    AppendManyRequest, AppendManyResponse,
    InsertRequest, InsertResponse,
    InsertKeyedManyRequest, InsertKeyedManyResponse,
    GetVectorRequest, GetVectorResponse,
    GetMapRequest, GetMapResponse,
    DrainVectorRequest, DrainVectorResponse,
    DrainMapRequest, DrainMapResponse,
};

/// Server implementation of the InMemoryStorageService
pub struct InMemoryStorageServiceImpl {
    vector_storage: Arc<Mutex<Vec<Message>>>,
    map_storage: Arc<Mutex<HashMap<String, Message>>>,
}

impl InMemoryStorageServiceImpl {
    pub fn new() -> Self {
        Self {
            vector_storage: Arc::new(Mutex::new(Vec::new())),
            map_storage: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[tonic::async_trait]
impl InMemoryStorageService for InMemoryStorageServiceImpl {
    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        let req = request.into_inner();
        let message_bytes = req.message_bytes;

        let message = Message::from_bytes(&message_bytes);

        let mut storage_guard = self.vector_storage.lock().await;
        storage_guard.push(message);

        Ok(Response::new(AppendResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    async fn append_many(
        &self,
        request: Request<AppendManyRequest>,
    ) -> Result<Response<AppendManyResponse>, Status> {
        let req = request.into_inner();
        let messages_bytes_list = req.messages_bytes;

        let mut messages = Vec::new();
        for message_bytes in messages_bytes_list {
            let message = Message::from_bytes(&message_bytes);
            messages.push(message);
        }

        let mut storage_guard = self.vector_storage.lock().await;
        storage_guard.extend(messages);

        Ok(Response::new(AppendManyResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    async fn insert(
        &self,
        request: Request<InsertRequest>,
    ) -> Result<Response<InsertResponse>, Status> {
        let req = request.into_inner();
        let key = req.key;
        let message_bytes = req.message_bytes;

        let message = Message::from_bytes(&message_bytes);

        let mut storage_guard = self.map_storage.lock().await;
        storage_guard.insert(key, message);

        Ok(Response::new(InsertResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    async fn insert_keyed_many(
        &self,
        request: Request<InsertKeyedManyRequest>,
    ) -> Result<Response<InsertKeyedManyResponse>, Status> {
        let req = request.into_inner();
        let keyed_messages_bytes = req.keyed_messages;

        let mut keyed_messages = HashMap::new();
        for (key, message_bytes) in keyed_messages_bytes {
            let message = Message::from_bytes(&message_bytes);
            keyed_messages.insert(key, message);
        }

        let mut storage_guard = self.map_storage.lock().await;
        storage_guard.extend(keyed_messages);

        Ok(Response::new(InsertKeyedManyResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    async fn get_vector(
        &self,
        _request: Request<GetVectorRequest>,
    ) -> Result<Response<GetVectorResponse>, Status> {
        let storage_guard = self.vector_storage.lock().await;
        let mut messages_bytes = Vec::new();

        for message in storage_guard.iter() {
            let bytes = message.to_bytes();
            messages_bytes.push(bytes);
        }

        Ok(Response::new(GetVectorResponse {
            success: true,
            error_message: String::new(),
            messages_bytes,
        }))
    }

    async fn get_map(
        &self,
        _request: Request<GetMapRequest>,
    ) -> Result<Response<GetMapResponse>, Status> {
        let storage_guard = self.map_storage.lock().await;
        let mut keyed_messages = HashMap::new();

        for (key, message) in storage_guard.iter() {
            let message_bytes = message.to_bytes();
            keyed_messages.insert(key.clone(), message_bytes);
        }

        Ok(Response::new(GetMapResponse {
            success: true,
            error_message: String::new(),
            keyed_messages,
        }))
    }

    async fn drain_vector(
        &self,
        _request: Request<DrainVectorRequest>,
    ) -> Result<Response<DrainVectorResponse>, Status> {
        let mut storage_guard = self.vector_storage.lock().await;
        let drained: Vec<Message> = std::mem::take(&mut *storage_guard);

        let mut messages_bytes = Vec::with_capacity(drained.len());
        for message in drained {
            messages_bytes.push(message.to_bytes());
        }

        Ok(Response::new(DrainVectorResponse {
            success: true,
            error_message: String::new(),
            messages_bytes,
        }))
    }

    async fn drain_map(
        &self,
        _request: Request<DrainMapRequest>,
    ) -> Result<Response<DrainMapResponse>, Status> {
        let mut storage_guard = self.map_storage.lock().await;
        let drained: HashMap<String, Message> = std::mem::take(&mut *storage_guard);

        let mut keyed_messages = HashMap::with_capacity(drained.len());
        for (key, message) in drained {
            keyed_messages.insert(key, message.to_bytes());
        }

        Ok(Response::new(DrainMapResponse {
            success: true,
            error_message: String::new(),
            keyed_messages,
        }))
    }
}

/// Server that hosts the InMemoryStorageService
pub struct InMemoryStorageServer {
    service: InMemoryStorageServiceImpl,
    server_handle: Option<tokio::task::JoinHandle<()>>,
    shutdown_sender: Option<tokio::sync::oneshot::Sender<()>>,
}

impl InMemoryStorageServer {
    pub fn new() -> Self {
        Self {
            service: InMemoryStorageServiceImpl::new(),
            server_handle: None,
            shutdown_sender: None,
        }
    }

    /// Start the gRPC server on the specified address
    pub async fn start(&mut self, addr: &str) -> anyhow::Result<()> {
        let addr = addr.parse()?;
        let service = in_memory_storage_service::in_memory_storage_service_server::InMemoryStorageServiceServer::new(
            self.service.clone()
        );

        println!("[IN_MEMORY_STORAGE_SERVER] Starting InMemoryStorageService server on {}", addr);

        // Create shutdown channel
        let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel::<()>();

        let server_handle = tokio::spawn(async move {
            match tonic::transport::Server::builder()
                .max_frame_size(Some(12 * 1024 * 1024)) // 12MB
                .add_service(service)
                .serve_with_shutdown(addr, async {
                    shutdown_receiver.await.ok();
                    println!("[IN_MEMORY_STORAGE_SERVER] Received shutdown signal");
                })
                .await {
                Ok(_) => println!("[IN_MEMORY_STORAGE_SERVER] Server shutdown gracefully"),
                Err(e) => eprintln!("[IN_MEMORY_STORAGE_SERVER] Server error: {}", e),
            }
        });

        self.server_handle = Some(server_handle);
        self.shutdown_sender = Some(shutdown_sender);
        Ok(())
    }

    /// Stop the gRPC server gracefully
    pub async fn stop(&mut self) {
        // First, send shutdown signal for graceful shutdown
        if let Some(shutdown_sender) = self.shutdown_sender.take() {
            println!("[IN_MEMORY_STORAGE_SERVER] Sending graceful shutdown signal...");
            let _ = shutdown_sender.send(());
        }

        // Then wait for the server to finish
        if let Some(handle) = self.server_handle.take() {
            match handle.await {
                Ok(_) => println!("[IN_MEMORY_STORAGE_SERVER] Server stopped gracefully"),
                Err(e) if e.is_cancelled() => println!("[IN_MEMORY_STORAGE_SERVER] Server stopped (cancelled)"),
                Err(e) => eprintln!("[IN_MEMORY_STORAGE_SERVER] Server stopped with error: {}", e),
            }
        }
    }
}

impl Clone for InMemoryStorageServiceImpl {
    fn clone(&self) -> Self {
        Self {
            vector_storage: self.vector_storage.clone(),
            map_storage: self.map_storage.clone(),
        }
    }
}
