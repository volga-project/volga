use tonic::{Request, Response, Status};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::runtime::worker::Worker;
use crate::runtime::execution_graph::ExecutionGraph;

pub mod worker_service {
    tonic::include_proto!("worker_service");
}

use worker_service::{
    worker_service_server::WorkerService,
    GetWorkerStateRequest, GetWorkerStateResponse,
    StartWorkerRequest, StartWorkerResponse,
    StartTasksRequest, StartTasksResponse,
    CloseTasksRequest, CloseTasksResponse,
    CloseWorkerRequest, CloseWorkerResponse,
};

/// Server implementation of the WorkerService
pub struct WorkerServiceImpl {
    worker: Arc<Mutex<Option<Worker>>>,
}

impl WorkerServiceImpl {
    pub fn new() -> Self {
        Self {
            worker: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_worker(worker: Worker) -> Self {
        Self {
            worker: Arc::new(Mutex::new(Some(worker))),
        }
    }

    /// Initialize the worker with execution graph and vertex IDs
    pub async fn initialize_worker(
        &self,
        graph: ExecutionGraph,
        vertex_ids: Vec<String>,
        num_io_threads: usize,
    ) {
        let mut worker_guard = self.worker.lock().await;
        *worker_guard = Some(Worker::new(graph, vertex_ids, num_io_threads));
    }
}

#[tonic::async_trait]
impl WorkerService for WorkerServiceImpl {
    async fn get_worker_state(
        &self,
        _request: Request<GetWorkerStateRequest>,
    ) -> Result<Response<GetWorkerStateResponse>, Status> {
        let worker_guard = self.worker.lock().await;
        
        if let Some(worker) = worker_guard.as_ref() {
            // TODO: Implement worker state retrieval
            // let state = worker.get_state().await;
            // let state_bytes = state.to_bytes().map_err(|e| {
            //     Status::internal(format!("Failed to serialize worker state: {}", e))
            // })?;
            
            // For now, return empty response
            Ok(Response::new(GetWorkerStateResponse {
                worker_state_bytes: vec![],
            }))
        } else {
            Err(Status::failed_precondition("Worker not initialized"))
        }
    }

    async fn start_worker(
        &self,
        _request: Request<StartWorkerRequest>,
    ) -> Result<Response<StartWorkerResponse>, Status> {
        let mut worker_guard = self.worker.lock().await;
        
        if let Some(worker) = worker_guard.as_mut() {
            // TODO: Implement worker start logic
            // worker.start();
            
            Ok(Response::new(StartWorkerResponse {
                success: true,
                error_message: String::new(),
            }))
        } else {
            Ok(Response::new(StartWorkerResponse {
                success: false,
                error_message: "Worker not initialized".to_string(),
            }))
        }
    }

    async fn start_tasks(
        &self,
        _request: Request<StartTasksRequest>,
    ) -> Result<Response<StartTasksResponse>, Status> {
        let mut worker_guard = self.worker.lock().await;
        
        if let Some(worker) = worker_guard.as_mut() {
            // TODO: Implement tasks start logic
            // worker.execute();
            
            Ok(Response::new(StartTasksResponse {
                success: true,
                error_message: String::new(),
            }))
        } else {
            Ok(Response::new(StartTasksResponse {
                success: false,
                error_message: "Worker not initialized".to_string(),
            }))
        }
    }

    async fn close_tasks(
        &self,
        _request: Request<CloseTasksRequest>,
    ) -> Result<Response<CloseTasksResponse>, Status> {
        let mut worker_guard = self.worker.lock().await;
        
        if let Some(worker) = worker_guard.as_mut() {
            // TODO: Implement tasks close logic
            // worker.close_tasks().await;
            
            Ok(Response::new(CloseTasksResponse {
                success: true,
                error_message: String::new(),
            }))
        } else {
            Ok(Response::new(CloseTasksResponse {
                success: false,
                error_message: "Worker not initialized".to_string(),
            }))
        }
    }

    async fn close_worker(
        &self,
        _request: Request<CloseWorkerRequest>,
    ) -> Result<Response<CloseWorkerResponse>, Status> {
        let mut worker_guard = self.worker.lock().await;
        
        if let Some(worker) = worker_guard.as_mut() {
            // TODO: Implement worker close logic
            // worker.close().await;
            
            Ok(Response::new(CloseWorkerResponse {
                success: true,
                error_message: String::new(),
            }))
        } else {
            Ok(Response::new(CloseWorkerResponse {
                success: false,
                error_message: "Worker not initialized".to_string(),
            }))
        }
    }
}

/// Server that hosts the WorkerService
pub struct WorkerServer {
    service: WorkerServiceImpl,
    server_handle: Option<tokio::task::JoinHandle<()>>,
}

impl WorkerServer {
    pub fn new() -> Self {
        Self {
            service: WorkerServiceImpl::new(),
            server_handle: None,
        }
    }

    pub fn with_worker(worker: Worker) -> Self {
        Self {
            service: WorkerServiceImpl::with_worker(worker),
            server_handle: None,
        }
    }

    /// Initialize the worker with execution graph and vertex IDs
    pub async fn initialize_worker(
        &self,
        graph: ExecutionGraph,
        vertex_ids: Vec<String>,
        num_io_threads: usize,
    ) {
        self.service.initialize_worker(graph, vertex_ids, num_io_threads).await;
    }

    /// Start the gRPC server on the specified address
    pub async fn start(&mut self, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let addr = addr.parse()?;
        let service = worker_service::worker_service_server::WorkerServiceServer::new(
            self.service.clone()
        );

        println!("[WORKER_SERVER] Starting WorkerService server on {}", addr);
        
        let server_handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve(addr)
                .await
                .unwrap();
        });

        self.server_handle = Some(server_handle);
        Ok(())
    }

    /// Stop the gRPC server
    pub async fn stop(&mut self) {
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
        println!("[WORKER_SERVER] WorkerService server stopped");
    }
}

impl Clone for WorkerServiceImpl {
    fn clone(&self) -> Self {
        Self {
            worker: self.worker.clone(),
        }
    }
} 