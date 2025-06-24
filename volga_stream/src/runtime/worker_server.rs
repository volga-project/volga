use tonic::{Request, Response, Status};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::runtime::worker::Worker;
use crate::runtime::execution_graph::ExecutionGraph;
use crate::runtime::stream_task::StreamTaskStatus;

pub mod worker_service {
    tonic::include_proto!("worker_service");
}

use worker_service::{
    worker_service_server::WorkerService,
    GetWorkerStateRequest, GetWorkerStateResponse,
    StartWorkerRequest, StartWorkerResponse,
    RunWorkerTasksRequest, RunWorkerTasksResponse,
    CloseWorkerTasksRequest, CloseWorkerTasksResponse,
    CloseWorkerRequest, CloseWorkerResponse,
};

/// Server implementation of the WorkerService
pub struct WorkerServiceImpl {
    worker: Arc<Mutex<Worker>>,
}

impl WorkerServiceImpl {
    pub fn new(graph: ExecutionGraph, vertex_ids: Vec<String>, num_io_threads: usize) -> Self {
        let worker = Worker::new(graph, vertex_ids, num_io_threads);
        Self {
            worker: Arc::new(Mutex::new(worker)),
        }
    }
}

#[tonic::async_trait]
impl WorkerService for WorkerServiceImpl {
    async fn get_worker_state(
        &self,
        _request: Request<GetWorkerStateRequest>,
    ) -> Result<Response<GetWorkerStateResponse>, Status> {
        let worker_guard = self.worker.lock().await;
        let state = worker_guard.get_state().await;
        let state_bytes = bincode::serialize(&state).map_err(|e| {
            Status::internal(format!("Failed to serialize worker state: {}", e))
        })?;
        
        Ok(Response::new(GetWorkerStateResponse {
            worker_state_bytes: state_bytes,
        }))
    }

    async fn start_worker(
        &self,
        _request: Request<StartWorkerRequest>,
    ) -> Result<Response<StartWorkerResponse>, Status> {
        let mut worker_guard = self.worker.lock().await;
        worker_guard.start().await;
        println!("[WORKER_SERVER] Worker started successfully");
        Ok(Response::new(StartWorkerResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    async fn run_worker_tasks(
        &self,
        _request: Request<RunWorkerTasksRequest>,
    ) -> Result<Response<RunWorkerTasksResponse>, Status> {
        let mut worker_guard = self.worker.lock().await;
        worker_guard.run_tasks().await;
        println!("[WORKER_SERVER] Tasks started successfully");
        Ok(Response::new(RunWorkerTasksResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    async fn close_worker(
        &self,
        _request: Request<CloseWorkerRequest>,
    ) -> Result<Response<CloseWorkerResponse>, Status> {
        let mut worker_guard = self.worker.lock().await;
        worker_guard.close().await;
        
        println!("[WORKER_SERVER] Worker closed successfully");
        
        Ok(Response::new(CloseWorkerResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    async fn close_worker_tasks(
        &self,
        _request: Request<CloseWorkerTasksRequest>,
    ) -> Result<Response<CloseWorkerTasksResponse>, Status> {
        let mut worker_guard = self.worker.lock().await;
        worker_guard.close_tasks().await;
        println!("[WORKER_SERVER] Tasks closed successfully");
        Ok(Response::new(CloseWorkerTasksResponse {
            success: true,
            error_message: String::new(),
        }))
    }
}

/// Server that hosts the WorkerService
pub struct WorkerServer {
    service: WorkerServiceImpl,
    server_handle: Option<tokio::task::JoinHandle<()>>,
}

impl WorkerServer {
    pub fn new(graph: ExecutionGraph, vertex_ids: Vec<String>, num_io_threads: usize) -> Self {
        Self {
            service: WorkerServiceImpl::new(graph, vertex_ids, num_io_threads),
            server_handle: None,
        }
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