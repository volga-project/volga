use tonic::{Request, Response, Status};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Duration;
use uuid::Uuid;

use crate::orchestrator::orchestrator::Orchestrator;
use crate::common::types::PipelineId;
use crate::runtime::master_server::master_service::master_service_client::MasterServiceClient;
use crate::runtime::worker_config_utils::{build_execution_graph, resolve_num_threads_per_task, resolve_transport_backend_type, WorkerInitPayload};
use crate::runtime::worker::{Worker, WorkerConfig};

pub mod worker_service {
    tonic::include_proto!("worker_service");
}

use worker_service::{
    worker_service_server::WorkerService,
    ConfigureWorkerRequest, ConfigureWorkerResponse,
    GetWorkerStateRequest, GetWorkerStateResponse,
    StartWorkerRequest, StartWorkerResponse,
    RunWorkerTasksRequest, RunWorkerTasksResponse,
    CloseWorkerTasksRequest, CloseWorkerTasksResponse,
    CloseWorkerRequest, CloseWorkerResponse,
    TriggerCheckpointRequest, TriggerCheckpointResponse,
};

/// Server implementation of the WorkerService
pub struct WorkerServiceImpl {
    worker: Arc<Mutex<Worker>>,
    orchestrator: Arc<dyn Orchestrator>,
}

impl WorkerServiceImpl {
    pub fn new(worker_id: String, orchestrator: Arc<dyn Orchestrator>) -> Self {
        let worker = Worker::new(worker_id);
        Self {
            worker: Arc::new(Mutex::new(worker)),
            orchestrator,
        }
    }
}

#[tonic::async_trait]
impl WorkerService for WorkerServiceImpl {
    async fn configure_worker(
        &self,
        request: Request<ConfigureWorkerRequest>,
    ) -> Result<Response<ConfigureWorkerResponse>, Status> {
        let req = request.into_inner();
        let payload: WorkerInitPayload = bincode::deserialize(&req.init_payload_bytes)
            .map_err(|e| Status::invalid_argument(format!("Invalid worker init payload bytes: {}", e)))?;
        let spec = payload.pipeline_spec.clone();
        let execution_graph = build_execution_graph(&spec, &payload.task_worker_mapping);
        let execution_graph_signature = execution_graph.signature();

        let vertex_ids = payload
            .vertex_ids
            .iter()
            .map(|v| Arc::<str>::from(v.as_str()))
            .collect::<Vec<_>>();

        let transport_backend_type = resolve_transport_backend_type(&execution_graph, &payload.vertex_ids);
        let num_threads_per_task = resolve_num_threads_per_task(&spec);

        let pipeline_uuid = Uuid::parse_str(&payload.pipeline_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid pipeline_id: {}", e)))?;
        let mut worker_config = WorkerConfig::new(
            payload.worker_id,
            PipelineId(pipeline_uuid),
            execution_graph,
            vertex_ids,
            num_threads_per_task.max(1),
            transport_backend_type,
        );
        let master_addr = self.orchestrator.get_master_service_addr().await;
        if !master_addr.is_empty() {
            worker_config.master_addr = Some(master_addr);
        }
        worker_config.storage_budgets = spec.worker_runtime.storage.budgets.clone();
        worker_config.inmem_store_lock_pool_size = spec.worker_runtime.storage.inmem_store_lock_pool_size;
        worker_config.inmem_store_bucket_granularity = spec.worker_runtime.storage.inmem_store_bucket_granularity;
        worker_config.inmem_store_max_batch_size = spec.worker_runtime.storage.inmem_store_max_batch_size;
        worker_config.operator_type_storage_overrides = spec.operator_type_storage_overrides();

        let mut worker_guard = self.worker.lock().await;
        worker_guard.configure(worker_config);

        Ok(Response::new(ConfigureWorkerResponse {
            success: true,
            error_message: String::new(),
            execution_graph_signature,
        }))
    }

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
        if !worker_guard.is_configured() {
            return Ok(Response::new(StartWorkerResponse {
                success: false,
                error_message: "Worker is not configured yet".to_string(),
            }));
        }
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
        worker_guard.signal_tasks_run().await;
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
        worker_guard.signal_tasks_close().await;
        println!("[WORKER_SERVER] Tasks closed successfully");
        Ok(Response::new(CloseWorkerTasksResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    async fn trigger_checkpoint(
        &self,
        request: Request<TriggerCheckpointRequest>,
    ) -> Result<Response<TriggerCheckpointResponse>, Status> {
        let checkpoint_id = request.get_ref().checkpoint_id;
        let mut worker_guard = self.worker.lock().await;
        worker_guard.trigger_checkpoint(checkpoint_id).await;
        Ok(Response::new(TriggerCheckpointResponse {
            success: true,
            error_message: String::new(),
        }))
    }
}

/// Server that hosts the WorkerService
pub struct WorkerServer {
    service: WorkerServiceImpl,
    server_handle: Option<tokio::task::JoinHandle<()>>,
    worker_id: String,
    orchestrator: Arc<dyn Orchestrator>,
}

impl WorkerServer {
    pub fn new(worker_id: String, orchestrator: Arc<dyn Orchestrator>) -> Self {
        Self {
            service: WorkerServiceImpl::new(worker_id.clone(), orchestrator.clone()),
            server_handle: None,
            worker_id,
            orchestrator,
        }
    }

    pub async fn start(&mut self, addr: &str) -> anyhow::Result<()> {
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

    pub async fn register_with_master(&mut self) -> anyhow::Result<()> {
        const MAX_RETRIES: u32 = 10;
        const RETRY_DELAY_MS: u64 = 500;

        let master_addr = self.orchestrator.get_master_service_addr().await;
        if master_addr.is_empty() {
            println!(
                "[WORKER_SERVER] Skipping registration: worker_id={} master_addr is empty",
                self.worker_id
            );
            return Ok(());
        }
        println!(
            "[WORKER_SERVER] Registering with master: worker_id={} master_addr={}",
            self.worker_id, master_addr
        );
        let endpoint = format!("http://{}", master_addr);
        let mut last_err: Option<anyhow::Error> = None;
        for attempt in 0..MAX_RETRIES {
            let req = crate::runtime::master_server::master_service::RegisterWorkerRequest {
                worker_id: self.worker_id.clone(),
            };

            match MasterServiceClient::connect(endpoint.clone()).await {
                Ok(mut client) => match client.register_worker(tonic::Request::new(req)).await {
                    Ok(resp) => {
                        let resp = resp.into_inner();
                        if !resp.success {
                            return Err(anyhow::anyhow!(
                                "Master rejected worker registration: {}",
                                resp.error_message
                            ));
                        }
                        println!(
                            "[WORKER_SERVER] Registration succeeded: worker_id={}",
                            self.worker_id
                        );
                        return Ok(());
                    }
                    Err(e) => {
                        last_err = Some(anyhow::anyhow!(
                            "register_worker RPC failed on attempt {}: {}",
                            attempt + 1,
                            e
                        ));
                    }
                },
                Err(e) => {
                    last_err = Some(anyhow::anyhow!(
                        "connect failed on attempt {}: {}",
                        attempt + 1,
                        e
                    ));
                }
            }

            if attempt + 1 < MAX_RETRIES {
                println!(
                    "[WORKER_SERVER] Registration retry: worker_id={} attempt={}/{}",
                    self.worker_id,
                    attempt + 1,
                    MAX_RETRIES
                );
                tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS * (attempt as u64 + 1))).await;
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow::anyhow!(
            "Failed to register worker {} to master {} for unknown reason",
            self.worker_id,
            master_addr
        )))
    }

    /// Stop the gRPC server
    pub async fn stop(&mut self) {
        // Ensure we shut down the worker runtimes cleanly; otherwise dropping tokio runtimes
        // inside async contexts can panic.
        {
            let mut worker_guard = self.service.worker.lock().await;
            worker_guard.close().await;
        }
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
            orchestrator: self.orchestrator.clone(),
        }
    }
} 