use tonic::{Request, Response, Status};
use tonic::transport::Endpoint;
use std::sync::Arc;
use std::pin::Pin;
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;

use crate::orchestrator::orchestrator::WorkerOrchestrator;
use crate::common::types::PipelineId;
use crate::runtime::health::WorkerFatalReason;
use crate::runtime::master::server::master_service::master_service_client::MasterServiceClient;
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
    ResetWorkerRequest, ResetWorkerResponse, ShutdownWorkerRequest, ShutdownWorkerResponse,
    TriggerCheckpointRequest, TriggerCheckpointResponse,
    MasterHeartbeatMessage, WorkerHeartbeatMessage,
    WorkerFatalReason as WorkerFatalReasonProto,
};

/// Server implementation of the WorkerService
pub struct WorkerServiceImpl {
    worker: Arc<Mutex<Worker>>,
    orchestrator: Arc<dyn WorkerOrchestrator>,
    close_worker_notify: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl WorkerServiceImpl {
    pub fn new(
        worker_id: String,
        orchestrator: Arc<dyn WorkerOrchestrator>,
        close_worker_notify: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    ) -> Self {
        let worker = Worker::new(worker_id);
        Self {
            worker: Arc::new(Mutex::new(worker)),
            orchestrator,
            close_worker_notify,
        }
    }

    async fn validate_execution_attempt(
        &self,
        execution_attempt_id: u64,
    ) -> Result<(), Status> {
        let worker_guard = self.worker.lock().await;
        let current_execution_attempt_id = worker_guard.execution_attempt_id();
        if execution_attempt_id != current_execution_attempt_id {
            return Err(Status::failed_precondition(format!(
                "stale worker command execution attempt: got {}, current {}",
                execution_attempt_id, current_execution_attempt_id
            )));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl WorkerService for WorkerServiceImpl {
    type StreamHeartbeatStream =
        Pin<Box<dyn Stream<Item = Result<WorkerHeartbeatMessage, Status>> + Send + 'static>>;
    async fn configure_worker(
        &self,
        request: Request<ConfigureWorkerRequest>,
    ) -> Result<Response<ConfigureWorkerResponse>, Status> {
        let req = request.into_inner();
        let payload_len = req.init_payload_bytes.len();
        let payload: WorkerInitPayload = serde_json::from_slice(&req.init_payload_bytes)
            .map_err(|e| {
                Status::invalid_argument(format!(
                    "Invalid worker init payload bytes: {} (len={})",
                    e, payload_len
                ))
            })?;
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

        let mut worker_config = WorkerConfig::new(
            payload.worker_id,
            PipelineId(payload.pipeline_id),
            execution_graph,
            vertex_ids,
            num_threads_per_task.max(1),
            transport_backend_type,
        );
        worker_config.execution_attempt_id = req.execution_attempt_id;
        worker_config.restore_checkpoint_id = payload.restore_checkpoint_id;
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
        if worker_guard.is_running() {
            return Ok(Response::new(ConfigureWorkerResponse {
                success: false,
                error_message: "Worker is already running; reconfigure is not supported".to_string(),
                execution_graph_signature: String::new(),
            }));
        }
        // Recreate the worker instance so every (re)configure starts from clean state:
        // fresh runtimes, transport backend, and actors. If a previous incarnation was
        // configured (e.g. recovery reset), close it first to release its runtimes
        // gracefully before it is dropped.
        if worker_guard.is_configured() {
            worker_guard.close().await;
        }
        let worker_id = worker_guard.worker_id();
        *worker_guard = Worker::new(worker_id);
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
        request: Request<StartWorkerRequest>,
    ) -> Result<Response<StartWorkerResponse>, Status> {
        self.validate_execution_attempt(request.get_ref().execution_attempt_id)
            .await?;
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
        request: Request<RunWorkerTasksRequest>,
    ) -> Result<Response<RunWorkerTasksResponse>, Status> {
        self.validate_execution_attempt(request.get_ref().execution_attempt_id)
            .await?;
        let mut worker_guard = self.worker.lock().await;
        worker_guard.signal_tasks_run().await;
        println!("[WORKER_SERVER] Tasks started successfully");
        Ok(Response::new(RunWorkerTasksResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    async fn reset_worker(
        &self,
        request: Request<ResetWorkerRequest>,
    ) -> Result<Response<ResetWorkerResponse>, Status> {
        self.validate_execution_attempt(request.get_ref().execution_attempt_id)
            .await?;
        let mut worker_guard = self.worker.lock().await;
        worker_guard.close().await;
        drop(worker_guard);

        println!("[WORKER_SERVER] Worker reset successfully");

        Ok(Response::new(ResetWorkerResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    async fn shutdown_worker(
        &self,
        request: Request<ShutdownWorkerRequest>,
    ) -> Result<Response<ShutdownWorkerResponse>, Status> {
        self.validate_execution_attempt(request.get_ref().execution_attempt_id)
            .await?;
        let mut worker_guard = self.worker.lock().await;
        worker_guard.close().await;
        drop(worker_guard);

        let mut notify_guard = self.close_worker_notify.lock().await;
        if let Some(tx) = notify_guard.take() {
            let _ = tx.send(());
        }
        println!("[WORKER_SERVER] Worker shut down successfully");

        Ok(Response::new(ShutdownWorkerResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    async fn close_worker_tasks(
        &self,
        request: Request<CloseWorkerTasksRequest>,
    ) -> Result<Response<CloseWorkerTasksResponse>, Status> {
        self.validate_execution_attempt(request.get_ref().execution_attempt_id)
            .await?;
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

    async fn stream_heartbeat(
        &self,
        request: Request<tonic::Streaming<MasterHeartbeatMessage>>,
    ) -> Result<Response<Self::StreamHeartbeatStream>, Status> {
        let mut inbound = request.into_inner();
        let health = {
            let worker_guard = self.worker.lock().await;
            worker_guard.health()
        };
        let mut fatal_events = health.subscribe();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<WorkerHeartbeatMessage, Status>>(32);
        let worker = self.worker.clone();

        tokio::spawn(async move {
            let mut last_master_msg_at = std::time::Instant::now();
            let tick = Duration::from_secs(1);
            let mut interval = tokio::time::interval(tick);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let (worker_id, pipeline_id, execution_attempt_id) = {
                            let worker_guard = worker.lock().await;
                            (
                                worker_guard.worker_id(),
                                worker_guard.pipeline_id().unwrap_or_default(),
                                worker_guard.execution_attempt_id(),
                            )
                        };
                        let fatal = health.last_fatal();
                        let (healthy, fatal_reason, fatal_message) = match fatal {
                            Some(f) => (
                                false,
                                match f.reason {
                                    WorkerFatalReason::Panic => WorkerFatalReasonProto::Panic as i32,
                                    WorkerFatalReason::TransportDisconnect => WorkerFatalReasonProto::TransportDisconnect as i32,
                                    WorkerFatalReason::TaskFailure => WorkerFatalReasonProto::TaskFailure as i32,
                                },
                                f.message,
                            ),
                            None => (
                                true,
                                WorkerFatalReasonProto::Unspecified as i32,
                                String::new(),
                            ),
                        };
                        if tx.send(Ok(WorkerHeartbeatMessage {
                            worker_id,
                            pipeline_id,
                            sent_at_ms: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64,
                            healthy,
                            fatal_reason,
                            fatal_message,
                            execution_attempt_id,
                        })).await.is_err() {
                            break;
                        }

                        // If master stops talking for too long, stop this stream loop.
                        // Worker shutdown-on-master-loss policy will be added separately.
                        if last_master_msg_at.elapsed() > Duration::from_secs(30) {
                            break;
                        }
                    }
                    event = fatal_events.recv() => {
                        if let Ok(fatal) = event {
                            let (worker_id, pipeline_id, execution_attempt_id) = {
                                let worker_guard = worker.lock().await;
                                (
                                    worker_guard.worker_id(),
                                    worker_guard.pipeline_id().unwrap_or_default(),
                                    worker_guard.execution_attempt_id(),
                                )
                            };
                            let fatal_reason = match fatal.reason {
                                WorkerFatalReason::Panic => WorkerFatalReasonProto::Panic as i32,
                                WorkerFatalReason::TransportDisconnect => WorkerFatalReasonProto::TransportDisconnect as i32,
                                WorkerFatalReason::TaskFailure => WorkerFatalReasonProto::TaskFailure as i32,
                            };
                            let _ = tx.send(Ok(WorkerHeartbeatMessage {
                                worker_id,
                                pipeline_id,
                                sent_at_ms: std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_millis() as u64,
                                healthy: false,
                                fatal_reason,
                                fatal_message: fatal.message,
                                execution_attempt_id,
                            })).await;
                        }
                    }
                    msg = inbound.message() => {
                        match msg {
                            Ok(Some(_)) => {
                                last_master_msg_at = std::time::Instant::now();
                            }
                            Ok(None) => break,
                            Err(_) => break,
                        }
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// Server that hosts the WorkerService
pub struct WorkerServer {
    service: WorkerServiceImpl,
    server_handle: Option<tokio::task::JoinHandle<()>>,
    close_worker_rx: Option<oneshot::Receiver<()>>,
    worker_id: String,
    orchestrator: Arc<dyn WorkerOrchestrator>,
}

impl WorkerServer {
    pub fn new(worker_id: String, orchestrator: Arc<dyn WorkerOrchestrator>) -> Self {
        let (close_worker_tx, close_worker_rx) = oneshot::channel::<()>();
        let close_worker_notify = Arc::new(Mutex::new(Some(close_worker_tx)));
        Self {
            service: WorkerServiceImpl::new(
                worker_id.clone(),
                orchestrator.clone(),
                close_worker_notify,
            ),
            server_handle: None,
            close_worker_rx: Some(close_worker_rx),
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
        const MAX_RETRIES: u32 = 60;
        const RETRY_DELAY_MS: u64 = 500;
        const CONNECT_TIMEOUT_MS: u64 = 3000;
        const RPC_TIMEOUT_MS: u64 = 3000;

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
        let endpoint = Endpoint::from_shared(endpoint)?.connect_timeout(Duration::from_millis(CONNECT_TIMEOUT_MS));
        let mut last_err: Option<anyhow::Error> = None;
        for attempt in 0..MAX_RETRIES {
            let req = crate::runtime::master::server::master_service::RegisterWorkerRequest {
                worker_id: self.worker_id.clone(),
                ..Default::default()
            };

            match endpoint.clone().connect().await {
                Ok(channel) => match tokio::time::timeout(
                    Duration::from_millis(RPC_TIMEOUT_MS),
                    MasterServiceClient::new(channel).register_worker(tonic::Request::new(req)),
                )
                .await
                {
                    Ok(Ok(resp)) => {
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
                    Ok(Err(e)) => {
                        last_err = Some(anyhow::anyhow!(
                            "register_worker RPC failed on attempt {}: {}",
                            attempt + 1,
                            e
                        ));
                    }
                    Err(e) => {
                        last_err = Some(anyhow::anyhow!(
                            "register_worker RPC timeout on attempt {} after {}ms: {}",
                            attempt + 1,
                            RPC_TIMEOUT_MS,
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

    pub async fn wait_for_close_request(&mut self) {
        if let Some(rx) = self.close_worker_rx.take() {
            let _ = rx.await;
        }
    }

    pub async fn worker_health(&self) -> Arc<crate::runtime::health::WorkerHealth> {
        self.service.worker.lock().await.health()
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

    pub async fn crash_for_testing(&mut self) {
        {
            let mut worker = self.service.worker.lock().await;
            worker.kill_for_testing().await;
        }
        if let Some(handle) = self.server_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

impl Clone for WorkerServiceImpl {
    fn clone(&self) -> Self {
        Self {
            worker: self.worker.clone(),
            orchestrator: self.orchestrator.clone(),
            close_worker_notify: self.close_worker_notify.clone(),
        }
    }
} 