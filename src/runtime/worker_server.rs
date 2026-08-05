use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::common::grpc::master::{master_client, worker_register};
use crate::common::grpc::server_builder;
use crate::common::grpc::spawn_with_shutdown;
use crate::common::grpc::worker::worker_server;
use crate::common::grpc::GrpcServeHandle;
use crate::common::types::PipelineId;
use crate::orchestrator::orchestrator::WorkerOrchestrator;
use crate::runtime::checkpoint::{SerializedRestore, TaskKey};
use crate::runtime::consts::{
    runtime_consts, WORKER_HEARTBEAT_MASTER_SILENCE_TIMEOUT, WORKER_HEARTBEAT_SEND_INTERVAL,
};
use crate::runtime::health::WorkerFatalReason;
use crate::runtime::operators::operator::operator_config_requires_checkpoint;
use crate::runtime::worker::{Worker, WorkerConfig};
use crate::runtime::worker_config_utils::{
    build_execution_graph, resolve_num_threads_per_task, resolve_transport_backend_type,
    WorkerInitPayload,
};

/// Re-export generated stubs (single include lives in `common::grpc::stubs`).
pub use crate::common::grpc::stubs::worker_service;

use worker_service::{
    worker_service_server::WorkerService, CloseWorkerTasksRequest, CloseWorkerTasksResponse,
    ConfigureWorkerRequest, ConfigureWorkerResponse, GetWorkerStateRequest, GetWorkerStateResponse,
    MasterHeartbeatMessage, ResetWorkerRequest, ResetWorkerResponse, RunWorkerTasksRequest,
    RunWorkerTasksResponse, ShutdownWorkerRequest, ShutdownWorkerResponse, StartWorkerRequest,
    StartWorkerResponse, StopSourcesRequest, StopSourcesResponse, TriggerCheckpointBarrierRequest,
    TriggerCheckpointBarrierResponse, WorkerFatalReason as WorkerFatalReasonProto,
    WorkerHeartbeatMessage,
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
        let execution_attempt_id = req.execution_attempt_id;
        let restoring = req.restoring;
        let payload_len = req.init_payload_bytes.len();
        let payload: WorkerInitPayload = serde_json::from_slice(&req.init_payload_bytes)
            .map_err(|e| {
                Status::invalid_argument(format!(
                    "Invalid worker init payload bytes: {} (len={})",
                    e, payload_len
                ))
            })?;
        let spec = payload.pipeline_spec.clone();
        spec.validate().map_err(Status::invalid_argument)?;
        let execution_graph = build_execution_graph(&spec, &payload.task_worker_mapping);
        let execution_graph_signature = execution_graph.signature();

        let vertex_ids = payload
            .vertex_ids
            .iter()
            .map(|v| Arc::<str>::from(v.as_str()))
            .collect::<Vec<_>>();
        let task_restore_data = req
            .task_restore_data
            .into_iter()
            .map(|task_restore| {
                (
                    TaskKey {
                        vertex_id: task_restore.vertex_id,
                        task_index: task_restore.task_index,
                    },
                    SerializedRestore::new(task_restore.restore_data),
                )
            })
            .collect::<HashMap<_, _>>();
        let expected_restore_tasks = if restoring {
            vertex_ids
                .iter()
                .filter_map(|vertex_id| {
                    let vertex = execution_graph
                        .get_vertex(vertex_id.as_ref())
                        .expect("configured vertex must exist");
                    operator_config_requires_checkpoint(&vertex.operator_config).then(|| TaskKey {
                        vertex_id: vertex_id.as_ref().to_string(),
                        task_index: vertex.task_index,
                    })
                })
                .collect::<HashSet<_>>()
        } else {
            HashSet::new()
        };
        let received_restore_tasks = task_restore_data.keys().cloned().collect::<HashSet<_>>();
        if received_restore_tasks != expected_restore_tasks {
            return Err(Status::invalid_argument(format!(
                "restore data does not match assigned checkpointable tasks: expected {}, received {}",
                expected_restore_tasks.len(),
                received_restore_tasks.len()
            )));
        }

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
        worker_config.execution_attempt_id = execution_attempt_id;
        worker_config.task_restore_data = task_restore_data;
        let master_addr = self.orchestrator.get_master_service_addr().await;
        if !master_addr.is_empty() {
            worker_config.master_addr = Some(master_addr);
        }
        worker_config.operator_state_backend = spec.state.operator_backend.clone();
        worker_config.request_store = spec.state.request_store.clone();

        let mut worker_guard = self.worker.lock().await;
        if worker_guard.is_running() {
            return Err(Status::failed_precondition(
                "Worker is already running; reset before reconfigure",
            ));
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
        request: Request<GetWorkerStateRequest>,
    ) -> Result<Response<GetWorkerStateResponse>, Status> {
        self.validate_execution_attempt(request.get_ref().execution_attempt_id)
            .await?;
        let worker_guard = self.worker.lock().await;
        if !worker_guard.is_configured() {
            return Err(Status::failed_precondition(
                "worker is not configured for this attempt",
            ));
        }
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
            return Err(Status::failed_precondition("Worker is not configured yet"));
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
        let _ = request;
        // Reuse probe: drop current Worker (Drop → shutdown) and leave an empty shell.
        let mut worker_guard = self.worker.lock().await;
        let worker_id = worker_guard.worker_id();
        *worker_guard = Worker::new(worker_id);
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
        worker_guard.close();
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

    async fn trigger_checkpoint_barrier(
        &self,
        request: Request<TriggerCheckpointBarrierRequest>,
    ) -> Result<Response<TriggerCheckpointBarrierResponse>, Status> {
        let req = request.into_inner();
        // Attempt + configured under one lock so reset/close cannot race past validation.
        let mut worker_guard = self.worker.lock().await;
        let current_execution_attempt_id = worker_guard.execution_attempt_id();
        if req.execution_attempt_id != current_execution_attempt_id {
            return Err(Status::failed_precondition(format!(
                "stale worker command execution attempt: got {}, current {}",
                req.execution_attempt_id, current_execution_attempt_id
            )));
        }
        if !worker_guard.is_configured() {
            return Ok(Response::new(TriggerCheckpointBarrierResponse {
                success: false,
                error_message: "worker is not configured for this attempt".to_string(),
            }));
        }
        let triggered = worker_guard
            .trigger_checkpoint_barrier(req.checkpoint_id)
            .await;
        Ok(Response::new(TriggerCheckpointBarrierResponse {
            success: triggered,
            error_message: if triggered {
                String::new()
            } else {
                "worker rejected checkpoint barrier trigger".to_string()
            },
        }))
    }

    async fn stop_sources(
        &self,
        request: Request<StopSourcesRequest>,
    ) -> Result<Response<StopSourcesResponse>, Status> {
        let req = request.into_inner();
        let mut worker_guard = self.worker.lock().await;
        let current_execution_attempt_id = worker_guard.execution_attempt_id();
        if req.execution_attempt_id != current_execution_attempt_id {
            return Err(Status::failed_precondition(format!(
                "stale worker command execution attempt: got {}, current {}",
                req.execution_attempt_id, current_execution_attempt_id
            )));
        }
        if !worker_guard.is_configured() {
            return Ok(Response::new(StopSourcesResponse {
                success: false,
                error_message: "worker is not configured for this attempt".to_string(),
            }));
        }
        let stopped = worker_guard.stop_sources();
        Ok(Response::new(StopSourcesResponse {
            success: stopped,
            error_message: if stopped {
                String::new()
            } else {
                "worker rejected stop_sources".to_string()
            },
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
            let tick = runtime_consts().duration(WORKER_HEARTBEAT_SEND_INTERVAL);
            let master_silence = runtime_consts().duration(WORKER_HEARTBEAT_MASTER_SILENCE_TIMEOUT);
            let mut interval = tokio::time::interval(tick);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let (worker_id, pipeline_id, execution_attempt_id, configured) = {
                            let worker_guard = worker.lock().await;
                            (
                                worker_guard.worker_id(),
                                worker_guard.pipeline_id().unwrap_or_default(),
                                worker_guard.execution_attempt_id(),
                                worker_guard.is_configured(),
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
                            configured,
                        })).await.is_err() {
                            break;
                        }

                        // If master stops talking for too long, stop this stream loop.
                        // Worker shutdown-on-master-loss policy will be added separately.
                        if last_master_msg_at.elapsed() > master_silence {
                            break;
                        }
                    }
                    event = fatal_events.recv() => {
                        if let Ok(fatal) = event {
                            let (worker_id, pipeline_id, execution_attempt_id, configured) = {
                                let worker_guard = worker.lock().await;
                                (
                                    worker_guard.worker_id(),
                                    worker_guard.pipeline_id().unwrap_or_default(),
                                    worker_guard.execution_attempt_id(),
                                    worker_guard.is_configured(),
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
                                configured,
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
    serve: Option<GrpcServeHandle>,
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
            serve: None,
            close_worker_rx: Some(close_worker_rx),
            worker_id,
            orchestrator,
        }
    }

    pub async fn start(&mut self, addr: &str) -> anyhow::Result<()> {
        let addr = addr.parse()?;
        let service = worker_server(self.service.clone());

        println!("[WORKER_SERVER] Starting WorkerService server on {}", addr);

        self.serve = Some(spawn_with_shutdown(
            addr,
            server_builder().add_service(service),
        ));
        Ok(())
    }

    pub async fn register_with_master(&mut self) -> anyhow::Result<()> {
        let cfg = worker_register();
        let rpc_timeout = cfg
            .rpc_timeout
            .expect("worker_register config sets rpc_timeout");

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
        let mut last_err: Option<anyhow::Error> = None;
        for attempt in 0..cfg.max_attempts {
            let req = crate::runtime::master::server::master_service::RegisterWorkerRequest {
                worker_id: self.worker_id.clone(),
                ..Default::default()
            };

            // Single dial attempt per outer loop iteration (connect_timeout from cfg).
            let mut dial = cfg.clone();
            dial.max_attempts = 1;
            match master_client(&master_addr, &dial).await {
                Ok(mut client) => match tokio::time::timeout(
                    rpc_timeout,
                    client.register_worker(tonic::Request::new(req)),
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
                            "register_worker RPC timeout on attempt {} after {:?}: {}",
                            attempt + 1,
                            rpc_timeout,
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

            if attempt + 1 < cfg.max_attempts {
                println!(
                    "[WORKER_SERVER] Registration retry: worker_id={} attempt={}/{}",
                    self.worker_id,
                    attempt + 1,
                    cfg.max_attempts
                );
                tokio::time::sleep(cfg.retry_delay).await;
            }
        }

        Err(last_err.unwrap_or_else(|| {
            anyhow::anyhow!(
                "Failed to register worker {} to master {} for unknown reason",
                self.worker_id,
                master_addr
            )
        }))
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
        {
            let mut worker_guard = self.service.worker.lock().await;
            worker_guard.close();
        }
        if let Some(mut serve) = self.serve.take() {
            serve.stop().await;
        }
        println!("[WORKER_SERVER] WorkerService server stopped");
    }

    #[cfg(test)]
    pub async fn run_until_stopped(
        &mut self,
        mut shutdown_rx: oneshot::Receiver<()>,
        mut crash_rx: oneshot::Receiver<bool>,
    ) {
        tokio::select! {
            _ = &mut shutdown_rx => self.stop().await,
            result = &mut crash_rx => {
                self.kill_for_testing(result.unwrap_or(false)).await;
            }
        }
    }

    #[cfg(test)]
    pub async fn kill_for_testing(&mut self, inject_panic: bool) {
        if inject_panic {
            let worker = self.service.worker.lock().await;
            worker
                .health()
                .report_fatal(WorkerFatalReason::Panic, "local test worker crash");
        }
        if let Some(mut serve) = self.serve.take() {
            serve.abort();
        }
        let mut worker = self.service.worker.lock().await;
        worker.close();
    }
}

impl Drop for WorkerServer {
    fn drop(&mut self) {
        if let Some(mut serve) = self.serve.take() {
            serve.abort();
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