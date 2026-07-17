use std::fmt;

use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tonic::transport::Endpoint;
use tonic::Code;

use crate::api::PipelineSpec;
use crate::orchestrator::task_assignment::TaskWorkerMapping;
use crate::runtime::observability::snapshot_types::WorkerSnapshot;
use crate::runtime::worker_config_utils::WorkerInitPayload;
use crate::runtime::consts::{
    runtime_consts, MASTER_RPC_MAX_RETRIES, MASTER_RPC_RETRY_DELAY, MASTER_WORKER_CONNECT_TIMEOUT,
};

use crate::common::failure::FailureEvent;
use super::heartbeat::WorkerHeartbeatMonitor;
use super::worker_service::{
    worker_service_client::WorkerServiceClient, CloseWorkerTasksRequest, ResetWorkerRequest,
    ShutdownWorkerRequest,
    ConfigureWorkerRequest, GetWorkerStateRequest, RunWorkerTasksRequest, StartWorkerRequest,
    TriggerCheckpointBarrierRequest,
};

enum Attempt<T> {
    Done(T),
    Retry(String),
    Fail(WorkerCallError),
}

#[derive(Debug)]
pub enum WorkerCallError {
    Rejected(String),
    Unreachable(String),
}

impl fmt::Display for WorkerCallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(detail) => write!(f, "rejected: {}", detail),
            Self::Unreachable(detail) => write!(f, "unreachable: {}", detail),
        }
    }
}

impl std::error::Error for WorkerCallError {}

fn retry_delay(attempt: u32) -> Duration {
    runtime_consts().duration(MASTER_RPC_RETRY_DELAY) * (attempt + 1)
}

fn is_retryable_status(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable
            | Code::DeadlineExceeded
            | Code::ResourceExhausted
            | Code::Aborted
            | Code::Unknown
    )
}

fn status_attempt<T>(status: tonic::Status) -> Attempt<T> {
    if is_retryable_status(&status) {
        Attempt::Retry(status.to_string())
    } else {
        Attempt::Fail(WorkerCallError::Rejected(status.to_string()))
    }
}

async fn with_retry<T, F, Fut>(
    op_name: &str,
    target: &str,
    mut op: F,
) -> Result<T, WorkerCallError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Attempt<T>>,
{
    let mut last_error = None;
    for attempt in 0..runtime_consts().u64(MASTER_RPC_MAX_RETRIES) as u32 {
        match op().await {
            Attempt::Done(v) => return Ok(v),
            Attempt::Fail(e) => return Err(e),
            Attempt::Retry(msg) => {
                println!(
                    "[MASTER] {} attempt {} on {} failed (retryable): {}",
                    op_name,
                    attempt + 1,
                    target,
                    msg
                );
                last_error = Some(msg);
                if attempt + 1 < runtime_consts().u64(MASTER_RPC_MAX_RETRIES) as u32 {
                    sleep(retry_delay(attempt)).await;
                }
            }
        }
    }
    Err(WorkerCallError::Unreachable(format!(
        "{} failed on {} after {} attempts: {:?}",
        op_name, target, runtime_consts().u64(MASTER_RPC_MAX_RETRIES), last_error
    )))
}

/// Dial worker control gRPC with a bounded connect timeout (see `MASTER_WORKER_CONNECT_TIMEOUT`).
pub(super) async fn connect_worker_client(
    addr: &str,
) -> Result<WorkerServiceClient<tonic::transport::Channel>, String> {
    let connect_timeout = runtime_consts().duration(MASTER_WORKER_CONNECT_TIMEOUT);
    let endpoint = Endpoint::from_shared(format!("http://{addr}"))
        .map_err(|e| format!("invalid worker endpoint {addr}: {e}"))?
        .connect_timeout(connect_timeout);
    endpoint
        .connect()
        .await
        .map(WorkerServiceClient::new)
        .map_err(|e| format!("connect to {addr} failed: {e}"))
}

async fn dial(addr: &str) -> Attempt<WorkerServiceClient<tonic::transport::Channel>> {
    match connect_worker_client(addr).await {
        Ok(client) => Attempt::Done(client),
        Err(e) => Attempt::Retry(e),
    }
}

/// Control session to one worker.
pub struct WorkerClient {
    client: WorkerServiceClient<tonic::transport::Channel>,
    worker_ip: String,
    _heartbeat_monitor: Option<WorkerHeartbeatMonitor>,
    execution_attempt_id: u64,
}

impl WorkerClient {
    pub async fn open(
        worker_id: &str,
        worker_ip: String,
        execution_attempt_id: u64,
    ) -> Result<Self, WorkerCallError> {
        let client = with_retry("connect", &worker_ip, || dial(&worker_ip)).await?;
        println!("[MASTER] Connected to worker {} ({})", worker_id, worker_ip);
        Ok(Self {
            client,
            worker_ip,
            _heartbeat_monitor: None,
            execution_attempt_id,
        })
    }

    pub fn start_heartbeat(
        &mut self,
        worker_id: String,
        failure_tx: mpsc::Sender<FailureEvent>,
    ) {
        self._heartbeat_monitor = Some(WorkerHeartbeatMonitor::spawn(
            worker_id,
            self.worker_ip.clone(),
            self.execution_attempt_id,
            self.client.clone(),
            failure_tx,
        ));
    }

    pub async fn configure(
        &self,
        worker_id: String,
        pipeline_id: String,
        spec: PipelineSpec,
        vertex_ids: Vec<String>,
        task_worker_mapping: TaskWorkerMapping,
        restore_checkpoint_id: Option<u64>,
    ) -> Result<String, WorkerCallError> {
        let payload = WorkerInitPayload {
            worker_id: worker_id.clone(),
            pipeline_id,
            pipeline_spec: spec,
            vertex_ids,
            task_worker_mapping,
            restore_checkpoint_id,
        };
        let init_payload_bytes =
            serde_json::to_vec(&payload).map_err(|e| WorkerCallError::Rejected(e.to_string()))?;

        let execution_attempt_id = self.execution_attempt_id;
        let response = self
            .rpc("configure", |mut client| {
            let init_payload_bytes = init_payload_bytes.clone();
            async move {
                client.configure_worker(tonic::Request::new(ConfigureWorkerRequest {
                        init_payload_bytes,
                        execution_attempt_id,
                    })).await
            }
            })
            .await?
            .into_inner();
        if !response.success {
            return Err(WorkerCallError::Rejected(format!(
                "worker {}: {}",
                worker_id, response.error_message
            )));
        }
        Ok(response.execution_graph_signature)
    }

    async fn rpc<T, F, Fut>(&self, op_name: &str, mut op: F) -> Result<T, WorkerCallError>
    where
        F: FnMut(WorkerServiceClient<tonic::transport::Channel>) -> Fut,
        Fut: std::future::Future<Output = Result<T, tonic::Status>>,
    {
        with_retry(op_name, &self.worker_ip, || {
            let fut = op(self.client.clone());
            async move {
                match fut.await {
                    Ok(v) => Attempt::Done(v),
                    Err(status) => status_attempt(status),
                }
            }
        })
        .await
    }

    pub async fn start_worker(&self) -> Result<(), WorkerCallError> {
        let execution_attempt_id = self.execution_attempt_id;
        let response = self
            .rpc("start_worker", |mut client| async move {
                client
                    .start_worker(tonic::Request::new(StartWorkerRequest {
                        execution_attempt_id,
                    }))
                    .await
            })
            .await?
            .into_inner();
        response_result("start", response.success, response.error_message)
    }

    pub async fn run_worker_tasks(&self) -> Result<(), WorkerCallError> {
        let execution_attempt_id = self.execution_attempt_id;
        let response = self
            .rpc("run_worker_tasks", |mut client| async move {
                client
                    .run_worker_tasks(tonic::Request::new(RunWorkerTasksRequest {
                        execution_attempt_id,
                    }))
                    .await
            })
            .await?
            .into_inner();
        response_result("run", response.success, response.error_message)
    }

    pub async fn close_worker_tasks(&self) -> anyhow::Result<bool> {
        let execution_attempt_id = self.execution_attempt_id;
        Ok(self
            .rpc("close_worker_tasks", |mut client| async move {
                client
                    .close_worker_tasks(tonic::Request::new(CloseWorkerTasksRequest {
                        execution_attempt_id,
                    }))
                    .await
            })
            .await
            .map_err(anyhow::Error::new)?
            .into_inner()
            .success)
    }

    pub async fn reset_worker(&self) -> anyhow::Result<bool> {
        let execution_attempt_id = self.execution_attempt_id;
        Ok(self
            .rpc("reset_worker", |mut client| async move {
                client
                    .reset_worker(tonic::Request::new(ResetWorkerRequest {
                        execution_attempt_id,
                    }))
                    .await
            })
            .await
            .map_err(anyhow::Error::new)?
            .into_inner()
            .success)
    }

    pub async fn shutdown_worker(&self) -> anyhow::Result<bool> {
        let execution_attempt_id = self.execution_attempt_id;
        Ok(self
            .rpc("shutdown_worker", |mut client| async move {
                client
                    .shutdown_worker(tonic::Request::new(ShutdownWorkerRequest {
                        execution_attempt_id,
                    }))
                    .await
            })
            .await?
            .into_inner()
            .success)
    }

    pub async fn get_worker_state(&self) -> Result<WorkerSnapshot, WorkerCallError> {
        let execution_attempt_id = self.execution_attempt_id;
        let state_bytes = self
            .rpc("get_worker_state", |mut client| async move {
                client
                    .get_worker_state(tonic::Request::new(GetWorkerStateRequest {
                        execution_attempt_id,
                    }))
                    .await
            })
            .await?
            .into_inner()
            .worker_state_bytes;
        if state_bytes.is_empty() {
            return Err(WorkerCallError::Rejected(
                "worker state is empty".to_string(),
            ));
        }
        WorkerSnapshot::from_bytes(&state_bytes)
            .map_err(|error| WorkerCallError::Rejected(error.to_string()))
    }

    pub async fn trigger_checkpoint_barrier(&self, checkpoint_id: u64) -> anyhow::Result<bool> {
        let execution_attempt_id = self.execution_attempt_id;
        Ok(self
            .rpc("trigger_checkpoint_barrier", |mut client| async move {
                client
                    .trigger_checkpoint_barrier(tonic::Request::new(
                        TriggerCheckpointBarrierRequest {
                            checkpoint_id,
                            execution_attempt_id,
                        },
                    ))
                    .await
            })
            .await
            .map_err(anyhow::Error::new)?
            .into_inner()
            .success)
    }

}

fn response_result(
    operation: &str,
    success: bool,
    error_message: String,
) -> Result<(), WorkerCallError> {
    if success {
        Ok(())
    } else {
        Err(WorkerCallError::Rejected(format!(
            "{}: {}",
            operation, error_message
        )))
    }
}
