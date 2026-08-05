use std::time::Duration;

use tonic::transport::Channel;

use crate::runtime::consts::{
    runtime_consts, MASTER_RESET_WORKER_TIMEOUT, MASTER_RPC_MAX_RETRIES, MASTER_RPC_RETRY_DELAY,
    MASTER_WORKER_CONNECT_TIMEOUT,
};

use super::stubs::worker_service::worker_service_client::WorkerServiceClient;
use super::stubs::worker_service::worker_service_server::{WorkerService, WorkerServiceServer};
use super::{connect, connect_with_retry, GrpcConfig, RetryPolicy, WithMessageLimits};

/// Master → worker control-plane policy (dial + RPC retry + reset timeout).
#[derive(Debug, Clone)]
pub struct MasterToWorkerPolicy {
    pub dial: GrpcConfig,
    /// Retries for control RPCs / dial after connect (`master.rpc_*`).
    pub rpc_retry: RetryPolicy,
    pub reset_worker_timeout: Duration,
}

/// Profile knobs: `master.worker_connect_timeout`, `master.rpc_max_retries`,
/// `master.rpc_retry_delay`, `master.reset_worker_timeout`.
pub fn master_to_worker_policy() -> MasterToWorkerPolicy {
    MasterToWorkerPolicy {
        dial: GrpcConfig::default_limits()
            .with_connect_timeout(runtime_consts().duration(MASTER_WORKER_CONNECT_TIMEOUT)),
        rpc_retry: RetryPolicy::linear(
            runtime_consts().u64(MASTER_RPC_MAX_RETRIES) as u32,
            runtime_consts().duration(MASTER_RPC_RETRY_DELAY),
        ),
        reset_worker_timeout: runtime_consts().duration(MASTER_RESET_WORKER_TIMEOUT),
    }
}

pub fn worker_server<S>(svc: S) -> WorkerServiceServer<S>
where
    S: WorkerService,
{
    WorkerServiceServer::new(svc).with_message_limits()
}

pub async fn worker_client(
    addr: &str,
) -> Result<WorkerServiceClient<Channel>, tonic::transport::Error> {
    worker_client_with_config(addr, &master_to_worker_policy().dial).await
}

pub async fn worker_client_with_config(
    addr: &str,
    cfg: &GrpcConfig,
) -> Result<WorkerServiceClient<Channel>, tonic::transport::Error> {
    let channel = connect(addr, cfg).await?;
    Ok(WorkerServiceClient::new(channel).with_message_limits())
}

pub async fn worker_client_with_retry(
    addr: &str,
    cfg: &GrpcConfig,
    retry: &RetryPolicy,
) -> Result<WorkerServiceClient<Channel>, tonic::transport::Error> {
    let channel = connect_with_retry(addr, cfg, retry).await?;
    Ok(WorkerServiceClient::new(channel).with_message_limits())
}
