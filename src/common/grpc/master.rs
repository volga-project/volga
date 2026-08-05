use std::time::Duration;

use tonic::transport::Channel;

use crate::runtime::consts::{
    runtime_consts, WORKER_REGISTER_CONNECT_TIMEOUT, WORKER_REGISTER_MAX_RETRIES,
    WORKER_REGISTER_RETRY_DELAY, WORKER_REGISTER_RPC_TIMEOUT,
};

use super::stubs::master_service::master_service_client::MasterServiceClient;
use super::stubs::master_service::master_service_server::{MasterService, MasterServiceServer};
use super::{connect, connect_with_retry, GrpcConfig, RetryPolicy, WithMessageLimits};

/// Limits-only config for master servers and clients that historically used
/// `MasterServiceClient::connect` with no Endpoint dial timeout (stream_task, harness).
pub fn control_plane_config() -> GrpcConfig {
    GrpcConfig::default_limits()
}

/// Full worker → master registration policy (dial + outer retry + per-RPC timeout).
#[derive(Debug, Clone)]
pub struct WorkerRegisterPolicy {
    pub dial: GrpcConfig,
    pub retry: RetryPolicy,
    pub rpc_timeout: Duration,
}

/// Profile knobs: `worker.register_connect_timeout`, `register_max_retries`,
/// `register_retry_delay`, `register_rpc_timeout`.
pub fn worker_register_policy() -> WorkerRegisterPolicy {
    WorkerRegisterPolicy {
        dial: GrpcConfig::default_limits().with_connect_timeout(
            runtime_consts().duration(WORKER_REGISTER_CONNECT_TIMEOUT),
        ),
        retry: RetryPolicy::linear(
            runtime_consts().u64(WORKER_REGISTER_MAX_RETRIES) as u32,
            runtime_consts().duration(WORKER_REGISTER_RETRY_DELAY),
        ),
        rpc_timeout: runtime_consts().duration(WORKER_REGISTER_RPC_TIMEOUT),
    }
}

pub fn master_server<S>(svc: S) -> MasterServiceServer<S>
where
    S: MasterService,
{
    MasterServiceServer::new(svc).with_message_limits()
}

pub async fn master_client(
    addr: &str,
    cfg: &GrpcConfig,
) -> Result<MasterServiceClient<Channel>, tonic::transport::Error> {
    let channel = connect(addr, cfg).await?;
    Ok(MasterServiceClient::new(channel).with_message_limits())
}

pub async fn master_client_with_retry(
    addr: &str,
    cfg: &GrpcConfig,
    retry: &RetryPolicy,
) -> Result<MasterServiceClient<Channel>, tonic::transport::Error> {
    let channel = connect_with_retry(addr, cfg, retry).await?;
    Ok(MasterServiceClient::new(channel).with_message_limits())
}
