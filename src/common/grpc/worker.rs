use tonic::transport::Channel;

use crate::runtime::consts::{
    runtime_consts, MASTER_RPC_MAX_RETRIES, MASTER_RPC_RETRY_DELAY, MASTER_WORKER_CONNECT_TIMEOUT,
};

use super::stubs::worker_service::worker_service_client::WorkerServiceClient;
use super::stubs::worker_service::worker_service_server::{WorkerService, WorkerServiceServer};
use super::{connect_with_retry, GrpcConfig, WithMessageLimits};

pub fn master_to_worker() -> GrpcConfig {
    GrpcConfig::new()
        .with_connect_timeout(runtime_consts().duration(MASTER_WORKER_CONNECT_TIMEOUT))
        .with_retries(
            runtime_consts().u64(MASTER_RPC_MAX_RETRIES) as u32,
            runtime_consts().duration(MASTER_RPC_RETRY_DELAY),
        )
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
    let channel = connect_with_retry(addr, &master_to_worker()).await?;
    Ok(WorkerServiceClient::new(channel).with_message_limits())
}
