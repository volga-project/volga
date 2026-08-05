use tonic::transport::Channel;

use crate::runtime::consts::{runtime_consts, MASTER_WORKER_CONNECT_TIMEOUT};

use super::stubs::worker_service::worker_service_client::WorkerServiceClient;
use super::stubs::worker_service::worker_service_server::{WorkerService, WorkerServiceServer};
use super::{connect, connect_with_retry, GrpcConfig, RetryPolicy, WithMessageLimits};

/// Master → worker control-plane dials (`master.worker_connect_timeout` per profile).
pub fn master_to_worker_config() -> GrpcConfig {
    GrpcConfig::default_limits()
        .with_connect_timeout(runtime_consts().duration(MASTER_WORKER_CONNECT_TIMEOUT))
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
    worker_client_with_config(addr, &master_to_worker_config()).await
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
