use tonic::transport::Channel;

use crate::runtime::consts::{runtime_consts, WORKER_REGISTER_CONNECT_TIMEOUT};

use super::stubs::master_service::master_service_client::MasterServiceClient;
use super::stubs::master_service::master_service_server::{MasterService, MasterServiceServer};
use super::{connect, connect_with_retry, GrpcConfig, RetryPolicy, WithMessageLimits};

/// Limits-only config for master servers and clients that historically used
/// `MasterServiceClient::connect` with no Endpoint dial timeout (stream_task, harness).
pub fn control_plane_config() -> GrpcConfig {
    GrpcConfig::default_limits()
}

/// Worker → master registration dials (`worker.register_connect_timeout` per profile).
pub fn worker_register_config() -> GrpcConfig {
    GrpcConfig::default_limits()
        .with_connect_timeout(runtime_consts().duration(WORKER_REGISTER_CONNECT_TIMEOUT))
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
