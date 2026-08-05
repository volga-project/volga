use tonic::transport::Channel;

use crate::runtime::consts::{
    runtime_consts, WORKER_REGISTER_CONNECT_TIMEOUT, WORKER_REGISTER_MAX_RETRIES,
    WORKER_REGISTER_RETRY_DELAY, WORKER_REGISTER_RPC_TIMEOUT,
};

use super::stubs::master_service::master_service_client::MasterServiceClient;
use super::stubs::master_service::master_service_server::{MasterService, MasterServiceServer};
use super::{connect_with_retry, GrpcConfig, WithMessageLimits};

pub fn worker_register() -> GrpcConfig {
    GrpcConfig::new()
        .with_connect_timeout(runtime_consts().duration(WORKER_REGISTER_CONNECT_TIMEOUT))
        .with_retries(
            runtime_consts().u64(WORKER_REGISTER_MAX_RETRIES) as u32,
            runtime_consts().duration(WORKER_REGISTER_RETRY_DELAY),
        )
        .with_rpc_timeout(runtime_consts().duration(WORKER_REGISTER_RPC_TIMEOUT))
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
    let channel = connect_with_retry(addr, cfg).await?;
    Ok(MasterServiceClient::new(channel).with_message_limits())
}
