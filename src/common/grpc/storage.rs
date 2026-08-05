use std::time::Duration;

use tonic::transport::Channel;

use super::stubs::in_memory_storage_service::in_memory_storage_service_client::InMemoryStorageServiceClient;
use super::stubs::in_memory_storage_service::in_memory_storage_service_server::{
    InMemoryStorageService, InMemoryStorageServiceServer,
};
use super::{connect_with_retry, GrpcConfig, WithMessageLimits};

pub fn storage() -> GrpcConfig {
    GrpcConfig::new().with_retries(5, Duration::from_secs(1))
}

pub fn storage_server<S>(svc: S) -> InMemoryStorageServiceServer<S>
where
    S: InMemoryStorageService,
{
    InMemoryStorageServiceServer::new(svc).with_message_limits()
}

pub async fn storage_client(
    addr: &str,
) -> Result<InMemoryStorageServiceClient<Channel>, tonic::transport::Error> {
    let channel = connect_with_retry(addr, &storage()).await?;
    Ok(InMemoryStorageServiceClient::new(channel).with_message_limits())
}
