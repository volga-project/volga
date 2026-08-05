use std::time::Duration;

use tonic::transport::Channel;

use super::stubs::in_memory_storage_service::in_memory_storage_service_client::InMemoryStorageServiceClient;
use super::stubs::in_memory_storage_service::in_memory_storage_service_server::{
    InMemoryStorageService, InMemoryStorageServiceServer,
};
use super::{connect_with_retry, GrpcConfig, RetryPolicy, WithMessageLimits};

/// Historical inmem client defaults (not profile-driven): 5 attempts, linear 1s backoff.
pub fn inmem_retry_policy() -> RetryPolicy {
    RetryPolicy::linear(5, Duration::from_secs(1))
}

/// Limits only — no dial timeout (matches prior `InMemoryStorageServiceClient::connect`).
pub fn storage_config() -> GrpcConfig {
    GrpcConfig::default_limits()
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
    storage_client_with_retry(addr, &storage_config(), &inmem_retry_policy()).await
}

pub async fn storage_client_with_retry(
    addr: &str,
    cfg: &GrpcConfig,
    retry: &RetryPolicy,
) -> Result<InMemoryStorageServiceClient<Channel>, tonic::transport::Error> {
    let channel = connect_with_retry(addr, cfg, retry).await?;
    Ok(InMemoryStorageServiceClient::new(channel).with_message_limits())
}
