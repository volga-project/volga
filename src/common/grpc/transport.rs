use tonic::transport::Channel;

use crate::runtime::consts::{
    runtime_consts, TRANSPORT_GRPC_CONNECT_MAX_RETRIES, TRANSPORT_GRPC_CONNECT_RETRY_DELAY,
};

use super::stubs::message_stream::message_stream_service_client::MessageStreamServiceClient;
use super::stubs::message_stream::message_stream_service_server::{
    MessageStreamService, MessageStreamServiceServer,
};
use super::{connect_with_retry, GrpcConfig, RetryPolicy, WithMessageLimits};

/// Data-plane servers/clients: message limits only (no dial timeout; matches prior
/// `MessageStreamServiceClient::connect`).
pub fn transport_config() -> GrpcConfig {
    GrpcConfig::default_limits()
}

/// `transport.grpc_connect_max_retries` / `transport.grpc_connect_retry_delay` per profile.
///
/// Historical `MessageStreamClient` treated max_retries as *additional* retries after the
/// first try, so total attempts = max_retries + 1.
pub fn transport_retry_policy() -> RetryPolicy {
    let max_retries = runtime_consts().u64(TRANSPORT_GRPC_CONNECT_MAX_RETRIES) as u32;
    RetryPolicy::fixed(
        max_retries.saturating_add(1),
        runtime_consts().duration(TRANSPORT_GRPC_CONNECT_RETRY_DELAY),
    )
}

pub fn message_stream_server<S>(svc: S) -> MessageStreamServiceServer<S>
where
    S: MessageStreamService,
{
    MessageStreamServiceServer::new(svc).with_message_limits()
}

pub async fn message_stream_client(
    addr: &str,
) -> Result<MessageStreamServiceClient<Channel>, tonic::transport::Error> {
    message_stream_client_with_retry(addr, &transport_config(), &transport_retry_policy()).await
}

pub async fn message_stream_client_with_retry(
    addr: &str,
    cfg: &GrpcConfig,
    retry: &RetryPolicy,
) -> Result<MessageStreamServiceClient<Channel>, tonic::transport::Error> {
    let channel = connect_with_retry(addr, cfg, retry).await?;
    Ok(MessageStreamServiceClient::new(channel).with_message_limits())
}
