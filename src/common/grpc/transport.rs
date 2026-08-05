use tonic::transport::Channel;

use crate::runtime::consts::{
    runtime_consts, TRANSPORT_GRPC_CONNECT_MAX_RETRIES, TRANSPORT_GRPC_CONNECT_RETRY_DELAY,
};

use super::stubs::message_stream::message_stream_service_client::MessageStreamServiceClient;
use super::stubs::message_stream::message_stream_service_server::{
    MessageStreamService, MessageStreamServiceServer,
};
use super::{connect_with_retry, GrpcConfig, WithMessageLimits};

pub fn transport() -> GrpcConfig {
    GrpcConfig::new().with_retries(
        runtime_consts().u64(TRANSPORT_GRPC_CONNECT_MAX_RETRIES) as u32,
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
    let channel = connect_with_retry(addr, &transport()).await?;
    Ok(MessageStreamServiceClient::new(channel).with_message_limits())
}
