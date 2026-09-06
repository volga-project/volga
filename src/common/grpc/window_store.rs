use std::time::Duration;

use tonic::transport::Channel;

use super::stubs::window_store_service::window_store_service_client::WindowStoreServiceClient;
use super::stubs::window_store_service::window_store_service_server::{
    WindowStoreService, WindowStoreServiceServer,
};
use super::{connect_with_retry, GrpcConfig, WithMessageLimits};

pub fn window_store() -> GrpcConfig {
    GrpcConfig::new().with_retries(5, Duration::from_secs(1))
}

pub fn window_store_server<S>(svc: S) -> WindowStoreServiceServer<S>
where
    S: WindowStoreService,
{
    WindowStoreServiceServer::new(svc).with_message_limits()
}

pub async fn window_store_client(
    addr: &str,
) -> Result<WindowStoreServiceClient<Channel>, tonic::transport::Error> {
    let channel = connect_with_retry(addr, &window_store()).await?;
    Ok(WindowStoreServiceClient::new(channel).with_message_limits())
}
