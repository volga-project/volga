pub mod master;
pub mod storage;
pub mod stubs;
pub mod worker;

use std::net::SocketAddr;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Endpoint, Server};

// Above inmem window checkpoint inline limit (8 MiB); tonic default decode is 4 MiB.
pub const GRPC_MAX_MESSAGE_BYTES: usize = 12 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct GrpcConfig {
    pub connect_timeout: Option<Duration>,
    pub max_attempts: u32,
    pub retry_delay: Duration,
    pub rpc_timeout: Option<Duration>,
}

impl GrpcConfig {
    pub fn new() -> Self {
        Self {
            connect_timeout: None,
            max_attempts: 1,
            retry_delay: Duration::ZERO,
            rpc_timeout: None,
        }
    }

    pub fn with_connect_timeout(mut self, t: Duration) -> Self {
        self.connect_timeout = Some(t);
        self
    }

    pub fn with_retries(mut self, max_attempts: u32, retry_delay: Duration) -> Self {
        self.max_attempts = max_attempts.max(1);
        self.retry_delay = retry_delay;
        self
    }

    pub fn with_rpc_timeout(mut self, t: Duration) -> Self {
        self.rpc_timeout = Some(t);
        self
    }
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self::new()
    }
}

pub trait WithMessageLimits: Sized {
    fn with_message_limits(self) -> Self;
}

macro_rules! impl_client_limits {
    ($($ty:ty),+ $(,)?) => {$(
        impl WithMessageLimits for $ty {
            fn with_message_limits(self) -> Self {
                self.max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES)
            }
        }
    )+};
}

macro_rules! impl_server_limits {
    ($($ty:ty),+ $(,)?) => {$(
        impl<T> WithMessageLimits for $ty {
            fn with_message_limits(self) -> Self {
                self.max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES)
            }
        }
    )+};
}

impl_client_limits! {
    stubs::master_service::master_service_client::MasterServiceClient<Channel>,
    stubs::worker_service::worker_service_client::WorkerServiceClient<Channel>,
    stubs::in_memory_storage_service::in_memory_storage_service_client::InMemoryStorageServiceClient<Channel>,
}

impl_server_limits! {
    stubs::master_service::master_service_server::MasterServiceServer<T>,
    stubs::worker_service::worker_service_server::WorkerServiceServer<T>,
    stubs::in_memory_storage_service::in_memory_storage_service_server::InMemoryStorageServiceServer<T>,
}

fn normalize_endpoint(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

pub async fn connect(addr: &str, cfg: &GrpcConfig) -> Result<Channel, tonic::transport::Error> {
    let mut endpoint = Endpoint::from_shared(normalize_endpoint(addr))?;
    if let Some(timeout) = cfg.connect_timeout {
        endpoint = endpoint.connect_timeout(timeout);
    }
    endpoint.connect().await
}

pub async fn connect_with_retry(
    addr: &str,
    cfg: &GrpcConfig,
) -> Result<Channel, tonic::transport::Error> {
    let mut last_err = None;
    for attempt in 0..cfg.max_attempts {
        match connect(addr, cfg).await {
            Ok(channel) => return Ok(channel),
            Err(e) => {
                last_err = Some(e);
                if attempt + 1 < cfg.max_attempts {
                    tokio::time::sleep(cfg.retry_delay).await;
                }
            }
        }
    }
    Err(last_err.expect("retry loop ran at least once"))
}

pub fn server_builder() -> Server {
    Server::builder().max_frame_size(Some(GRPC_MAX_MESSAGE_BYTES as u32))
}

pub struct GrpcServeHandle {
    join: Option<JoinHandle<()>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl GrpcServeHandle {
    fn new(join: JoinHandle<()>, shutdown_tx: oneshot::Sender<()>) -> Self {
        Self {
            join: Some(join),
            shutdown_tx: Some(shutdown_tx),
        }
    }

    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
    }

    pub fn abort(&mut self) {
        self.shutdown_tx.take();
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }
}

impl Drop for GrpcServeHandle {
    fn drop(&mut self) {
        self.abort();
    }
}

pub async fn serve_with_shutdown(
    addr: SocketAddr,
    router: tonic::transport::server::Router,
    signal: impl std::future::Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    router.serve_with_shutdown(addr, signal).await
}

pub fn spawn_with_shutdown(
    addr: SocketAddr,
    router: tonic::transport::server::Router,
) -> GrpcServeHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let join = tokio::spawn(async move {
        let _ = serve_with_shutdown(addr, router, async {
            let _ = shutdown_rx.await;
        })
        .await;
    });
    GrpcServeHandle::new(join, shutdown_tx)
}
