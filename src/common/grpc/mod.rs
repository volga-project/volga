//! Shared gRPC construction: limits, channel connect, server serve, typed factories.
//!
//! Call sites should use the factories in [`master`], [`worker`], [`storage`], and
//! [`transport`] instead of `*Client::connect` / ad-hoc message-size setters.

pub mod master;
pub mod storage;
pub mod stubs;
pub mod transport;
pub mod worker;

use std::net::SocketAddr;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Endpoint, Server};

/// Max gRPC message size for control-plane and transport RPCs.
///
/// Must stay above the in-memory window checkpoint inline limit (8 MiB) so
/// checkpoint/restore payloads are not rejected by tonic's 4 MiB default.
pub const GRPC_MAX_MESSAGE_BYTES: usize = 12 * 1024 * 1024;

/// Shared transport settings applied at client/server construction.
#[derive(Debug, Clone)]
pub struct GrpcConfig {
    pub max_decoding_message_bytes: usize,
    pub max_encoding_message_bytes: usize,
    /// HTTP/2 max frame size for [`Server::builder`] (not the gRPC message limit).
    pub max_frame_size: Option<u32>,
    /// When set, applied via [`Endpoint::connect_timeout`].
    ///
    /// `None` matches historical `*Client::connect` (no dial deadline). Only set this
    /// from profile `runtime_consts` at call sites that previously used an Endpoint timeout
    /// (`master.worker_connect_timeout`, `worker.register_connect_timeout`).
    pub connect_timeout: Option<Duration>,
}

impl GrpcConfig {
    /// Message / frame size limits only — no dial timeout.
    pub fn default_limits() -> Self {
        Self {
            max_decoding_message_bytes: GRPC_MAX_MESSAGE_BYTES,
            max_encoding_message_bytes: GRPC_MAX_MESSAGE_BYTES,
            max_frame_size: Some(GRPC_MAX_MESSAGE_BYTES as u32),
            connect_timeout: None,
        }
    }

    pub fn with_connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = Some(connect_timeout);
        self
    }

    pub fn without_frame_size_limit(mut self) -> Self {
        self.max_frame_size = None;
        self
    }
}

/// Retry policy for [`connect_with_retry`].
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Total connection attempts (must be >= 1).
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub backoff: RetryBackoff,
}

#[derive(Debug, Clone, Copy)]
pub enum RetryBackoff {
    /// Sleep `base_delay` between attempts.
    Fixed,
    /// Sleep `base_delay * attempt_number` (1-based) between attempts.
    Linear,
}

impl RetryPolicy {
    pub fn fixed(max_attempts: u32, base_delay: Duration) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            base_delay,
            backoff: RetryBackoff::Fixed,
        }
    }

    pub fn linear(max_attempts: u32, base_delay: Duration) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            base_delay,
            backoff: RetryBackoff::Linear,
        }
    }

    /// Delay before the next attempt after `attempt` (0-based) failed.
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        match self.backoff {
            RetryBackoff::Fixed => self.base_delay,
            RetryBackoff::Linear => self.base_delay.saturating_mul(attempt.saturating_add(1)),
        }
    }
}

/// Apply Volga message-size limits to a generated tonic client or server stub.
pub trait WithMessageLimits: Sized {
    fn with_message_limits(self) -> Self;
}

macro_rules! impl_client_message_limits {
    ($($ty:ty),+ $(,)?) => {$(
        impl WithMessageLimits for $ty {
            fn with_message_limits(self) -> Self {
                self.max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES)
            }
        }
    )+};
}

macro_rules! impl_server_message_limits {
    ($($ty:ty),+ $(,)?) => {$(
        impl<T> WithMessageLimits for $ty {
            fn with_message_limits(self) -> Self {
                self.max_decoding_message_size(GRPC_MAX_MESSAGE_BYTES)
                    .max_encoding_message_size(GRPC_MAX_MESSAGE_BYTES)
            }
        }
    )+};
}

impl_client_message_limits! {
    stubs::master_service::master_service_client::MasterServiceClient<Channel>,
    stubs::worker_service::worker_service_client::WorkerServiceClient<Channel>,
    stubs::in_memory_storage_service::in_memory_storage_service_client::InMemoryStorageServiceClient<Channel>,
    stubs::message_stream::message_stream_service_client::MessageStreamServiceClient<Channel>,
}

impl_server_message_limits! {
    stubs::master_service::master_service_server::MasterServiceServer<T>,
    stubs::worker_service::worker_service_server::WorkerServiceServer<T>,
    stubs::in_memory_storage_service::in_memory_storage_service_server::InMemoryStorageServiceServer<T>,
    stubs::message_stream::message_stream_service_server::MessageStreamServiceServer<T>,
}

fn normalize_endpoint(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

/// Dial a channel (single attempt). Applies `cfg.connect_timeout` when present.
pub async fn connect(addr: &str, cfg: &GrpcConfig) -> Result<Channel, tonic::transport::Error> {
    let mut endpoint = Endpoint::from_shared(normalize_endpoint(addr))?;
    if let Some(timeout) = cfg.connect_timeout {
        endpoint = endpoint.connect_timeout(timeout);
    }
    endpoint.connect().await
}

/// Dial with retries. `max_attempts` includes the first try.
pub async fn connect_with_retry(
    addr: &str,
    cfg: &GrpcConfig,
    retry: &RetryPolicy,
) -> Result<Channel, tonic::transport::Error> {
    let mut last_err = None;
    for attempt in 0..retry.max_attempts {
        match connect(addr, cfg).await {
            Ok(channel) => return Ok(channel),
            Err(e) => {
                last_err = Some(e);
                if attempt + 1 < retry.max_attempts {
                    tokio::time::sleep(retry.delay_for_attempt(attempt)).await;
                }
            }
        }
    }
    Err(last_err.expect("retry loop ran at least once"))
}

/// Build a tonic [`Server`] with optional HTTP/2 frame size from `cfg`.
pub fn server_builder(cfg: &GrpcConfig) -> Server {
    let mut builder = Server::builder();
    if let Some(frame) = cfg.max_frame_size {
        builder = builder.max_frame_size(Some(frame));
    }
    builder
}

/// Own a spawned serve task and its shutdown signal.
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

    /// Signal shutdown and wait for the serve task to finish.
    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
    }

    /// Abort the serve task without graceful shutdown (used from `Drop`).
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

/// Serve a pre-built tonic [`Router`] until `signal` completes.
pub async fn serve_with_shutdown(
    addr: SocketAddr,
    router: tonic::transport::server::Router,
    signal: impl std::future::Future<Output = ()>,
) -> Result<(), tonic::transport::Error> {
    router.serve_with_shutdown(addr, signal).await
}

/// Spawn [`serve_with_shutdown`] and return a handle for graceful stop.
///
/// Build the router with [`server_builder`] + `add_service(...)` at the call site.
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
