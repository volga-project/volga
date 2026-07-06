use std::env;

use anyhow::Result;
use volga::storage::InMemoryStorageServer;

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let bind_addr =
        env::var("VOLGA_TEST_STORAGE_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:50071".to_string());
    let mut storage_server = InMemoryStorageServer::new();
    storage_server.start(&bind_addr).await?;
    println!("[VOLGA_TEST_STORAGE] listening on {}", bind_addr);
    shutdown_signal().await;
    println!("[VOLGA_TEST_STORAGE] shutdown signal received");
    storage_server.stop().await;
    Ok(())
}
