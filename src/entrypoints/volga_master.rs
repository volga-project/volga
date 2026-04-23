use std::env;

use anyhow::{anyhow, Result};
use base64::{engine::general_purpose, Engine as _};

use volga_stream::executor::bootstrap::MasterBootstrapPayload;
use volga_stream::runtime::entrypoints::standalone;

fn env_var(key: &str) -> Result<String> {
    env::var(key).map_err(|_| anyhow!("missing env var {key}"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let bind_addr = env::var("VOLGA_MASTER_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:7000".to_string());
    let worker_addrs_raw = env_var("VOLGA_WORKER_ADDRS")?;
    let bootstrap_b64 = env_var("VOLGA_BOOTSTRAP_B64")?;
    let bootstrap_bytes = general_purpose::STANDARD
        .decode(bootstrap_b64)
        .map_err(|e| anyhow!("failed to decode bootstrap bytes: {e}"))?;
    let bootstrap: MasterBootstrapPayload = bincode::deserialize(&bootstrap_bytes)
        .map_err(|e| anyhow!("failed to decode bootstrap payload: {e}"))?;

    let worker_addrs = worker_addrs_raw
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    standalone::run_master(bind_addr, worker_addrs, bootstrap).await
}
