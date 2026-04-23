use std::env;
use anyhow::{anyhow, Result};

use volga_stream::runtime::entrypoints::standalone;

fn env_var(key: &str) -> Result<String> {
    env::var(key).map_err(|_| anyhow!("missing env var {key}"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let master_addr = env_var("VOLGA_MASTER_ADDR")?;
    let bind_addr = env_var("VOLGA_WORKER_BIND_ADDR")?;
    let worker_id = env_var("VOLGA_WORKER_ID")?;

    standalone::run_worker(master_addr, bind_addr, worker_id).await
}
