use anyhow::Result;

use crate::runtime::entrypoints::standalone::fetch_worker_bootstrap;

pub async fn run_volga_worker(master_addr: &str, worker_id: &str) -> Result<()> {
    let _bootstrap = fetch_worker_bootstrap(master_addr, worker_id).await?;
    Ok(())
}
