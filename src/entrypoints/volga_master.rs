use anyhow::Result;

use crate::runtime::master_server::MasterServer;

pub async fn run_volga_master(bind_addr: &str) -> Result<()> {
    let mut server = MasterServer::new();
    server.start(bind_addr).await?;
    Ok(())
}
