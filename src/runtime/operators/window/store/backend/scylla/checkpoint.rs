use std::sync::atomic::Ordering;

use anyhow::{anyhow, Result};

use crate::runtime::operators::window::store::backend::{StateVersion, WindowBackendSnapshot};

use super::lease;
use super::store::ScyllaWindowStoreClient;

pub(super) async fn checkpoint(client: &ScyllaWindowStoreClient) -> Result<WindowBackendSnapshot> {
    lease::flush_stolen(client).await?;
    Ok(WindowBackendSnapshot::Versioned {
        version: StateVersion {
            attempt: client.scope.attempt.clone(),
            epoch: client.writer_epoch().max(0) as u64,
        },
    })
}

pub(super) async fn restore(
    client: &ScyllaWindowStoreClient,
    snapshot: &WindowBackendSnapshot,
) -> Result<()> {
    let WindowBackendSnapshot::Versioned { version } = snapshot else {
        return Err(anyhow!("Scylla restore requires Versioned snapshot"));
    };
    client.set_restore_base(version.clone());
    client
        .last_epoch
        .store(version.epoch as i64, Ordering::Release);
    client.clear_stolen();
    Ok(())
}
