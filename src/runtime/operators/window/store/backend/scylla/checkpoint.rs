use std::sync::atomic::Ordering;

use anyhow::{anyhow, Result};

use crate::runtime::operators::window::store::backend::{StateVersion, WindowBackendSnapshot};

use super::store::ScyllaWindowStoreClient;

pub(super) async fn checkpoint(client: &ScyllaWindowStoreClient) -> Result<WindowBackendSnapshot> {
    Ok(WindowBackendSnapshot::Versioned {
        version: StateVersion {
            attempt: client.scope.attempt.clone(),
            epoch: client.last_epoch.load(Ordering::Acquire).max(0) as u64,
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
    let session = client.inner.session().await?;
    let prepared = client.inner.prepared().await?;
    session
        .execute_unpaged(
            &prepared.insert_recovery_bases,
            (
                client.scope.namespace.bytes.clone(),
                client.scope.attempt.clone(),
                client.scope.key_group_range.start as i32,
                client.scope.key_group_range.end as i32,
                version.attempt.clone(),
                version.epoch as i64,
            ),
        )
        .await?;
    *client.restore_base.lock().await = Some(version.clone());
    client
        .last_epoch
        .store(version.epoch as i64, Ordering::Release);
    client.head_claims.clear();
    Ok(())
}
