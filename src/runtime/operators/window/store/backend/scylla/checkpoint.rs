use std::sync::atomic::Ordering;

use anyhow::{anyhow, Result};

use crate::runtime::operators::window::model::PartitionKey;
use crate::runtime::operators::window::store::backend::{StateVersion, WindowBackendSnapshot};

use super::cql::HeadClaim;
use super::store::ScyllaWindowStoreClient;
use super::write;

pub(super) async fn checkpoint(client: &ScyllaWindowStoreClient) -> Result<WindowBackendSnapshot> {
    flush_serving(client).await?;
    Ok(WindowBackendSnapshot::Versioned {
        version: StateVersion {
            attempt: client.scope.attempt.clone(),
            epoch: client.last_epoch.load(Ordering::Acquire).max(0) as u64,
        },
    })
}

/// Promote serving for keys that are live but lagging the writer (interval /
/// checkpoint cadence). OnCommit keys are usually already current; skip if
/// the last promote epoch matches.
async fn flush_serving(client: &ScyllaWindowStoreClient) -> Result<()> {
    let epoch = client.last_epoch.load(Ordering::Acquire);
    if epoch <= 0 {
        return Ok(());
    }
    let session = client.inner.session();
    let keys: Vec<Vec<u8>> = client
        .head_claims
        .iter()
        .filter(|e| matches!(*e.value(), HeadClaim::Pending | HeadClaim::Ours))
        .map(|e| e.key().clone())
        .collect();
    for key in keys {
        if client
            .last_promoted
            .get(&key)
            .is_some_and(|e| e.0 == epoch)
        {
            continue;
        }
        let partition = PartitionKey {
            namespace: client.scope.namespace.bytes.clone(),
            business_key: key,
        };
        let kg = client.key_group(&partition)?;
        write::promote_serving(client, &session, &partition, kg, epoch).await?;
    }
    Ok(())
}

pub(super) async fn restore(
    client: &ScyllaWindowStoreClient,
    snapshot: &WindowBackendSnapshot,
) -> Result<()> {
    let WindowBackendSnapshot::Versioned { version } = snapshot else {
        return Err(anyhow!("Scylla restore requires Versioned snapshot"));
    };
    let session = client.inner.session();
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
    // Overlay pin only. Steal owner on first touch; serving stays at the
    // previous cut until catch-up promote.
    *client.restore_base.lock().await = Some(version.clone());
    client
        .last_epoch
        .store(version.epoch as i64, Ordering::Release);
    client.head_claims.clear();
    client.last_promoted.clear();
    Ok(())
}
