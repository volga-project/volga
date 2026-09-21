//! Scylla WO checkpoint / restore. Barrier captures cuts; publication waits
//! for completion. Request-mode `window_kg_meta` is trigger 1 (complete) and
//! trigger 2 (heal at open).

use std::collections::HashMap;

use anyhow::Result;
use futures::future::try_join_all;

use crate::runtime::operators::window::store::backend::{
    CutHistory, WindowBackendSnapshot, WindowStoreTaskScope,
};

use super::meta::{self, PublishPayload};
use super::store::ScyllaWindowStoreClient;

pub(super) async fn checkpoint(client: &ScyllaWindowStoreClient) -> Result<WindowBackendSnapshot> {
    anyhow::ensure!(
        client.in_flight_key_count() == 0,
        "checkpoint while a key commit is in flight"
    );
    let range = client.scope.key_group_range;
    Ok(WindowBackendSnapshot::Versioned {
        attempt: client.my_attempt(),
        range,
        cuts: client.snapshot_cuts()?,
    })
}

pub(super) async fn restore(
    client: &ScyllaWindowStoreClient,
    snapshot: &WindowBackendSnapshot,
) -> Result<()> {
    let WindowBackendSnapshot::Versioned {
        attempt,
        range,
        cuts,
    } = snapshot
    else {
        anyhow::bail!("Scylla window store requires a Versioned snapshot");
    };
    let scope: &WindowStoreTaskScope = &client.scope;
    anyhow::ensure!(
        *range == scope.key_group_range,
        "Scylla restore requires same assignment (key_group_range)"
    );
    anyhow::ensure!(
        cuts.len() == range.end.saturating_sub(range.start),
        "Versioned cuts must be parallel to the bound key-group range"
    );
    let me = client.my_attempt();
    anyhow::ensure!(
        me >= *attempt,
        "execution attempt {me} does not dominate snapshot attempt {attempt}"
    );
    let mut by_group = HashMap::new();
    for (offset, cut) in cuts.iter().enumerate() {
        let kg = (range.start + offset) as i32;
        for e in cut.entries() {
            anyhow::ensure!(
                me >= e.attempt,
                "execution attempt {me} does not dominate inherited attempt {}",
                e.attempt
            );
        }
        anyhow::ensure!(
            cut.entries().len() <= CutHistory::MAX_ENTRIES,
            "restored cut history for key group {kg} exceeded {} entries",
            CutHistory::MAX_ENTRIES
        );
        by_group.insert(kg, cut.clone());
    }
    client.reset_for_restore(by_group);
    Ok(())
}

pub(super) async fn prepare_attempt(
    client: &ScyllaWindowStoreClient,
    restored: &WindowBackendSnapshot,
    committed_wm: Option<i64>,
    retention_floor: Option<i64>,
    restored_checkpoint_id: Option<u64>,
) -> Result<()> {
    if !client.scope.request_mode {
        return Ok(());
    }
    let WindowBackendSnapshot::Versioned { range, cuts, .. } = restored else {
        anyhow::bail!("Scylla prepare_attempt requires a Versioned snapshot");
    };
    anyhow::ensure!(
        cuts.len() == range.end.saturating_sub(range.start),
        "Versioned cuts must be parallel to the bound key-group range"
    );
    try_join_all(cuts.iter().enumerate().map(|(offset, cut)| {
        let kg = (range.start + offset) as i32;
        meta::heal_or_take(
            client,
            kg,
            cut,
            committed_wm,
            retention_floor,
            restored_checkpoint_id,
        )
    }))
    .await?;
    Ok(())
}

pub(super) async fn on_checkpoint_complete(
    client: &ScyllaWindowStoreClient,
    checkpoint_id: u64,
    snapshot: &WindowBackendSnapshot,
    committed_wm: Option<i64>,
    retention_floor: Option<i64>,
) -> Result<()> {
    let WindowBackendSnapshot::Versioned { range, cuts, .. } = snapshot else {
        anyhow::bail!("Scylla on_checkpoint_complete requires a Versioned snapshot");
    };
    anyhow::ensure!(
        cuts.len() == range.end.saturating_sub(range.start),
        "Versioned cuts must be parallel to the bound key-group range"
    );
    let mut by_group = std::collections::HashMap::new();
    for (offset, cut) in cuts.iter().enumerate() {
        by_group.insert((range.start + offset) as i32, cut.clone());
    }
    client.advance_published_cuts(by_group);
    if !client.scope.request_mode {
        return Ok(());
    }
    try_join_all(cuts.iter().enumerate().map(|(offset, cut)| {
        let kg = (range.start + offset) as i32;
        let payload = PublishPayload {
            cut: cut.clone(),
            committed_wm,
            retention_floor,
            checkpoint_id,
        };
        async move { meta::publish(client, kg, &payload).await }
    }))
    .await?;
    Ok(())
}
