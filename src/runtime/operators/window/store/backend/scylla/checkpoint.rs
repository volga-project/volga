//! Scylla WO checkpoint / restore. The barrier captures cuts. Completion does
//! not write Scylla; the next attempt restores the snapshot it is given.

use std::collections::HashMap;

use anyhow::Result;

use crate::runtime::operators::window::store::backend::{
    CutHistory, WindowBackendSnapshot, WindowStoreTaskScope,
};

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
        me > *attempt,
        "execution attempt {me} must be newer than snapshot attempt {attempt}"
    );
    let mut by_group = HashMap::new();
    for (offset, cut) in cuts.iter().enumerate() {
        let kg = (range.start + offset) as i32;
        for e in cut.entries() {
            anyhow::ensure!(
                me > e.attempt,
                "execution attempt {me} must be newer than inherited attempt {}",
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

pub(super) async fn on_checkpoint_complete(
    client: &ScyllaWindowStoreClient,
    _checkpoint_id: u64,
    snapshot: &WindowBackendSnapshot,
    _committed_wm: Option<i64>,
    _retention_floor: Option<i64>,
) -> Result<()> {
    let WindowBackendSnapshot::Versioned { range, cuts, .. } = snapshot else {
        anyhow::bail!("Scylla on_checkpoint_complete requires a Versioned snapshot");
    };
    anyhow::ensure!(
        cuts.len() == range.end.saturating_sub(range.start),
        "Versioned cuts must be parallel to the bound key-group range"
    );
    let mut by_group = HashMap::new();
    for (offset, cut) in cuts.iter().enumerate() {
        by_group.insert((range.start + offset) as i32, cut.clone());
    }
    client.advance_published_cuts(by_group);
    Ok(())
}
