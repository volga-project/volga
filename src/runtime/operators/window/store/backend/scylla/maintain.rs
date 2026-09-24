//! Maintain / GC, from the last completed checkpoint.
//!
//! The floor is `committed_watermark - longest_window - lateness`. A full
//! delete drops every version in a time range. A version trim runs only on
//! what that delete left behind.
//!
//! Full deletes:
//! - a raw minute whose `bucket_start` is below the floor
//! - a tile whose `tile_start + granularity` is at or below the floor
//! - a trigger with `fire_ts` at or below the committed watermark, on a shard
//!   this task fully owns
//!
//! Version trim keeps three versions of one cell and deletes every other
//! stored `(attempt, epoch)`. The newest of the current attempt is the live
//! writer, including epochs written after the last checkpoint. The newest the
//! published cut allows is the checkpoint a reader is pinned to. The newest
//! the previous cut allows is the checkpoint a reader may still be on while
//! the next one is published. This applies to a tile that still overlaps the
//! floor, and to key state, which has no time range. Raw rows inside a live
//! minute are not version-trimmed; the minute delete takes them all.
//!
//! The `window_kg_buckets` row is deleted after the tile trim and the key-state
//! trim succeed, so a failed tick can find the key again.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use anyhow::Result;
use futures::future::try_join_all;
use scylla::client::session::Session;

use crate::runtime::operators::window::store::backend::version::{Attempt, CutHistory, Version};
use crate::runtime::state::OperatorTaskState;

use super::cql::{unlogged_batch, PreparedGc};
use super::schema::{align_down, fully_owned_trigger_shards, RAW_BUCKET_MS};
use super::store::ScyllaWindowStoreClient;

pub(super) fn keep_versions(
    versions: impl IntoIterator<Item = Version>,
    my_attempt: Attempt,
    cut: &CutHistory,
    prev_cut: &CutHistory,
) -> BTreeSet<Version> {
    let mut mine = None;
    let mut at_cut = None;
    let mut at_prev = None;
    for v in versions {
        if v.attempt == my_attempt {
            mine = Some(mine.map_or(v, |m: Version| m.max(v)));
        }
        if cut.allows(v) {
            at_cut = Some(at_cut.map_or(v, |m: Version| m.max(v)));
        }
        if prev_cut.allows(v) {
            at_prev = Some(at_prev.map_or(v, |m: Version| m.max(v)));
        }
    }
    [mine, at_cut, at_prev].into_iter().flatten().collect()
}

#[derive(Clone)]
struct Retention {
    attempt: Attempt,
    cut: CutHistory,
    prev_cut: CutHistory,
}

fn versions_outside_slots(versions: &[Version], slots: &Retention) -> Vec<Version> {
    let keep = keep_versions(
        versions.iter().copied(),
        slots.attempt,
        &slots.cut,
        &slots.prev_cut,
    );
    versions
        .iter()
        .copied()
        .filter(|v| !keep.contains(v))
        .collect()
}

fn retention_for(client: &ScyllaWindowStoreClient, kg: i32) -> Retention {
    Retention {
        attempt: client.my_attempt(),
        cut: client.cp_cut_for(kg),
        prev_cut: client.prev_cut_for(kg),
    }
}

pub(super) async fn maintain(
    client: &ScyllaWindowStoreClient,
    ns: &crate::runtime::operators::window::model::StateNamespace,
    state: &dyn OperatorTaskState,
) -> Result<()> {
    let Some(wo) = state
        .as_any()
        .downcast_ref::<crate::runtime::operators::window::state::WindowOperatorState>()
    else {
        return Ok(());
    };
    let Some((committed_wm, floor)) = wo.committed_retention_cutoff() else {
        return Ok(());
    };
    let session = client.inner.session();
    let gc = client.inner.prepared_gc().await?;
    let range = wo.scope().key_group_range;
    let floor_bucket = align_down(floor, RAW_BUCKET_MS);
    let granularities = wo.tile_granularity_ms();

    let scans = (range.start..range.end).map(|g| {
        let session = Arc::clone(&session);
        let stmt = gc.select_kg_buckets.clone();
        let ns = ns.bytes.clone();
        async move {
            let result = session.execute_unpaged(&stmt, (ns, g as i32)).await?;
            Ok::<_, anyhow::Error>((g as i32, result))
        }
    });

    let mut by_key: BTreeMap<(i32, Vec<u8>), Vec<i64>> = BTreeMap::new();
    for (kg, result) in try_join_all(scans).await? {
        let rows = result.into_rows_result()?;
        for row in rows.rows::<(i64, Vec<u8>)>()? {
            let (bucket_start, business_key) = row?;
            let expired = by_key.entry((kg, business_key)).or_default();
            if bucket_start < floor_bucket {
                expired.push(bucket_start);
            }
        }
    }

    let mut slots_by_kg: HashMap<i32, Retention> = HashMap::new();
    for ((kg, _), _) in &by_key {
        slots_by_kg
            .entry(*kg)
            .or_insert_with(|| retention_for(client, *kg));
    }

    let mut key_futs = Vec::new();
    for ((kg, key), expired) in by_key {
        let session = Arc::clone(&session);
        let gc = gc.clone();
        let ns = ns.bytes.clone();
        let granularities = granularities.clone();
        let slots = slots_by_kg
            .get(&kg)
            .cloned()
            .unwrap_or_else(|| retention_for(client, kg));
        key_futs.push(async move {
            gc_key(
                session,
                gc,
                ns,
                kg,
                key,
                expired,
                granularities,
                floor,
                slots,
            )
            .await
        });
    }

    let mut trigger_futs = Vec::new();
    for shard in fully_owned_trigger_shards(range, wo.scope().max_parallelism) {
        let session = Arc::clone(&session);
        let stmt = gc.delete_triggers.clone();
        let ns = ns.bytes.clone();
        trigger_futs.push(async move {
            session
                .execute_unpaged(&stmt, (ns, shard, committed_wm))
                .await?;
            Ok::<_, anyhow::Error>(())
        });
    }
    tokio::try_join!(try_join_all(key_futs), try_join_all(trigger_futs))?;
    Ok(())
}

/// First `tile_start` kept. A tile stays while `tile_start + granularity > floor`.
fn tile_keep_from(floor: i64, granularity_ms: i64) -> i64 {
    match floor.checked_sub(granularity_ms) {
        Some(start) => start.saturating_add(1),
        None => i64::MIN,
    }
}

async fn gc_key(
    session: Arc<Session>,
    gc: PreparedGc,
    ns: Vec<u8>,
    kg: i32,
    key: Vec<u8>,
    expired: Vec<i64>,
    granularities: Vec<i64>,
    floor: i64,
    slots: Retention,
) -> Result<()> {
    let raw_futs = expired.iter().copied().map(|bucket| {
        let session = Arc::clone(&session);
        let stmt = gc.delete_raw.clone();
        let ns = ns.clone();
        let key = key.clone();
        async move {
            session
                .execute_unpaged(&stmt, (ns, kg, key, bucket))
                .await?;
            Ok::<_, anyhow::Error>(())
        }
    });
    let tile_futs = granularities.iter().copied().map(|gran| {
        gc_tile_gran(
            Arc::clone(&session),
            gc.clone(),
            ns.clone(),
            kg,
            key.clone(),
            gran,
            floor,
            slots.clone(),
        )
    });
    let key_state = trim_key_state(
        Arc::clone(&session),
        gc.clone(),
        ns.clone(),
        kg,
        key.clone(),
        slots.clone(),
    );
    tokio::try_join!(try_join_all(raw_futs), try_join_all(tile_futs), key_state)?;

    // Index row last, after the raw delete, tile GC, and key-state trim.
    // A failure before those two leaves the key for the next tick.
    // The raw delete is idempotent.
    let index_futs = expired.into_iter().map(|bucket| {
        let session = Arc::clone(&session);
        let stmt = gc.delete_kg_buckets.clone();
        let ns = ns.clone();
        let key = key.clone();
        async move {
            session
                .execute_unpaged(&stmt, (ns, kg, bucket, key))
                .await?;
            Ok::<_, anyhow::Error>(())
        }
    });
    try_join_all(index_futs).await?;
    Ok(())
}

async fn gc_tile_gran(
    session: Arc<Session>,
    gc: PreparedGc,
    ns: Vec<u8>,
    kg: i32,
    key: Vec<u8>,
    gran: i64,
    floor: i64,
    slots: Retention,
) -> Result<()> {
    session
        .execute_unpaged(
            &gc.delete_tiles,
            (
                ns.clone(),
                kg,
                key.clone(),
                gran,
                tile_keep_from(floor, gran),
            ),
        )
        .await?;
    let tiles = session
        .execute_unpaged(
            &gc.select_tile_versions,
            (ns.clone(), kg, key.clone(), gran),
        )
        .await?;
    let mut by_cell: HashMap<i64, Vec<Version>> = HashMap::new();
    for row in tiles.into_rows_result()?.rows::<(i64, i64, i64)>()? {
        let (tile_start, attempt, epoch) = row?;
        by_cell.entry(tile_start).or_default().push(Version {
            attempt: attempt as Attempt,
            epoch: epoch as u64,
        });
    }
    let mut tile_deletes = Vec::new();
    for (tile_start, cell) in by_cell {
        for v in versions_outside_slots(&cell, &slots) {
            tile_deletes.push((
                ns.clone(),
                kg,
                key.clone(),
                gran,
                tile_start,
                v.attempt as i64,
                v.epoch as i64,
            ));
        }
    }
    unlogged_batch(&session, &gc.delete_tile_version, tile_deletes).await?;
    Ok(())
}

async fn trim_key_state(
    session: Arc<Session>,
    gc: PreparedGc,
    ns: Vec<u8>,
    kg: i32,
    key: Vec<u8>,
    slots: Retention,
) -> Result<()> {
    let key_rows = session
        .execute_unpaged(&gc.select_key_state_versions, (ns.clone(), kg, key.clone()))
        .await?;
    let mut versions = Vec::new();
    for row in key_rows.into_rows_result()?.rows::<(i64, i64)>()? {
        let (attempt, epoch) = row?;
        versions.push(Version {
            attempt: attempt as Attempt,
            epoch: epoch as u64,
        });
    }
    let mut deletes = Vec::new();
    for v in versions_outside_slots(&versions, &slots) {
        deletes.push((
            ns.clone(),
            kg,
            key.clone(),
            v.attempt as i64,
            v.epoch as i64,
        ));
    }
    unlogged_batch(&session, &gc.delete_key_state_version, deletes).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keep_versions_holds_mine_cut_and_prev() {
        let cut = CutHistory::empty().advance(1, Some(4));
        let prev = CutHistory::empty().advance(1, Some(2));
        let versions = [
            Version {
                attempt: 1,
                epoch: 10,
            },
            Version {
                attempt: 1,
                epoch: 4,
            },
            Version {
                attempt: 1,
                epoch: 2,
            },
            Version {
                attempt: 1,
                epoch: 0,
            },
            Version {
                attempt: 2,
                epoch: 0,
            },
        ];
        let keep = keep_versions(versions, 2, &cut, &prev);
        assert!(keep.contains(&Version {
            attempt: 2,
            epoch: 0
        }));
        assert!(keep.contains(&Version {
            attempt: 1,
            epoch: 4
        }));
        assert!(keep.contains(&Version {
            attempt: 1,
            epoch: 2
        }));
        assert!(!keep.contains(&Version {
            attempt: 1,
            epoch: 10
        }));
        assert!(!keep.contains(&Version {
            attempt: 1,
            epoch: 0
        }));
    }

    #[test]
    fn tile_keep_from_matches_in_memory_overlap() {
        assert_eq!(super::tile_keep_from(10_000, 1_000), 9_001);
        assert_eq!(super::tile_keep_from(0, 1_000), -999);
    }
}
