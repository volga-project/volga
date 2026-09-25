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
use std::time::Instant;

use anyhow::Result;
use futures::future::try_join_all;
use scylla::client::session::Session;

use crate::runtime::operators::window::metrics;
use crate::runtime::operators::window::store::backend::version::{Attempt, CutHistory, Version};
use crate::runtime::state::OperatorTaskState;

use super::cql::{unlogged_batch, PreparedGc};
use super::observe::{self, note_rows, note_stmt, CallMeter};
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
    also: Option<Box<Retention>>,
}

struct TickStats {
    kg_buckets_live: u64,
    keys: u64,
    kg_buckets_expired: u64,
    tile_version_rows: u64,
    key_state_rows: u64,
    raw_partitions_deleted: u64,
    tile_range_deletes: u64,
    trigger_shard_deletes: u64,
    tile_versions_deleted: u64,
    key_state_versions_deleted: u64,
}

fn versions_to_drop(versions: &[Version], retention: &Retention) -> Vec<Version> {
    let mut keep = keep_versions(
        versions.iter().copied(),
        retention.attempt,
        &retention.cut,
        &retention.prev_cut,
    );
    if let Some(also) = &retention.also {
        keep.extend(keep_versions(
            versions.iter().copied(),
            also.attempt,
            &also.cut,
            &also.prev_cut,
        ));
    }
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
        also: None,
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
    // Gauges follow the live watermark. Deletes wait for a committed checkpoint.
    let Some((_, live_floor)) = wo.retention_cutoff() else {
        return Ok(());
    };
    let committed = wo.committed_retention_cutoff();
    if !client.begin_maintain_tick() {
        return Ok(());
    }
    let meter = client.call_meter();
    observe::observe(
        &meter,
        "maintain",
        run_maintain(client, ns, wo, live_floor, committed, &meter),
    )
    .await
}

async fn run_maintain(
    client: &ScyllaWindowStoreClient,
    ns: &crate::runtime::operators::window::model::StateNamespace,
    wo: &crate::runtime::operators::window::state::WindowOperatorState,
    live_floor: i64,
    committed: Option<(i64, i64)>,
    meter: &CallMeter,
) -> Result<()> {
    let session = client.inner.session();
    let gc = client.inner.prepared_gc().await?;
    let range = wo.scope().key_group_range;
    let floor_bucket = align_down(live_floor, RAW_BUCKET_MS);
    let committed_floor_bucket = committed.map(|(_, floor)| align_down(floor, RAW_BUCKET_MS));
    let granularities = wo.tile_granularity_ms();

    let started = Instant::now();
    let scans = (range.start..range.end).map(|g| {
        let session = Arc::clone(&session);
        let stmt = gc.select_kg_buckets.clone();
        let ns = ns.bytes.clone();
        async move {
            note_stmt();
            let result = session.execute_unpaged(&stmt, (ns, g as i32)).await?;
            Ok::<_, anyhow::Error>((g as i32, result))
        }
    });

    let mut by_key: BTreeMap<(i32, Vec<u8>), Vec<i64>> = BTreeMap::new();
    let mut kg_buckets_live = 0u64;
    let mut kg_buckets_expired = 0u64;
    for (kg, result) in try_join_all(scans).await? {
        let rows = result.into_rows_result()?;
        for row in rows.rows::<(i64, Vec<u8>)>()? {
            let (bucket_start, business_key) = row?;
            note_rows(1);
            let expired = by_key.entry((kg, business_key)).or_default();
            if bucket_start < floor_bucket {
                kg_buckets_expired += 1;
            } else {
                kg_buckets_live += 1;
            }
            if committed_floor_bucket.is_some_and(|bound| bucket_start < bound) {
                expired.push(bucket_start);
            }
        }
    }
    phase_done(meter, "index_scan", started);

    let keys = by_key.len() as u64;
    let Some((committed_wm, committed_floor)) = committed else {
        publish_tick(
            meter,
            &TickStats {
                kg_buckets_live,
                keys,
                kg_buckets_expired,
                tile_version_rows: 0,
                key_state_rows: 0,
                raw_partitions_deleted: 0,
                tile_range_deletes: 0,
                trigger_shard_deletes: 0,
                tile_versions_deleted: 0,
                key_state_versions_deleted: 0,
            },
        );
        return Ok(());
    };

    let kgs: Vec<i32> = {
        let mut seen = BTreeSet::new();
        for ((kg, _), _) in &by_key {
            seen.insert(*kg);
        }
        seen.into_iter().collect()
    };
    let retention_by_kg: HashMap<i32, Retention> = try_join_all(
        kgs.into_iter()
            .map(|kg| async move { Ok::<_, anyhow::Error>((kg, retention_for(client, kg))) }),
    )
    .await?
    .into_iter()
    .collect();

    let key_started = Instant::now();
    let mut key_futs = Vec::new();
    for ((kg, key), expired) in by_key {
        let session = Arc::clone(&session);
        let gc = gc.clone();
        let ns = ns.bytes.clone();
        let granularities = granularities.clone();
        let retention = retention_by_kg
            .get(&kg)
            .cloned()
            .expect("retention for a scanned key group");
        key_futs.push(async move {
            gc_key(
                session,
                gc,
                ns,
                kg,
                key,
                expired,
                granularities,
                committed_floor,
                retention,
            )
            .await
        });
    }

    let trigger_started = Instant::now();
    let shards = fully_owned_trigger_shards(range, wo.scope().max_parallelism);
    let mut trigger_futs = Vec::new();
    for shard in shards {
        let session = Arc::clone(&session);
        let stmt = gc.delete_triggers.clone();
        let ns = ns.bytes.clone();
        trigger_futs.push(async move {
            note_stmt();
            session
                .execute_unpaged(&stmt, (ns, shard, committed_wm))
                .await?;
            Ok::<_, anyhow::Error>(())
        });
    }
    let trigger_shard_deletes = trigger_futs.len() as u64;
    let key_work = async move {
        let stats = try_join_all(key_futs).await?;
        phase_done(meter, "key_gc", key_started);
        Ok::<_, anyhow::Error>(stats)
    };
    let trigger_work = async move {
        try_join_all(trigger_futs).await?;
        phase_done(meter, "trigger_delete", trigger_started);
        Ok(())
    };
    let (key_stats, ()) = tokio::try_join!(key_work, trigger_work)?;

    let mut raw_partitions_deleted = 0u64;
    let mut tile_range_deletes = 0u64;
    let mut tile_version_rows = 0u64;
    let mut tile_versions_deleted = 0u64;
    let mut key_state_rows = 0u64;
    let mut key_state_versions_deleted = 0u64;
    for stats in key_stats {
        raw_partitions_deleted += stats.0;
        tile_range_deletes += stats.1;
        tile_version_rows += stats.2;
        tile_versions_deleted += stats.3;
        key_state_rows += stats.4;
        key_state_versions_deleted += stats.5;
    }
    publish_tick(
        meter,
        &TickStats {
            kg_buckets_live,
            keys,
            kg_buckets_expired,
            tile_version_rows,
            key_state_rows,
            raw_partitions_deleted,
            tile_range_deletes,
            trigger_shard_deletes,
            tile_versions_deleted,
            key_state_versions_deleted,
        },
    );
    Ok(())
}

fn phase_done(meter: &CallMeter, phase: &'static str, started: Instant) {
    let Some(labels) = meter.labels.as_ref() else {
        return;
    };
    metrics::record_scylla_maintain_phase(
        &meter.task_id,
        labels,
        phase,
        started.elapsed().as_secs_f64() * 1000.0,
    );
}

fn publish_tick(meter: &CallMeter, stats: &TickStats) {
    let Some(labels) = meter.labels.as_ref() else {
        return;
    };
    let task_id = meter.task_id.as_str();
    for (name, value) in [
        (
            metrics::METRIC_WO_SCYLLA_KG_BUCKETS_LIVE,
            stats.kg_buckets_live,
        ),
        (metrics::METRIC_WO_SCYLLA_KEYS, stats.keys),
        (
            metrics::METRIC_WO_SCYLLA_KG_BUCKETS_EXPIRED,
            stats.kg_buckets_expired,
        ),
        (
            metrics::METRIC_WO_SCYLLA_TILE_VERSION_ROWS,
            stats.tile_version_rows,
        ),
        (
            metrics::METRIC_WO_SCYLLA_KEY_STATE_ROWS,
            stats.key_state_rows,
        ),
    ] {
        metrics::set_scylla_gauge(task_id, labels, name, value as f64);
    }
    metrics::add_scylla_counter(
        task_id,
        labels,
        metrics::METRIC_WO_SCYLLA_RAW_PARTITIONS_DELETED,
        stats.raw_partitions_deleted,
    );
    metrics::add_scylla_counter(
        task_id,
        labels,
        metrics::METRIC_WO_SCYLLA_TILE_RANGE_DELETES,
        stats.tile_range_deletes,
    );
    metrics::add_scylla_counter(
        task_id,
        labels,
        metrics::METRIC_WO_SCYLLA_TRIGGER_SHARD_DELETES,
        stats.trigger_shard_deletes,
    );
    metrics::add_scylla_version_rows_deleted(task_id, labels, "tiles", stats.tile_versions_deleted);
    metrics::add_scylla_version_rows_deleted(
        task_id,
        labels,
        "key_states",
        stats.key_state_versions_deleted,
    );
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
    retention: Retention,
) -> Result<(u64, u64, u64, u64, u64, u64)> {
    let raw_partitions_deleted = expired.len() as u64;
    let raw_futs = expired.iter().copied().map(|bucket| {
        let session = Arc::clone(&session);
        let stmt = gc.delete_raw.clone();
        let ns = ns.clone();
        let key = key.clone();
        async move {
            note_stmt();
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
            retention.clone(),
        )
    });
    let key_state = trim_key_state(
        Arc::clone(&session),
        gc.clone(),
        ns.clone(),
        kg,
        key.clone(),
        retention.clone(),
    );
    let (_, tile_stats, (key_state_rows, key_state_versions_deleted)) =
        tokio::try_join!(try_join_all(raw_futs), try_join_all(tile_futs), key_state)?;

    let mut tile_range_deletes = 0u64;
    let mut tile_version_rows = 0u64;
    let mut tile_versions_deleted = 0u64;
    for (issued, seen, deleted) in tile_stats {
        tile_range_deletes += issued;
        tile_version_rows += seen;
        tile_versions_deleted += deleted;
    }

    // Index row last, after the raw delete, tile GC, and key-state trim.
    // A failure before those two leaves the key for the next tick.
    // The raw delete is idempotent.
    let index_futs = expired.into_iter().map(|bucket| {
        let session = Arc::clone(&session);
        let stmt = gc.delete_kg_buckets.clone();
        let ns = ns.clone();
        let key = key.clone();
        async move {
            note_stmt();
            session
                .execute_unpaged(&stmt, (ns, kg, bucket, key))
                .await?;
            Ok::<_, anyhow::Error>(())
        }
    });
    try_join_all(index_futs).await?;
    Ok((
        raw_partitions_deleted,
        tile_range_deletes,
        tile_version_rows,
        tile_versions_deleted,
        key_state_rows,
        key_state_versions_deleted,
    ))
}

async fn gc_tile_gran(
    session: Arc<Session>,
    gc: PreparedGc,
    ns: Vec<u8>,
    kg: i32,
    key: Vec<u8>,
    gran: i64,
    floor: i64,
    retention: Retention,
) -> Result<(u64, u64, u64)> {
    note_stmt();
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
    note_stmt();
    let tiles = session
        .execute_unpaged(
            &gc.select_tile_versions,
            (ns.clone(), kg, key.clone(), gran),
        )
        .await?;
    let mut seen = 0u64;
    let mut by_cell: HashMap<i64, Vec<Version>> = HashMap::new();
    for row in tiles.into_rows_result()?.rows::<(i64, i64, i64)>()? {
        let (tile_start, attempt, epoch) = row?;
        note_rows(1);
        seen += 1;
        by_cell.entry(tile_start).or_default().push(Version {
            attempt: attempt as Attempt,
            epoch: epoch as u64,
        });
    }
    let mut tile_deletes = Vec::new();
    for (tile_start, cell) in by_cell {
        for v in versions_to_drop(&cell, &retention) {
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
    let deleted = tile_deletes.len() as u64;
    unlogged_batch(&session, &gc.delete_tile_version, tile_deletes).await?;
    Ok((1, seen, deleted))
}

async fn trim_key_state(
    session: Arc<Session>,
    gc: PreparedGc,
    ns: Vec<u8>,
    kg: i32,
    key: Vec<u8>,
    retention: Retention,
) -> Result<(u64, u64)> {
    note_stmt();
    let key_rows = session
        .execute_unpaged(&gc.select_key_state_versions, (ns.clone(), kg, key.clone()))
        .await?;
    let mut versions = Vec::new();
    for row in key_rows.into_rows_result()?.rows::<(i64, i64)>()? {
        let (attempt, epoch) = row?;
        note_rows(1);
        versions.push(Version {
            attempt: attempt as Attempt,
            epoch: epoch as u64,
        });
    }
    let mut deletes = Vec::new();
    for v in versions_to_drop(&versions, &retention) {
        deletes.push((
            ns.clone(),
            kg,
            key.clone(),
            v.attempt as i64,
            v.epoch as i64,
        ));
    }
    let key_deleted = deletes.len() as u64;
    let key_kept = versions.len() as u64 - key_deleted;
    unlogged_batch(&session, &gc.delete_key_state_version, deletes).await?;
    Ok((key_kept, key_deleted))
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
    fn either_cut_keeps_its_version() {
        let versions = [
            Version {
                attempt: 1,
                epoch: 10,
            },
            Version {
                attempt: 1,
                epoch: 6,
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
        ];
        let retention = Retention {
            attempt: 1,
            cut: CutHistory::empty().advance(1, Some(4)),
            prev_cut: CutHistory::empty().advance(1, Some(2)),
            also: Some(Box::new(Retention {
                attempt: 1,
                cut: CutHistory::empty().advance(1, Some(6)),
                prev_cut: CutHistory::empty().advance(1, Some(4)),
                also: None,
            })),
        };
        let dropped = versions_to_drop(&versions, &retention);
        assert!(dropped.contains(&Version {
            attempt: 1,
            epoch: 0
        }));
        assert!(!dropped.contains(&Version {
            attempt: 1,
            epoch: 6
        }));
        assert!(!dropped.contains(&Version {
            attempt: 1,
            epoch: 2
        }));
    }

    #[test]
    fn tile_keep_from_matches_in_memory_overlap() {
        assert_eq!(super::tile_keep_from(10_000, 1_000), 9_001);
        assert_eq!(super::tile_keep_from(0, 1_000), -999);
    }
}
