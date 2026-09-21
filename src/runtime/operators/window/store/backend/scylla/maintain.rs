//! Maintain / GC: committed-floor bucket deletes and three-slot version retention.
//!
//! Slot 1 is `my_attempt` (streaming) or `window_kg_meta.cur_attempt` (request).
//! Slots 2–3 are `cut` and `prev_cut`. Consumed-due GC waits on a due-bucket
//! index (deferred).

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use anyhow::Result;
use futures::future::try_join_all;
use scylla::client::session::Session;

use crate::runtime::operators::window::store::backend::version::{Attempt, CutHistory, Version};
use crate::runtime::state::OperatorTaskState;

use super::cql::{unlogged_batch, PreparedGc};
use super::meta;
use super::schema::{align_down, RAW_BUCKET_MS};
use super::store::ScyllaWindowStoreClient;

pub(super) fn keep_slots(
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

struct Retention {
    attempt: Attempt,
    cut: CutHistory,
    prev_cut: CutHistory,
}

async fn retention_for(client: &ScyllaWindowStoreClient, kg: i32) -> Result<Retention> {
    if client.scope.request_mode {
        if let Some(cuts) = meta::load_cuts(client, kg).await? {
            return Ok(Retention {
                attempt: cuts.cur_attempt,
                cut: cuts.cut,
                prev_cut: cuts.prev_cut,
            });
        }
    }
    Ok(Retention {
        attempt: client.my_attempt(),
        cut: client.cp_cut_for(kg),
        prev_cut: client.prev_cut_for(kg),
    })
}

pub(super) async fn maintain(
    client: &ScyllaWindowStoreClient,
    ns: &crate::runtime::operators::window::model::StateNamespace,
    state: &dyn OperatorTaskState,
) -> Result<()> {
    let Some(wo) = state
        .as_any()
        .downcast_ref::<crate::runtime::operators::window::state::WindowOperatorState>(
    ) else {
        return Ok(());
    };
    let Some((_committed_wm, floor)) = wo.committed_retention_cutoff() else {
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
            let result = session
                .execute_unpaged(&stmt, (ns, g as i32))
                .await?;
            Ok::<_, anyhow::Error>((g as i32, result))
        }
    });

    let mut expired = Vec::new();
    let mut live: HashMap<(i32, Vec<u8>), Vec<i64>> = HashMap::new();
    let mut seen_kg: HashMap<i32, bool> = HashMap::new();
    for (kg, result) in try_join_all(scans).await? {
        let rows = result.into_rows_result()?;
        let mut any = false;
        for row in rows.rows::<(i64, Vec<u8>)>()? {
            let (bucket_start, business_key) = row?;
            any = true;
            if bucket_start < floor_bucket {
                expired.push((kg, business_key, bucket_start));
            } else {
                live.entry((kg, business_key)).or_default().push(bucket_start);
            }
        }
        seen_kg.insert(kg, any);
    }

    let mut expired_futs = Vec::new();
    for (kg, key, bucket) in expired {
        let session = Arc::clone(&session);
        let gc = gc.clone();
        let ns = ns.bytes.clone();
        let granularities = granularities.clone();
        expired_futs.push(async move {
            drop_expired_bucket(session, gc, ns, kg, key, bucket, granularities).await
        });
    }
    try_join_all(expired_futs).await?;

    let mut version_futs = Vec::new();
    for ((kg, key), buckets) in live {
        let session = Arc::clone(&session);
        let gc = gc.clone();
        let ns = ns.bytes.clone();
        let granularities = granularities.clone();
        let slots = retention_for(client, kg).await?;
        version_futs.push(async move {
            gc_versions(
                session,
                gc,
                ns,
                kg,
                key,
                buckets,
                granularities,
                slots,
            )
            .await
        });
    }
    try_join_all(version_futs).await?;

    if client.scope.request_mode {
        for (kg, had_rows) in seen_kg {
            if had_rows {
                continue;
            }
            session
                .execute_unpaged(&gc.delete_meta, (ns.bytes.clone(), kg))
                .await?;
        }
    }
    Ok(())
}

async fn drop_expired_bucket(
    session: Arc<Session>,
    gc: PreparedGc,
    ns: Vec<u8>,
    kg: i32,
    key: Vec<u8>,
    bucket: i64,
    granularities: Vec<i64>,
) -> Result<()> {
    session
        .execute_unpaged(
            &gc.delete_raw,
            (ns.clone(), kg, key.clone(), bucket),
        )
        .await?;
    let tile_futs = granularities.into_iter().map(|gran| {
        let session = Arc::clone(&session);
        let stmt = gc.delete_tiles.clone();
        let ns = ns.clone();
        let key = key.clone();
        async move {
            session
                .execute_unpaged(&stmt, (ns, kg, key, gran, bucket))
                .await?;
            Ok::<_, anyhow::Error>(())
        }
    });
    try_join_all(tile_futs).await?;
    session
        .execute_unpaged(&gc.delete_kg_buckets, (ns, kg, bucket, key))
        .await?;
    Ok(())
}

async fn gc_versions(
    session: Arc<Session>,
    gc: PreparedGc,
    ns: Vec<u8>,
    kg: i32,
    key: Vec<u8>,
    buckets: Vec<i64>,
    granularities: Vec<i64>,
    slots: Retention,
) -> Result<()> {
    let key_rows = session
        .execute_unpaged(
            &gc.select_key_state_versions,
            (ns.clone(), kg, key.clone()),
        )
        .await?;
    let mut versions = Vec::new();
    for row in key_rows.into_rows_result()?.rows::<(i64, i64)>()? {
        let (attempt, epoch) = row?;
        versions.push(Version {
            attempt: attempt as Attempt,
            epoch: epoch as u64,
        });
    }
    let keep = keep_slots(versions.iter().copied(), slots.attempt, &slots.cut, &slots.prev_cut);
    let mut deletes = Vec::new();
    for v in versions {
        if !keep.contains(&v) {
            deletes.push((ns.clone(), kg, key.clone(), v.attempt as i64, v.epoch as i64));
        }
    }
    unlogged_batch(&session, &gc.delete_key_state_version, deletes).await?;

    for bucket in buckets {
        let raw = session
            .execute_unpaged(
                &gc.select_raw_versions,
                (ns.clone(), kg, key.clone(), bucket),
            )
            .await?;
        let mut by_cell: HashMap<(i64, i64), Vec<Version>> = HashMap::new();
        for row in raw.into_rows_result()?.rows::<(i64, i64, i64, i64)>()? {
            let (event_ts, seq_no, attempt, epoch) = row?;
            by_cell.entry((event_ts, seq_no)).or_default().push(Version {
                attempt: attempt as Attempt,
                epoch: epoch as u64,
            });
        }
        let mut raw_deletes = Vec::new();
        for ((event_ts, seq_no), cell) in by_cell {
            let keep = keep_slots(cell.iter().copied(), slots.attempt, &slots.cut, &slots.prev_cut);
            for v in cell {
                if !keep.contains(&v) {
                    raw_deletes.push((
                        ns.clone(),
                        kg,
                        key.clone(),
                        bucket,
                        event_ts,
                        seq_no,
                        v.attempt as i64,
                        v.epoch as i64,
                    ));
                }
            }
        }
        unlogged_batch(&session, &gc.delete_raw_version, raw_deletes).await?;

        for gran in &granularities {
            let tiles = session
                .execute_unpaged(
                    &gc.select_tile_versions,
                    (ns.clone(), kg, key.clone(), *gran, bucket),
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
                let keep =
                    keep_slots(cell.iter().copied(), slots.attempt, &slots.cut, &slots.prev_cut);
                for v in cell {
                    if !keep.contains(&v) {
                        tile_deletes.push((
                            ns.clone(),
                            kg,
                            key.clone(),
                            *gran,
                            bucket,
                            tile_start,
                            v.attempt as i64,
                            v.epoch as i64,
                        ));
                    }
                }
            }
            unlogged_batch(&session, &gc.delete_tile_version, tile_deletes).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keep_slots_holds_mine_cut_and_prev() {
        let cut = CutHistory::empty().advance(1, Some(4));
        let prev = CutHistory::empty().advance(1, Some(2));
        let versions = [
            Version { attempt: 1, epoch: 10 },
            Version { attempt: 1, epoch: 4 },
            Version { attempt: 1, epoch: 2 },
            Version { attempt: 1, epoch: 0 },
            Version { attempt: 2, epoch: 0 },
        ];
        let keep = keep_slots(versions, 2, &cut, &prev);
        assert!(keep.contains(&Version { attempt: 2, epoch: 0 }));
        assert!(keep.contains(&Version { attempt: 1, epoch: 4 }));
        assert!(keep.contains(&Version { attempt: 1, epoch: 2 }));
        assert!(!keep.contains(&Version { attempt: 1, epoch: 10 }));
        assert!(!keep.contains(&Version { attempt: 1, epoch: 0 }));
    }
}
