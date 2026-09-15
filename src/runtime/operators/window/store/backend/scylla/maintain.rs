use std::sync::Arc;

use anyhow::Result;
use futures::future::try_join_all;
use scylla::client::session::Session;

use crate::runtime::state::OperatorTaskState;

use super::cql::{unlogged_batch, PreparedDml};
use super::schema::{align_down, RAW_BUCKET_MS};
use super::store::ScyllaWindowStore;

/// Displaced extras after steal: `prev.attempt ∧ E > prev.E`.
/// Keep unpublished current-writer rows and `E ≤ serving_E` holes.
pub(super) fn drop_prev_extra(
    attempt: &[u8],
    epoch: i64,
    prev_attempt: &[u8],
    prev_epoch: i64,
) -> bool {
    !prev_attempt.is_empty() && attempt == prev_attempt && epoch > prev_epoch
}

async fn gc_prev_extras(
    session: Arc<Session>,
    prepared: PreparedDml,
    ns: Vec<u8>,
    kg: i32,
    key: Vec<u8>,
    buckets: Vec<i64>,
    granularities: Vec<i64>,
    prev_attempt: Vec<u8>,
    prev_epoch: i64,
) -> Result<()> {
    let drop = |attempt: &[u8], epoch: i64| {
        drop_prev_extra(attempt, epoch, &prev_attempt, prev_epoch)
    };

    let key_states = session
        .execute_unpaged(
            &prepared.select_key_state_versions,
            (ns.clone(), kg, key.clone()),
        )
        .await?;
    let mut key_state_deletes = Vec::new();
    for row in key_states.into_rows_result()?.rows::<(Vec<u8>, i64)>()? {
        let (attempt, epoch) = row?;
        if drop(&attempt, epoch) {
            key_state_deletes.push((ns.clone(), kg, key.clone(), attempt, epoch));
        }
    }
    unlogged_batch(
        &session,
        &prepared.delete_key_state_version,
        key_state_deletes,
    )
    .await?;

    for bucket in buckets {
        let raw = session
            .execute_unpaged(
                &prepared.select_raw_versions,
                (ns.clone(), kg, key.clone(), bucket),
            )
            .await?;
        let mut raw_deletes = Vec::new();
        for row in raw.into_rows_result()?.rows::<(i64, i64, Vec<u8>, i64)>()? {
            let (event_ts, seq_no, attempt, epoch) = row?;
            if drop(&attempt, epoch) {
                raw_deletes.push((
                    ns.clone(),
                    kg,
                    key.clone(),
                    bucket,
                    event_ts,
                    seq_no,
                    attempt,
                    epoch,
                ));
            }
        }
        unlogged_batch(&session, &prepared.delete_raw_version, raw_deletes).await?;

        for gran in &granularities {
            let tiles = session
                .execute_unpaged(
                    &prepared.select_tile_versions,
                    (ns.clone(), kg, key.clone(), *gran, bucket),
                )
                .await?;
            let mut tile_deletes = Vec::new();
            for row in tiles.into_rows_result()?.rows::<(i64, Vec<u8>, i64)>()? {
                let (tile_start, attempt, epoch) = row?;
                if drop(&attempt, epoch) {
                    tile_deletes.push((
                        ns.clone(),
                        kg,
                        key.clone(),
                        *gran,
                        bucket,
                        tile_start,
                        attempt,
                        epoch,
                    ));
                }
            }
            unlogged_batch(&session, &prepared.delete_tile_version, tile_deletes).await?;
        }
    }
    Ok(())
}

pub(super) async fn maintain(
    store: &ScyllaWindowStore,
    ns: &crate::runtime::operators::window::model::StateNamespace,
    state: &dyn OperatorTaskState,
) -> Result<()> {
    let Some(wo) = state
        .as_any()
        .downcast_ref::<crate::runtime::operators::window::state::WindowOperatorState>()
    else {
        return Ok(());
    };
    let Some((_watermark, floor)) = wo.retention_cutoff() else {
        return Ok(());
    };
    let session = store.session();
    let prepared = store.prepared().await?;
    let key_group_range = wo.scope().key_group_range;
    let floor_bucket = align_down(floor, RAW_BUCKET_MS);
    let granularities = wo.tile_granularity_ms();
    let client = store.client(wo.scope().clone());

    let scans = (key_group_range.start..key_group_range.end).map(|kg| {
        let session = Arc::clone(&session);
        let select_kg_buckets = prepared.select_kg_buckets.clone();
        let ns = ns.bytes.clone();
        async move {
            let result = session
                .execute_unpaged(&select_kg_buckets, (ns, kg as i32))
                .await?;
            Ok::<_, anyhow::Error>((kg, result))
        }
    });
    let mut expired_futs = Vec::new();
    let mut live: std::collections::HashMap<(i32, Vec<u8>), Vec<i64>> =
        std::collections::HashMap::new();
    for (kg, result) in try_join_all(scans).await? {
        let rows = result.into_rows_result()?;
        for row in rows.rows::<(i64, Vec<u8>)>()? {
            let (bucket_start, business_key) = row?;
            if bucket_start < floor_bucket {
                let session = Arc::clone(&session);
                let delete_raw = prepared.delete_raw.clone();
                let delete_tiles = prepared.delete_tiles.clone();
                let delete_kg_buckets = prepared.delete_kg_buckets.clone();
                let ns = ns.bytes.clone();
                let granularities = granularities.clone();
                expired_futs.push(async move {
                    let tile_futs = granularities.into_iter().map(|gran| {
                        let session = Arc::clone(&session);
                        let delete_tiles = delete_tiles.clone();
                        let ns = ns.clone();
                        let key = business_key.clone();
                        async move {
                            session
                                .execute_unpaged(
                                    &delete_tiles,
                                    (ns, kg as i32, key, gran, bucket_start),
                                )
                                .await?;
                            Ok::<_, anyhow::Error>(())
                        }
                    });
                    let raw = {
                        let session = Arc::clone(&session);
                        let ns = ns.clone();
                        let key = business_key.clone();
                        async move {
                            session
                                .execute_unpaged(&delete_raw, (ns, kg as i32, key, bucket_start))
                                .await?;
                            Ok::<_, anyhow::Error>(())
                        }
                    };
                    tokio::try_join!(raw, try_join_all(tile_futs))?;
                    session
                        .execute_unpaged(
                            &delete_kg_buckets,
                            (ns, kg as i32, bucket_start, business_key),
                        )
                        .await?;
                    Ok::<_, anyhow::Error>(())
                });
            } else {
                live.entry((kg as i32, business_key))
                    .or_default()
                    .push(bucket_start);
            }
        }
    }

    let mut gc_futs = Vec::new();
    for ((kg, key), buckets) in live {
        let Some(lease) = super::lease::load_lease(&client, kg).await? else {
            continue;
        };
        if lease.prev_attempt.is_empty() {
            continue;
        }
        let session = Arc::clone(&session);
        let prepared = prepared.clone();
        let ns = ns.bytes.clone();
        let granularities = granularities.clone();
        gc_futs.push(async move {
            gc_prev_extras(
                session,
                prepared,
                ns,
                kg,
                key,
                buckets,
                granularities,
                lease.prev_attempt,
                lease.prev_epoch,
            )
            .await
        });
    }
    try_join_all(expired_futs).await?;
    try_join_all(gc_futs).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prev_extras_are_displaced_tail_only() {
        assert!(drop_prev_extra(b"a", 11, b"a", 10));
        assert!(!drop_prev_extra(b"a", 10, b"a", 10));
        assert!(!drop_prev_extra(b"b", 11, b"a", 10));
        assert!(!drop_prev_extra(b"a", 11, b"", 10));
    }
}
