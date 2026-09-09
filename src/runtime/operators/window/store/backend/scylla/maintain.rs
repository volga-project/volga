use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use futures::future::try_join_all;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;

use crate::runtime::operators::window::store::backend::StateVersion;
use crate::runtime::state::OperatorTaskState;

use super::cql::{unlogged_batch, PreparedDml};
use super::schema::{align_down, RAW_BUCKET_MS};
use super::store::ScyllaWindowStore;

struct HeadVersions {
    writer_attempt: Vec<u8>,
    writer_epoch: i64,
    serving_attempt: Vec<u8>,
    serving_epoch: i64,
}

/// Rows overlay would never return for this WO or a WRO pin on current head.
/// Keep the live job attempt (all epochs, including unpublished), head writer
/// and serving cuts, and the recovery-base cut. Timed serving-pin grace is
/// later: today serving tracks writer, and the base remains until dropped.
fn generation_keep(
    attempt: &[u8],
    epoch: i64,
    current_attempt: &[u8],
    head: Option<&HeadVersions>,
    base: Option<&StateVersion>,
) -> bool {
    if attempt == current_attempt {
        return true;
    }
    if let Some(head) = head {
        if attempt == head.serving_attempt.as_slice() && epoch <= head.serving_epoch {
            return true;
        }
        if attempt == head.writer_attempt.as_slice() && epoch <= head.writer_epoch {
            return true;
        }
    }
    if let Some(base) = base {
        return attempt == base.attempt.as_slice() && epoch <= base.epoch as i64;
    }
    false
}

fn recovery_base_for_kg(bases: &[(i32, i32, StateVersion)], kg: i32) -> Option<&StateVersion> {
    bases
        .iter()
        .find(|(start, end, _)| *start <= kg && kg < *end)
        .map(|(_, _, version)| version)
}

async fn load_recovery_bases(
    session: &Session,
    stmt: &PreparedStatement,
    ns: Vec<u8>,
    recovery_attempt: Vec<u8>,
) -> Result<Vec<(i32, i32, StateVersion)>> {
    let result = session
        .execute_unpaged(stmt, (ns, recovery_attempt))
        .await?;
    let rows = result.into_rows_result()?;
    let mut out = Vec::new();
    for row in rows.rows::<(i32, i32, Vec<u8>, i64)>()? {
        let (range_start, range_end, attempt, epoch) = row?;
        out.push((
            range_start,
            range_end,
            StateVersion {
                attempt,
                epoch: epoch.max(0) as u64,
            },
        ));
    }
    Ok(out)
}

async fn load_head_versions(
    session: &Session,
    stmt: &PreparedStatement,
    ns: Vec<u8>,
    kg: i32,
    key: Vec<u8>,
) -> Result<Option<HeadVersions>> {
    let result = session.execute_unpaged(stmt, (ns, kg, key)).await?;
    let rows = result.into_rows_result()?;
    let mut head = None;
    for row in rows.rows::<(Vec<u8>, i64, Vec<u8>, i64)>()? {
        let (writer_attempt, writer_epoch, serving_attempt, serving_epoch) = row?;
        head = Some(HeadVersions {
            writer_attempt,
            writer_epoch,
            serving_attempt,
            serving_epoch,
        });
    }
    Ok(head)
}

async fn gc_live_generations(
    session: Arc<Session>,
    prepared: PreparedDml,
    ns: Vec<u8>,
    kg: i32,
    key: Vec<u8>,
    buckets: Vec<i64>,
    granularities: Vec<i64>,
    current_attempt: Vec<u8>,
    base: Option<StateVersion>,
) -> Result<()> {
    let head = load_head_versions(
        &session,
        &prepared.select_head_versions,
        ns.clone(),
        kg,
        key.clone(),
    )
    .await?;
    let keep = |attempt: &[u8], epoch: i64| {
        generation_keep(
            attempt,
            epoch,
            &current_attempt,
            head.as_ref(),
            base.as_ref(),
        )
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
        if !keep(&attempt, epoch) {
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
            if !keep(&attempt, epoch) {
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
                if !keep(&attempt, epoch) {
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
    let Some((watermark, floor)) = wo.retention_cutoff() else {
        return Ok(());
    };
    let session = store.session();
    let prepared = store.prepared().await?;
    let key_group_range = wo.scope().key_group_range;
    let floor_bucket = align_down(floor, RAW_BUCKET_MS);
    let granularities = wo.tile_granularity_ms();
    let current_attempt = wo.scope().attempt.clone();
    let bases = load_recovery_bases(
        &session,
        &prepared.select_recovery_bases,
        ns.bytes.clone(),
        current_attempt.clone(),
    )
    .await?;
    // Later: batched CQL deletes; TWCS/TTL; Foyer for the kg_buckets index;
    // trigger range-delete; timed serving-pin grace (#291).
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
    let mut live: HashMap<(i32, Vec<u8>), Vec<i64>> = HashMap::new();
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
        let session = Arc::clone(&session);
        let prepared = prepared.clone();
        let ns = ns.bytes.clone();
        let granularities = granularities.clone();
        let current_attempt = current_attempt.clone();
        let base = recovery_base_for_kg(&bases, kg).cloned();
        gc_futs.push(async move {
            gc_live_generations(
                session,
                prepared,
                ns,
                kg,
                key,
                buckets,
                granularities,
                current_attempt,
                base,
            )
            .await
        });
    }
    try_join_all(expired_futs).await?;
    try_join_all(gc_futs).await?;
    let _ = watermark;
    Ok(())
}

#[cfg(test)]
mod generation_keep_tests {
    use super::*;

    fn head(writer: &[u8], we: i64, serving: &[u8], se: i64) -> HeadVersions {
        HeadVersions {
            writer_attempt: writer.to_vec(),
            writer_epoch: we,
            serving_attempt: serving.to_vec(),
            serving_epoch: se,
        }
    }

    #[test]
    fn keeps_current_attempt_any_epoch() {
        assert!(generation_keep(b"me", 99, b"me", None, None));
    }

    #[test]
    fn drops_unknown_attempt() {
        assert!(!generation_keep(b"dead", 1, b"me", None, None));
    }

    #[test]
    fn keeps_head_serving_at_or_below_cut() {
        let h = head(b"wr", 3, b"sv", 2);
        assert!(generation_keep(b"sv", 2, b"me", Some(&h), None));
        assert!(!generation_keep(b"sv", 3, b"me", Some(&h), None));
    }

    #[test]
    fn keeps_recovery_base_at_or_below_cut() {
        let base = StateVersion {
            attempt: b"old".to_vec(),
            epoch: 7,
        };
        assert!(generation_keep(b"old", 7, b"me", None, Some(&base)));
        assert!(!generation_keep(b"old", 8, b"me", None, Some(&base)));
    }
}
