use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use futures::future::try_join_all;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;

use crate::runtime::operators::window::model::{
    Cursor, KeyState, PartitionKey, TileMap, WindowTrigger, WindowTriggerKind,
};
use crate::runtime::operators::window::store::backend::codec::{
    decode_val, encode_batch, encode_val,
};
use crate::runtime::operators::window::store::data::cursors_from_batch;

use super::cql::{lwt_applied, unlogged_batch, encode_owner_writer, HeadClaim};
use super::schema::{align_down, kg_shard, RAW_BUCKET_MS, TRIGGER_BUCKET_MS};
use super::store::ScyllaWindowStoreClient;

pub(super) async fn insert_key_state(
    session: &Session,
    stmt: &PreparedStatement,
    ns: Vec<u8>,
    kg: i32,
    key: Vec<u8>,
    attempt: Vec<u8>,
    epoch: i64,
    state: &KeyState,
) -> Result<()> {
    session
        .execute_unpaged(stmt, (ns, kg, key, attempt, epoch, encode_val(state)?))
        .await?;
    Ok(())
}

/// Steal `owner_writer` without moving `serving_*`. Fence only.
/// CAS failure stops the WO; do not try a second LWT.
pub(super) async fn steal_owner(
    client: &ScyllaWindowStoreClient,
    session: &Session,
    partition: &PartitionKey,
    kg: i32,
) -> Result<()> {
    if client.head_claim(&partition.business_key).await != HeadClaim::Steal {
        return Ok(());
    }
    let prepared = client.inner.prepared().await?;
    let previous = {
        let base = client.restore_base.lock().await;
        let Some(base) = base.as_ref() else {
            anyhow::bail!("steal head without restore_base");
        };
        encode_owner_writer(&base.attempt, &client.scope.writer_id.0)
    };
    let result = session
        .execute_unpaged(
            &prepared.steal_head_if_owner,
            (
                client.owner_writer(),
                client.scope.namespace.bytes.clone(),
                kg,
                partition.business_key.clone(),
                previous,
            ),
        )
        .await?;
    if !lwt_applied(result)? {
        anyhow::bail!("window_head is owned by another writer; refusing to steal");
    }
    client
        .head_claims
        .insert(partition.business_key.clone(), HeadClaim::Fenced);
    Ok(())
}

fn serving_caught_up(writer: &KeyState, serving: &KeyState) -> bool {
    if writer.next_seq < serving.next_seq {
        return false;
    }
    match (&writer.evaluation, &serving.evaluation) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(w), Some(s)) => w.through >= s.through,
    }
}

async fn serving_key_state(
    client: &ScyllaWindowStoreClient,
    session: &Session,
    partition: &PartitionKey,
    kg: i32,
) -> Result<Option<KeyState>> {
    let prepared = client.inner.prepared().await?;
    let head = session
        .execute_unpaged(
            &prepared.select_head,
            (
                client.scope.namespace.bytes.clone(),
                kg,
                partition.business_key.clone(),
            ),
        )
        .await?;
    let rows = head.into_rows_result()?;
    let mut pin = None;
    for row in rows.rows::<(Vec<u8>, i64)>()? {
        pin = Some(row?);
    }
    let Some((serving_attempt, serving_epoch)) = pin else {
        return Ok(None);
    };
    let states = session
        .execute_unpaged(
            &prepared.select_key_state,
            (
                client.scope.namespace.bytes.clone(),
                kg,
                partition.business_key.clone(),
            ),
        )
        .await?;
    let rows = states.into_rows_result()?;
    for row in rows.rows::<(Vec<u8>, i64, Vec<u8>)>()? {
        let (attempt, epoch, payload) = row?;
        if attempt == serving_attempt && epoch == serving_epoch {
            return Ok(Some(decode_val(&payload)?));
        }
    }
    Ok(None)
}

async fn promote_serving(
    client: &ScyllaWindowStoreClient,
    session: &Session,
    partition: &PartitionKey,
    kg: i32,
    epoch: i64,
) -> Result<()> {
    let prepared = client.inner.prepared().await?;
    let me = client.owner_writer();
    let result = session
        .execute_unpaged(
            &prepared.promote_head_if_owner,
            (
                client.scope.attempt.clone(),
                epoch,
                client.scope.attempt.clone(),
                epoch,
                client.scope.namespace.bytes.clone(),
                kg,
                partition.business_key.clone(),
                me,
            ),
        )
        .await?;
    if !lwt_applied(result)? {
        anyhow::bail!("window_head is owned by another writer; refusing to publish serving");
    }
    client
        .head_claims
        .insert(partition.business_key.clone(), HeadClaim::Ours);
    Ok(())
}

async fn insert_head_empty(
    client: &ScyllaWindowStoreClient,
    session: &Session,
    partition: &PartitionKey,
    kg: i32,
    epoch: i64,
) -> Result<()> {
    let prepared = client.inner.prepared().await?;
    let me = client.owner_writer();
    let result = session
        .execute_unpaged(
            &prepared.insert_head_if_not_exists,
            (
                client.scope.namespace.bytes.clone(),
                kg,
                partition.business_key.clone(),
                me,
                client.scope.attempt.clone(),
                epoch,
                client.scope.attempt.clone(),
                epoch,
            ),
        )
        .await?;
    if !lwt_applied(result)? {
        anyhow::bail!("window_head is owned by another writer; refusing to publish serving");
    }
    client
        .head_claims
        .insert(partition.business_key.clone(), HeadClaim::Ours);
    Ok(())
}

/// After data is durable: first snapshot, or OnCommit promote once catch-up
/// allows it. Steal does not move serving.
///
/// Timeout is unknown Paxos. Do not `inc_epoch` and republish; fail the task.
pub(super) async fn publish_serving(
    client: &ScyllaWindowStoreClient,
    session: &Session,
    partition: &PartitionKey,
    kg: i32,
    epoch: i64,
    writer_state: &KeyState,
) -> Result<()> {
    let claim = client.head_claim(&partition.business_key).await;
    match claim {
        HeadClaim::Empty => insert_head_empty(client, session, partition, kg, epoch).await,
        HeadClaim::Steal | HeadClaim::Fenced => {
            if claim == HeadClaim::Steal {
                steal_owner(client, session, partition, kg).await?;
            }
            match serving_key_state(client, session, partition, kg).await? {
                Some(serving) if !serving_caught_up(writer_state, &serving) => Ok(()),
                _ => promote_serving(client, session, partition, kg, epoch).await,
            }
        }
        HeadClaim::Ours => promote_serving(client, session, partition, kg, epoch).await,
    }
}

#[cfg(test)]
mod serving_catch_up_tests {
    use super::*;
    use crate::runtime::operators::window::model::{Cursor, KeyEvaluationState};

    fn state(next_seq: u64, through: Option<(i64, u64)>) -> KeyState {
        KeyState {
            next_seq,
            evaluation: through.map(|(ts, seq_no)| KeyEvaluationState {
                through: Cursor::new(ts, seq_no),
                accumulators: Default::default(),
            }),
        }
    }

    #[test]
    fn seq_only_when_no_evaluation() {
        assert!(serving_caught_up(&state(2, None), &state(2, None)));
        assert!(!serving_caught_up(&state(1, None), &state(2, None)));
    }

    #[test]
    fn through_must_reach_serving() {
        let serving = state(5, Some((1_000, 4)));
        assert!(!serving_caught_up(&state(5, Some((1_000, 3))), &serving));
        assert!(serving_caught_up(&state(5, Some((1_000, 4))), &serving));
        assert!(serving_caught_up(&state(6, Some((1_001, 0))), &serving));
    }
}

/// Write data then publish serving if due. Data: one UNLOGGED BATCH per
/// Scylla partition. Independent partitions are joined. Owner steal (if
/// needed) runs before data; serving promote is a separate LWT after
/// (`OnCommit` once catch-up allows it).
///
/// `Err` after the driver gives up is unknown (write/LWT timeout). Do not
/// `inc_epoch` and republish; fail the task and restore. Same-epoch retry
/// is the only safe resend (idempotent INSERTs + the same LWT).
pub(super) async fn commit_events(
    client: &ScyllaWindowStoreClient,
    partition: &PartitionKey,
    ts_column_index: usize,
    events: &RecordBatch,
    tiles: &TileMap,
    meta: &KeyState,
    triggers: &[WindowTrigger],
) -> Result<()> {
    anyhow::ensure!(
        triggers.iter().all(|t| &t.partition == partition),
        "window trigger partition does not match committed partition"
    );
    let kg = client.key_group(partition)?;
    let session = client.inner.session();
    steal_owner(client, &session, partition, kg).await?;
    let prepared = client.inner.prepared().await?;
    let epoch = client.inc_epoch();
    let ns = client.scope.namespace.bytes.clone();
    let key = partition.business_key.clone();
    let attempt = client.scope.attempt.clone();

    let cursors = if events.num_rows() > 0 {
        cursors_from_batch(events, ts_column_index)?
    } else {
        Vec::new()
    };
    let mut by_bucket: BTreeMap<i64, Vec<(usize, Cursor)>> = BTreeMap::new();
    for (i, cursor) in cursors.iter().enumerate() {
        by_bucket
            .entry(align_down(cursor.ts, RAW_BUCKET_MS))
            .or_default()
            .push((i, *cursor));
    }
    let mut raw_futs = Vec::new();
    for (bucket, rows) in &by_bucket {
        let mut values = Vec::with_capacity(rows.len());
        for (idx, cursor) in rows {
            values.push((
                ns.clone(),
                kg,
                key.clone(),
                *bucket,
                cursor.ts,
                cursor.seq_no as i64,
                attempt.clone(),
                epoch,
                encode_batch(&events.slice(*idx, 1))?,
            ));
        }
        let session = Arc::clone(&session);
        let insert_raw = prepared.insert_raw.clone();
        let insert_kg_buckets = prepared.insert_kg_buckets.clone();
        let ns = ns.clone();
        let key = key.clone();
        let bucket = *bucket;
        raw_futs.push(async move {
            let index = async {
                session
                    .execute_unpaged(&insert_kg_buckets, (ns, kg, bucket, key))
                    .await?;
                Ok(())
            };
            tokio::try_join!(unlogged_batch(&session, &insert_raw, values), index)?;
            Ok(())
        });
    }

    let mut tiles_by_part: BTreeMap<(i64, i64), Vec<(i64, Vec<u8>)>> = BTreeMap::new();
    for ((granularity, tile_start), value) in tiles {
        let gran = granularity.to_millis();
        let bucket = align_down(*tile_start, RAW_BUCKET_MS);
        tiles_by_part
            .entry((gran, bucket))
            .or_default()
            .push((*tile_start, encode_val(value)?));
    }
    let mut tile_futs = Vec::new();
    for ((gran, bucket), part) in tiles_by_part {
        let mut values = Vec::with_capacity(part.len());
        for (tile_start, payload) in part {
            values.push((
                ns.clone(),
                kg,
                key.clone(),
                gran,
                bucket,
                tile_start,
                attempt.clone(),
                epoch,
                payload,
            ));
        }
        let session = Arc::clone(&session);
        let insert_tiles = prepared.insert_tiles.clone();
        tile_futs.push(async move { unlogged_batch(&session, &insert_tiles, values).await });
    }

    let key_state_fut = insert_key_state(
        &session,
        &prepared.insert_key_states,
        ns.clone(),
        kg,
        key.clone(),
        attempt.clone(),
        epoch,
        meta,
    );

    let mut triggers_by_part: BTreeMap<(i64, i32), Vec<(i64, u64, i8, i64)>> = BTreeMap::new();
    for trigger in triggers {
        let bucket = align_down(trigger.fire_at.ts, TRIGGER_BUCKET_MS);
        let shard = kg_shard(kg as usize, client.scope.max_parallelism);
        let (kind, window_id) = match trigger.kind {
            WindowTriggerKind::RowEmit => (0i8, 0i64),
            WindowTriggerKind::WindowEnd { window_id } => (1i8, window_id as i64),
        };
        triggers_by_part.entry((bucket, shard)).or_default().push((
            trigger.fire_at.ts,
            trigger.fire_at.seq_no,
            kind,
            window_id,
        ));
    }
    let mut trigger_futs = Vec::new();
    for ((bucket, shard), part) in triggers_by_part {
        let mut values = Vec::with_capacity(part.len());
        for (ts, seq, kind, window_id) in part {
            values.push((
                ns.clone(),
                bucket,
                shard,
                ts,
                seq as i64,
                key.clone(),
                kind,
                window_id,
                kg,
                attempt.clone(),
                epoch,
            ));
        }
        let session = Arc::clone(&session);
        let insert_triggers = prepared.insert_triggers.clone();
        trigger_futs.push(async move { unlogged_batch(&session, &insert_triggers, values).await });
    }

    tokio::try_join!(
        try_join_all(raw_futs),
        try_join_all(tile_futs),
        key_state_fut,
        try_join_all(trigger_futs),
    )?;
    publish_serving(client, &session, partition, kg, epoch, meta).await
}

pub(super) async fn store_key_state(
    client: &ScyllaWindowStoreClient,
    partition: &PartitionKey,
    state: &KeyState,
) -> Result<()> {
    let kg = client.key_group(partition)?;
    let session = client.inner.session();
    steal_owner(client, &session, partition, kg).await?;
    let prepared = client.inner.prepared().await?;
    let epoch = client.inc_epoch();
    insert_key_state(
        &session,
        &prepared.insert_key_states,
        client.scope.namespace.bytes.clone(),
        kg,
        partition.business_key.clone(),
        client.scope.attempt.clone(),
        epoch,
        state,
    )
    .await?;
    publish_serving(client, &session, partition, kg, epoch, state).await
}
