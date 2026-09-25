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
use crate::runtime::operators::window::store::backend::codec::{encode_batch, encode_val};
use crate::runtime::operators::window::store::data::cursors_from_batch;

use super::cql::unlogged_batch;
use super::observe::{note_bytes, note_rows, note_stmt};
use super::schema::{align_down, kg_shard, RAW_BUCKET_MS};
use super::store::ScyllaWindowStoreClient;

pub(super) async fn insert_key_state(
    session: &Session,
    stmt: &PreparedStatement,
    ns: Vec<u8>,
    kg: i32,
    key: Vec<u8>,
    attempt: i64,
    epoch: i64,
    state: &KeyState,
) -> Result<()> {
    note_stmt();
    note_rows(1);
    session
        .execute_unpaged(stmt, (ns, kg, key, attempt, epoch, encode_val(state)?))
        .await?;
    Ok(())
}

/// Write versioned raw / tiles / key_state / triggers. One UNLOGGED BATCH
/// per Scylla partition. No ingest LWT. Driver retries idempotent statements.
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
    client.begin_key(&partition.business_key)?;
    let epoch = client.alloc_epoch(kg);
    let result = commit_events_at(
        client,
        partition,
        kg,
        epoch,
        ts_column_index,
        events,
        tiles,
        meta,
        triggers,
    )
    .await;
    client.end_key(&partition.business_key);
    match result {
        Ok(()) => {
            client.ack_epoch(kg, epoch);
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn commit_events_at(
    client: &ScyllaWindowStoreClient,
    partition: &PartitionKey,
    kg: i32,
    epoch: u64,
    ts_column_index: usize,
    events: &RecordBatch,
    tiles: &TileMap,
    meta: &KeyState,
    triggers: &[WindowTrigger],
) -> Result<()> {
    let session = client.inner.session();
    let prepared = client.inner.prepared().await?;
    let ns = client.scope.namespace.bytes.clone();
    let key = partition.business_key.clone();
    let attempt = client.my_attempt() as i64;
    let epoch = epoch as i64;

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
                attempt,
                epoch,
                {
                    let payload = encode_batch(&events.slice(*idx, 1))?;
                    note_rows(1);
                    note_bytes(payload.len() as u64);
                    payload
                },
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

    let mut tiles_by_gran: BTreeMap<i64, Vec<(i64, Vec<u8>)>> = BTreeMap::new();
    for ((granularity, tile_start), value) in tiles {
        tiles_by_gran
            .entry(granularity.to_millis())
            .or_default()
            .push((*tile_start, encode_val(value)?));
    }
    let mut tile_futs = Vec::new();
    for (gran, part) in tiles_by_gran {
        let mut values = Vec::with_capacity(part.len());
        for (tile_start, payload) in part {
            values.push((
                ns.clone(),
                kg,
                key.clone(),
                gran,
                tile_start,
                attempt,
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
        attempt,
        epoch,
        meta,
    );

    let mut triggers_by_shard: BTreeMap<i32, Vec<(i64, u64, i8, i64)>> = BTreeMap::new();
    for trigger in triggers {
        let shard = kg_shard(kg as usize, client.scope.max_parallelism);
        let (kind, window_id) = match trigger.kind {
            WindowTriggerKind::RowEmit => (0i8, 0i64),
            WindowTriggerKind::WindowEnd { window_id } => (1i8, window_id as i64),
        };
        triggers_by_shard.entry(shard).or_default().push((
            trigger.fire_at.ts,
            trigger.fire_at.seq_no,
            kind,
            window_id,
        ));
    }
    let mut trigger_futs = Vec::new();
    for (shard, part) in triggers_by_shard {
        let mut values = Vec::with_capacity(part.len());
        for (ts, seq, kind, window_id) in part {
            values.push((
                ns.clone(),
                shard,
                ts,
                seq as i64,
                key.clone(),
                kind,
                window_id,
                kg,
                attempt,
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
    Ok(())
}

pub(super) async fn store_key_state(
    client: &ScyllaWindowStoreClient,
    partition: &PartitionKey,
    state: &KeyState,
) -> Result<()> {
    let kg = client.key_group(partition)?;
    client.begin_key(&partition.business_key)?;
    let epoch = client.alloc_epoch(kg);
    let session = client.inner.session();
    let prepared = client.inner.prepared().await?;
    let result = insert_key_state(
        &session,
        &prepared.insert_key_states,
        client.scope.namespace.bytes.clone(),
        kg,
        partition.business_key.clone(),
        client.my_attempt() as i64,
        epoch as i64,
        state,
    )
    .await;
    client.end_key(&partition.business_key);
    match result {
        Ok(()) => {
            client.ack_epoch(kg, epoch);
            Ok(())
        }
        Err(e) => Err(e),
    }
}
