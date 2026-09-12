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

/// Write versioned raw / tiles / key_state / triggers. One UNLOGGED BATCH
/// per Scylla partition (prepared statements — unprepared values in a batch
/// would prepare sequentially). Independent partitions are joined.
///
/// Serving (`window_head`) is not published here. WO reads use the writer
/// overlay (same attempt). Steal / promote lands in the checkpoint PR.
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
    Ok(())
}

pub(super) async fn store_key_state(
    client: &ScyllaWindowStoreClient,
    partition: &PartitionKey,
    state: &KeyState,
) -> Result<()> {
    let kg = client.key_group(partition)?;
    let session = client.inner.session();
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
    .await
}
