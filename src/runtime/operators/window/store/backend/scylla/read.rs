use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use futures::future::try_join_all;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;

use crate::runtime::operators::window::model::{
    Cursor, KeyState, PartitionKey, RawRun, TileMap, TileRun,
};
use crate::runtime::operators::window::store::backend::codec::{decode_batch, decode_val};

use crate::runtime::operators::window::store::backend::{Attempt, Version};

use super::observe::{note_bytes, note_rows, note_stmt};
use super::schema::{last_included_ts, time_buckets, RAW_BUCKET_MS};
use super::store::ScyllaWindowStoreClient;

fn row_version(attempt: i64, epoch: i64) -> Version {
    Version {
        attempt: attempt as Attempt,
        epoch: epoch as u64,
    }
}

pub(super) async fn load_key_state(
    client: &ScyllaWindowStoreClient,
    partition: &PartitionKey,
) -> Result<KeyState> {
    let kg = client.key_group(partition)?;
    let session = client.inner.session();
    let prepared = client.inner.prepared().await?;
    note_stmt();
    let result = session
        .execute_unpaged(
            &prepared.select_key_state,
            (
                client.scope.namespace.bytes.clone(),
                kg,
                partition.business_key.clone(),
            ),
        )
        .await?;
    // Clustering is (attempt DESC, epoch DESC); first visible row wins.
    for row in result.into_rows_result()?.rows::<(i64, i64, Vec<u8>)>()? {
        let (attempt, epoch, payload) = row?;
        note_rows(1);
        if client.overlay_visible(kg, attempt, epoch) {
            return decode_val(&payload);
        }
    }
    Ok(KeyState::default())
}

pub(super) async fn load_raw(
    client: &ScyllaWindowStoreClient,
    partition: &PartitionKey,
    runs: &[RawRun],
) -> Result<Vec<RecordBatch>> {
    let kg = client.key_group(partition)?;
    let prepared = client.inner.prepared().await?;
    fetch_raw(
        client.inner.session(),
        prepared.select_raw.clone(),
        client.scope.namespace.bytes.clone(),
        kg,
        partition.business_key.clone(),
        runs,
        |v, _| client.overlay_visible(kg, v.attempt as i64, v.epoch as i64),
    )
    .await
}

pub(super) async fn fetch_raw(
    session: Arc<Session>,
    select_raw: PreparedStatement,
    ns: Vec<u8>,
    kg: i32,
    key: Vec<u8>,
    runs: &[RawRun],
    is_visible: impl Fn(Version, i64) -> bool + Send + Sync,
) -> Result<Vec<RecordBatch>> {
    let mut pages = Vec::new();
    for run in runs {
        for bucket in time_buckets(run.from.ts, last_included_ts(run.to), RAW_BUCKET_MS) {
            let session = Arc::clone(&session);
            let select_raw = select_raw.clone();
            let ns = ns.clone();
            let key = key.clone();
            let from = run.from;
            let to = run.to;
            pages.push(async move {
                note_stmt();
                let result = session
                    .execute_unpaged(&select_raw, (ns, kg, key, bucket, from.ts, to.ts))
                    .await?;
                Ok::<_, anyhow::Error>((from, to, result))
            });
        }
    }
    let mut by_cursor: BTreeMap<Cursor, (Version, RecordBatch)> = BTreeMap::new();
    for (from, to, result) in try_join_all(pages).await? {
        let rows = result.into_rows_result()?;
        for row in rows.rows::<(i64, i64, i64, i64, Vec<u8>)>()? {
            let (ts, seq, attempt, epoch, payload) = row?;
            note_rows(1);
            note_bytes(payload.len() as u64);
            let cursor = Cursor::new(ts, seq as u64);
            if cursor < from || cursor >= to {
                continue;
            }
            let v = row_version(attempt, epoch);
            if !is_visible(v, ts) {
                continue;
            }
            match by_cursor.get(&cursor) {
                Some((best, _)) if *best >= v => {}
                _ => {
                    by_cursor.insert(cursor, (v, decode_batch(&payload)?));
                }
            }
        }
    }
    Ok(by_cursor.into_values().map(|(_, b)| b).collect())
}

pub(super) async fn load_tiles(
    client: &ScyllaWindowStoreClient,
    partition: &PartitionKey,
    runs: &[TileRun],
) -> Result<TileMap> {
    let kg = client.key_group(partition)?;
    let prepared = client.inner.prepared().await?;
    fetch_tiles(
        client.inner.session(),
        prepared.select_tiles.clone(),
        client.scope.namespace.bytes.clone(),
        kg,
        partition.business_key.clone(),
        runs,
        |v| client.overlay_visible(kg, v.attempt as i64, v.epoch as i64),
    )
    .await
}

pub(super) async fn fetch_tiles(
    session: Arc<Session>,
    select_tiles: PreparedStatement,
    ns: Vec<u8>,
    kg: i32,
    key: Vec<u8>,
    runs: &[TileRun],
    is_visible: impl Fn(Version) -> bool + Send + Sync,
) -> Result<TileMap> {
    let mut pages = Vec::new();
    for run in runs {
        let gran = run.granularity.to_millis();
        let granularity = run.granularity;
        let start_ts = run.start_ts;
        let end_ts = run.end_ts_exclusive;
        let session = Arc::clone(&session);
        let select_tiles = select_tiles.clone();
        let ns = ns.clone();
        let key = key.clone();
        pages.push(async move {
            note_stmt();
            let result = session
                .execute_unpaged(&select_tiles, (ns, kg, key, gran, start_ts, end_ts))
                .await?;
            Ok::<_, anyhow::Error>((granularity, result))
        });
    }
    let mut out = TileMap::new();
    for (granularity, result) in try_join_all(pages).await? {
        let rows = result.into_rows_result()?;
        let mut best: BTreeMap<i64, (Version, Vec<u8>)> = BTreeMap::new();
        for row in rows.rows::<(i64, i64, i64, Vec<u8>)>()? {
            let (tile_start, attempt, epoch, payload) = row?;
            note_rows(1);
            note_bytes(payload.len() as u64);
            let v = row_version(attempt, epoch);
            if !is_visible(v) {
                continue;
            }
            if best.get(&tile_start).map_or(true, |(e, _)| v > *e) {
                best.insert(tile_start, (v, payload));
            }
        }
        for (tile_start, (_, payload)) in best {
            let tiles: crate::runtime::operators::window::model::WindowTiles =
                decode_val(&payload)?;
            out.insert((granularity, tile_start), tiles);
        }
    }
    Ok(out)
}
