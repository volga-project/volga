use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use futures::future::try_join_all;

use crate::runtime::operators::window::model::{
    Cursor, KeyState, PartitionKey, RawRun, TileMap, TileRun,
};
use crate::runtime::operators::window::store::backend::codec::{decode_batch, decode_val};

use super::schema::{align_down, RAW_BUCKET_MS};
use super::store::ScyllaWindowStoreClient;
use super::write;

pub(super) async fn load_key_state(
    client: &ScyllaWindowStoreClient,
    partition: &PartitionKey,
) -> Result<KeyState> {
    let kg = client.key_group(partition)?;
    let session = client.inner.session();
    let prepared = client.inner.prepared().await?;
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
    let rows = result.into_rows_result()?;
    let mut best: Option<(Vec<u8>, i64, KeyState)> = None;
    for row in rows.rows::<(Vec<u8>, i64, Vec<u8>)>()? {
        let (attempt, epoch, payload) = row?;
        if !client.overlay_visible(&attempt, epoch).await {
            continue;
        }
        if best.as_ref().map_or(true, |(_, e, _)| epoch > *e) {
            best = Some((attempt, epoch, decode_val(&payload)?));
        }
    }
    write::steal_owner(client, session.as_ref(), partition, kg).await?;
    Ok(best.map(|(_, _, s)| s).unwrap_or_default())
}

pub(super) async fn load_raw(
    client: &ScyllaWindowStoreClient,
    partition: &PartitionKey,
    runs: &[RawRun],
) -> Result<Vec<RecordBatch>> {
    let kg = client.key_group(partition)?;
    let session = client.inner.session();
    let prepared = client.inner.prepared().await?;
    let mut pages = Vec::new();
    for run in runs {
        let mut bucket = align_down(run.from.ts, RAW_BUCKET_MS);
        let end_bucket = align_down(run.to.ts.saturating_sub(1).max(run.from.ts), RAW_BUCKET_MS);
        while bucket <= end_bucket {
            let session = Arc::clone(&session);
            let select_raw = prepared.select_raw.clone();
            let ns = client.scope.namespace.bytes.clone();
            let key = partition.business_key.clone();
            let from = run.from;
            let to = run.to;
            pages.push(async move {
                let result = session
                    .execute_unpaged(&select_raw, (ns, kg, key, bucket, from.ts, to.ts))
                    .await?;
                Ok::<_, anyhow::Error>((from, to, result))
            });
            bucket += RAW_BUCKET_MS;
        }
    }
    let mut by_cursor: BTreeMap<Cursor, RecordBatch> = BTreeMap::new();
    for (from, to, result) in try_join_all(pages).await? {
        let rows = result.into_rows_result()?;
        for row in rows.rows::<(i64, i64, Vec<u8>, i64, Vec<u8>)>()? {
            let (ts, seq, attempt, epoch, payload) = row?;
            let cursor = Cursor::new(ts, seq as u64);
            if cursor < from || cursor >= to {
                continue;
            }
            if !client.overlay_visible(&attempt, epoch).await {
                continue;
            }
            by_cursor.insert(cursor, decode_batch(&payload)?);
        }
    }
    Ok(by_cursor.into_values().collect())
}

pub(super) async fn load_tiles(
    client: &ScyllaWindowStoreClient,
    partition: &PartitionKey,
    runs: &[TileRun],
) -> Result<TileMap> {
    let kg = client.key_group(partition)?;
    let session = client.inner.session();
    let prepared = client.inner.prepared().await?;
    let pages = runs.iter().map(|run| {
        let gran = run.granularity.to_millis();
        let granularity = run.granularity;
        let bucket = align_down(run.start_ts, RAW_BUCKET_MS);
        let start_ts = run.start_ts;
        let end_ts = run.end_ts_exclusive;
        let session = Arc::clone(&session);
        let select_tiles = prepared.select_tiles.clone();
        let ns = client.scope.namespace.bytes.clone();
        let key = partition.business_key.clone();
        async move {
            let result = session
                .execute_unpaged(
                    &select_tiles,
                    (ns, kg, key, gran, bucket, start_ts, end_ts),
                )
                .await?;
            Ok::<_, anyhow::Error>((granularity, result))
        }
    });
    let mut out = TileMap::new();
    for (granularity, result) in try_join_all(pages).await? {
        let rows = result.into_rows_result()?;
        let mut best: BTreeMap<i64, (i64, Vec<u8>)> = BTreeMap::new();
        for row in rows.rows::<(i64, Vec<u8>, i64, Vec<u8>)>()? {
            let (tile_start, attempt, epoch, payload) = row?;
            if !client.overlay_visible(&attempt, epoch).await {
                continue;
            }
            if best.get(&tile_start).map_or(true, |(e, _)| epoch >= *e) {
                best.insert(tile_start, (epoch, payload));
            }
        }
        for (tile_start, (_, payload)) in best {
            let tiles: crate::runtime::operators::window::model::WindowTiles = decode_val(&payload)?;
            out.insert((granularity, tile_start), tiles);
        }
    }
    Ok(out)
}
