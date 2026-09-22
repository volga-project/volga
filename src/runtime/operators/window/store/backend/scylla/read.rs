use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use futures::future::try_join_all;

use crate::runtime::operators::window::model::{
    Cursor, KeyState, PartitionKey, RawRun, TileMap, TileRun,
};
use crate::runtime::operators::window::store::backend::codec::{decode_batch, decode_val};

use crate::runtime::operators::window::store::backend::{Attempt, Version};

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
    let session = client.inner.session();
    let prepared = client.inner.prepared().await?;
    let mut pages = Vec::new();
    for run in runs {
        for bucket in time_buckets(run.from.ts, last_included_ts(run.to), RAW_BUCKET_MS) {
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
        }
    }
    let mut by_cursor: BTreeMap<Cursor, (Version, RecordBatch)> = BTreeMap::new();
    for (from, to, result) in try_join_all(pages).await? {
        let rows = result.into_rows_result()?;
        for row in rows.rows::<(i64, i64, i64, i64, Vec<u8>)>()? {
            let (ts, seq, attempt, epoch, payload) = row?;
            let cursor = Cursor::new(ts, seq as u64);
            if cursor < from || cursor >= to {
                continue;
            }
            if !client.overlay_visible(kg, attempt, epoch) {
                continue;
            }
            let v = row_version(attempt, epoch);
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
    let session = client.inner.session();
    let prepared = client.inner.prepared().await?;
    let mut pages = Vec::new();
    for run in runs {
        let gran = run.granularity.to_millis();
        let granularity = run.granularity;
        let start_ts = run.start_ts;
        let end_ts = run.end_ts_exclusive;
        let last = if end_ts > start_ts {
            end_ts.saturating_sub(1)
        } else {
            start_ts.saturating_sub(1)
        };
        for bucket in time_buckets(start_ts, last, RAW_BUCKET_MS) {
            let session = Arc::clone(&session);
            let select_tiles = prepared.select_tiles.clone();
            let ns = client.scope.namespace.bytes.clone();
            let key = partition.business_key.clone();
            pages.push(async move {
                let result = session
                    .execute_unpaged(&select_tiles, (ns, kg, key, gran, bucket, start_ts, end_ts))
                    .await?;
                Ok::<_, anyhow::Error>((granularity, result))
            });
        }
    }
    let mut out = TileMap::new();
    for (granularity, result) in try_join_all(pages).await? {
        let rows = result.into_rows_result()?;
        let mut best: BTreeMap<i64, (Version, Vec<u8>)> = BTreeMap::new();
        for row in rows.rows::<(i64, i64, i64, Vec<u8>)>()? {
            let (tile_start, attempt, epoch, payload) = row?;
            if !client.overlay_visible(kg, attempt, epoch) {
                continue;
            }
            let v = row_version(attempt, epoch);
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
