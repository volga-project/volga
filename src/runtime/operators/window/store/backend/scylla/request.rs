use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use futures::future::try_join_all;

use crate::runtime::operators::window::model::{Cursor, PartitionKey, RawRun, TileMap, TileRun};
use crate::runtime::operators::window::store::backend::codec::{decode_batch, decode_val};
use crate::runtime::operators::window::store::data::WindowData;

use super::schema::{align_down, RAW_BUCKET_MS};
use super::store::ScyllaWindowStore;
use super::vis::{cell_newer, wro_visible};

struct LeasePin {
    serving_epoch: i64,
    prev_attempt: Vec<u8>,
    prev_epoch: i64,
}

async fn pin_lease(store: &ScyllaWindowStore, ns: &[u8], kg: i32) -> Result<Option<LeasePin>> {
    let session = store.session();
    let prepared = store.prepared().await?;
    let result = session
        .execute_unpaged(&prepared.select_lease, (ns.to_vec(), kg))
        .await?;
    let rows = result.into_rows_result()?;
    let mut iter = rows.rows::<(Vec<u8>, Vec<u8>, i64, i64, Vec<u8>, i64)>()?;
    let Some(row) = iter.next().transpose()? else {
        return Ok(None);
    };
    let (_owner, serving_attempt, serving_epoch, _serving_wm, prev_attempt, prev_epoch) = row;
    if serving_attempt.is_empty() {
        return Ok(None);
    }
    Ok(Some(LeasePin {
        serving_epoch,
        prev_attempt,
        prev_epoch,
    }))
}

pub(super) async fn load_window_data(
    store: &ScyllaWindowStore,
    max_parallelism: usize,
    partition: &PartitionKey,
    raw_runs: &[RawRun],
    tile_runs: &[TileRun],
) -> Result<WindowData> {
    let kg = partition.key_group(max_parallelism) as i32;
    let Some(pin) = pin_lease(store, &partition.namespace, kg).await? else {
        return Ok(WindowData::new(Vec::new(), TileMap::new()));
    };
    let vis = |attempt: &[u8], epoch: i64| {
        wro_visible(
            attempt,
            epoch,
            pin.serving_epoch,
            &pin.prev_attempt,
            pin.prev_epoch,
        )
    };
    let (raw, tiles) = tokio::try_join!(
        load_raw(store, partition, kg, raw_runs, &vis),
        load_tiles(store, partition, kg, tile_runs, &vis),
    )?;
    Ok(WindowData::new(raw, tiles))
}

async fn load_raw(
    store: &ScyllaWindowStore,
    partition: &PartitionKey,
    kg: i32,
    runs: &[RawRun],
    vis: &impl Fn(&[u8], i64) -> bool,
) -> Result<Vec<RecordBatch>> {
    let session = store.session();
    let prepared = store.prepared().await?;
    let mut pages = Vec::new();
    for run in runs {
        let mut bucket = align_down(run.from.ts, RAW_BUCKET_MS);
        let end_bucket = align_down(run.to.ts.saturating_sub(1).max(run.from.ts), RAW_BUCKET_MS);
        while bucket <= end_bucket {
            let session = Arc::clone(&session);
            let select_raw = prepared.select_raw.clone();
            let ns = partition.namespace.clone();
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
    let mut by_cursor: BTreeMap<Cursor, (i64, Vec<u8>, RecordBatch)> = BTreeMap::new();
    for (from, to, result) in try_join_all(pages).await? {
        let rows = result.into_rows_result()?;
        for row in rows.rows::<(i64, i64, Vec<u8>, i64, Vec<u8>)>()? {
            let (ts, seq, attempt, epoch, payload) = row?;
            let cursor = Cursor::new(ts, seq as u64);
            if cursor < from || cursor >= to || !vis(&attempt, epoch) {
                continue;
            }
            let replace = by_cursor
                .get(&cursor)
                .map_or(true, |(e, a, _)| cell_newer(epoch, &attempt, *e, a));
            if replace {
                by_cursor.insert(cursor, (epoch, attempt, decode_batch(&payload)?));
            }
        }
    }
    Ok(by_cursor.into_values().map(|(_, _, batch)| batch).collect())
}

async fn load_tiles(
    store: &ScyllaWindowStore,
    partition: &PartitionKey,
    kg: i32,
    runs: &[TileRun],
    vis: &impl Fn(&[u8], i64) -> bool,
) -> Result<TileMap> {
    let session = store.session();
    let prepared = store.prepared().await?;
    let pages = runs.iter().map(|run| {
        let gran = run.granularity.to_millis();
        let granularity = run.granularity;
        let bucket = align_down(run.start_ts, RAW_BUCKET_MS);
        let start_ts = run.start_ts;
        let end_ts = run.end_ts_exclusive;
        let session = Arc::clone(&session);
        let select_tiles = prepared.select_tiles.clone();
        let ns = partition.namespace.clone();
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
        let mut best: BTreeMap<i64, (i64, Vec<u8>, Vec<u8>)> = BTreeMap::new();
        for row in rows.rows::<(i64, Vec<u8>, i64, Vec<u8>)>()? {
            let (tile_start, attempt, epoch, payload) = row?;
            if !vis(&attempt, epoch) {
                continue;
            }
            let replace = best
                .get(&tile_start)
                .map_or(true, |(e, a, _)| cell_newer(epoch, &attempt, *e, a));
            if replace {
                best.insert(tile_start, (epoch, attempt, payload));
            }
        }
        for (tile_start, (_, _, payload)) in best {
            let tiles: crate::runtime::operators::window::model::WindowTiles = decode_val(&payload)?;
            out.insert((granularity, tile_start), tiles);
        }
    }
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct ScyllaWindowRequestStore {
    inner: ScyllaWindowStore,
    max_parallelism: usize,
}

impl ScyllaWindowRequestStore {
    pub fn new(inner: ScyllaWindowStore, max_parallelism: usize) -> Self {
        Self {
            inner,
            max_parallelism: max_parallelism.max(1),
        }
    }
}

#[async_trait::async_trait]
impl crate::runtime::operators::window::store::backend::WindowRequestStore
    for ScyllaWindowRequestStore
{
    async fn load_window_data(
        &self,
        partition: &PartitionKey,
        raw_runs: &[RawRun],
        tile_runs: &[TileRun],
    ) -> Result<WindowData> {
        load_window_data(
            &self.inner,
            self.max_parallelism,
            partition,
            raw_runs,
            tile_runs,
        )
        .await
    }
}
