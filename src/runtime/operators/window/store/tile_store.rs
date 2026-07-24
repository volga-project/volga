//! Tile SortedKV IO: get / scan / append / prune. No fan-out joins.

use anyhow::Result;
use futures::StreamExt;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::common::Key;
use crate::runtime::operators::window::state::tile::{TimeGranularity, WindowTiles};
use crate::storage::{KvSnapshot, SortedKV, WriteBatch};

use super::keys::{
    partition_key, tile_key, tile_key_prefix, tile_scan_end, tile_scan_start, StateNamespace,
};

#[derive(Debug, Clone)]
pub struct TileStore {
    kv: Arc<dyn SortedKV>,
    ns: StateNamespace,
}

impl TileStore {
    pub fn new(kv: Arc<dyn SortedKV>, ns: StateNamespace) -> Self {
        Self { kv, ns }
    }

    pub async fn write(&self, wb: WriteBatch) -> Result<()> {
        self.kv.write(wb).await
    }

    /// Partition-scoped snapshot for multi-scan reads of this business key.
    pub async fn snapshot(&self, key: &Key) -> Result<std::sync::Arc<dyn KvSnapshot>> {
        self.kv
            .snapshot_partition(&partition_key(&self.ns, key))
            .await
    }

    /// One KV get. Missing key → empty [`WindowTiles`].
    pub async fn get(
        &self,
        key: &Key,
        gran: TimeGranularity,
        tile_start: i64,
    ) -> Result<WindowTiles> {
        let k = tile_key(&self.ns, key, gran, tile_start);
        match self.kv.get(&k).await? {
            Some(bytes) => Ok(bincode::deserialize(&bytes)?),
            None => Ok(WindowTiles::default()),
        }
    }

    /// Append updated `WindowTiles` into `wb` (does not commit).
    pub fn append_window_tiles(
        &self,
        wb: &mut WriteBatch,
        key: &Key,
        by_key: &BTreeMap<(TimeGranularity, i64), WindowTiles>,
    ) -> Result<()> {
        for (&(gran, tile_start), window_tiles) in by_key {
            let k = tile_key(&self.ns, key, gran, tile_start);
            if window_tiles.windows.is_empty() {
                wb.delete(k);
            } else {
                wb.put(k, bincode::serialize(window_tiles)?);
            }
        }
        Ok(())
    }

    /// One KV scan of `WindowTiles` at `gran` in `[start_ts, end_ts_exclusive)`.
    ///
    /// Pass a [`KvSnapshot`] from `snapshot_partition` for multi-op loads.
    pub async fn scan(
        &self,
        reader: &dyn KvSnapshot,
        key: &Key,
        gran: TimeGranularity,
        start_ts: i64,
        end_ts_exclusive: i64,
    ) -> Result<Vec<(i64, WindowTiles)>> {
        let start = tile_scan_start(&self.ns, key, gran, start_ts);
        let end = tile_scan_end(&self.ns, key, gran, end_ts_exclusive);
        let mut out = Vec::new();
        let mut stream = reader.scan(&start, &end);
        while let Some(item) = stream.next().await {
            let (k, v) = item?;
            let tile_start = parse_tile_start_from_key(&k)?;
            let window_tiles: WindowTiles = bincode::deserialize(&v)?;
            out.push((tile_start, window_tiles));
        }
        Ok(out)
    }

    /// List + batch-delete tiles with tile_end <= cutoff.
    pub async fn prune_before(&self, key: &Key, cutoff_ts: i64) -> Result<()> {
        let prefix = tile_key_prefix(&self.ns, key);
        let mut end = prefix.clone();
        end.push(0xFF);
        let mut to_delete = Vec::new();
        let mut stream = self.kv.scan(&prefix, &end);
        while let Some(item) = stream.next().await {
            let (k, _v) = item?;
            let (gran_ms, tile_start) = parse_gran_and_start_from_key(&k)?;
            let tile_end = tile_start.saturating_add(gran_ms);
            if tile_end <= cutoff_ts {
                to_delete.push(k);
            }
        }
        if to_delete.is_empty() {
            return Ok(());
        }
        let mut wb = WriteBatch::with_capacity(to_delete.len());
        for k in to_delete {
            wb.delete(k);
        }
        self.kv.write(wb).await
    }
}

fn parse_gran_and_start_from_key(key: &[u8]) -> Result<(i64, i64)> {
    if key.len() < 17 {
        anyhow::bail!("tile key too short");
    }
    use super::keys::decode_i64;
    let start = decode_i64(key[key.len() - 8..].try_into()?);
    let gran = decode_i64(key[key.len() - 17..key.len() - 9].try_into()?);
    Ok((gran, start))
}

fn parse_tile_start_from_key(key: &[u8]) -> Result<i64> {
    Ok(parse_gran_and_start_from_key(key)?.1)
}
