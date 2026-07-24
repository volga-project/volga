//! Geometry envelope load: meta + raw + tile interior under one partition snapshot.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use datafusion::physical_plan::WindowExpr;
use futures::future::{BoxFuture, FutureExt};

use crate::common::Key;
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::state::tile::{
    merge_tile_runs, plan_coverage, project_tiles, Tile, TileConfig, TimeGranularity,
    WindowTiles,
};
use crate::runtime::operators::window::store::event_chunk::EventChunk;
use crate::runtime::operators::window::store::keys::partition_key;
use crate::runtime::operators::window::store::meta_store::KeyState;
use crate::runtime::operators::window::store::row_nav::RowNav;
use crate::runtime::operators::window::window_operator_state::WindowId;
use crate::storage::KvSnapshot;

use super::WindowStateStore;

pub type TileMap = BTreeMap<(TimeGranularity, i64), WindowTiles>;

/// Meta + data from one [`SortedKV::snapshot_partition`].
///
/// Not an eval “view” — that is [`WindowView`] (per-window projection of [`WindowData`]).
#[derive(Debug, Clone)]
pub struct PartitionData {
    pub key_state: KeyState,
    pub data: WindowData,
}

/// Loaded raw chunks + tiles for one key (eval input).
#[derive(Debug, Clone)]
pub struct WindowData {
    chunks: Arc<[EventChunk]>,
    tile_map: TileMap,
}

impl WindowData {
    pub fn new(chunks: Vec<EventChunk>, tile_map: TileMap) -> Self {
        Self {
            chunks: chunks.into(),
            tile_map,
        }
    }

    pub fn chunks(&self) -> &[EventChunk] {
        &self.chunks
    }

    pub fn for_window(
        &self,
        window_id: WindowId,
        window_expr: &Arc<dyn WindowExpr>,
    ) -> WindowView {
        WindowView {
            nav: RowNav::from_chunks(Arc::clone(&self.chunks), window_expr),
            tiles: project_tiles(&self.tile_map, window_id).into(),
        }
    }
}

#[derive(Debug)]
pub struct WindowView {
    pub nav: RowNav,
    pub tiles: Arc<[Tile]>,
}

impl WindowStateStore {
    /// One snapshot → meta + raw/tiles in `[from, to]`.
    /// Raw scan uses `bucket_ms` to touch only overlapping `bucket_ts` key ranges.
    pub async fn load_envelope(
        &self,
        key: &Key,
        from: Cursor,
        to: Cursor,
        bucket_ms: i64,
        tile_cfgs: &[&TileConfig],
    ) -> Result<PartitionData> {
        let pk = partition_key(&self.ns, key);
        let snap = self.kv.snapshot_partition(&pk).await?;
        let key_state = self.meta.get_key_on(snap.as_ref(), key).await?;
        let data = load_range_on_snap(self, snap, key, from, to, bucket_ms, tile_cfgs).await?;
        Ok(PartitionData { key_state, data })
    }

    /// Data-only load (same snap model as [`Self::load_envelope`], no meta).
    /// Used for rare WO underfetch widen after meta is already in hand.
    pub async fn load_data(
        &self,
        key: &Key,
        from: Cursor,
        to: Cursor,
        bucket_ms: i64,
        tile_cfgs: &[&TileConfig],
    ) -> Result<WindowData> {
        let pk = partition_key(&self.ns, key);
        let snap = self.kv.snapshot_partition(&pk).await?;
        load_range_on_snap(self, snap, key, from, to, bucket_ms, tile_cfgs).await
    }
}

async fn load_range_on_snap(
    store: &WindowStateStore,
    snap: Arc<dyn KvSnapshot>,
    key: &Key,
    from: Cursor,
    to: Cursor,
    bucket_ms: i64,
    tile_cfgs: &[&TileConfig],
) -> Result<WindowData> {
    if to < from {
        return Ok(WindowData::new(vec![], TileMap::new()));
    }

    enum Part {
        Raw(Vec<EventChunk>),
        Tile(TimeGranularity, Vec<(i64, WindowTiles)>),
    }

    let mut futs: Vec<BoxFuture<'_, Result<Part>>> = Vec::new();
    {
        let snap = Arc::clone(&snap);
        futs.push(
            async move {
                Ok(Part::Raw(
                    store
                        .events
                        .scan(snap.as_ref(), key, from, to, bucket_ms)
                        .await?,
                ))
            }
            .boxed(),
        );
    }

    if from.ts != i64::MIN && !tile_cfgs.is_empty() {
        let end_excl = Cursor::new(to.ts.saturating_add(1), 0);
        let start = Cursor::new(from.ts, 0);
        let mut tile_runs = Vec::new();
        for cfg in tile_cfgs {
            tile_runs.extend(plan_coverage(cfg, start, end_excl).tile_runs);
        }
        for run in merge_tile_runs(tile_runs) {
            let gran = run.granularity;
            let start_ts = run.start_ts;
            let end_ts = run.end_ts_exclusive;
            let snap = Arc::clone(&snap);
            futs.push(
                async move {
                    Ok(Part::Tile(
                        gran,
                        store
                            .tiles
                            .scan(snap.as_ref(), key, gran, start_ts, end_ts)
                            .await?,
                    ))
                }
                .boxed(),
            );
        }
    }

    let mut chunks = Vec::new();
    let mut tiles = TileMap::new();
    for part in futures::future::try_join_all(futs).await? {
        match part {
            Part::Raw(c) => chunks.extend(c),
            Part::Tile(gran, items) => {
                for (tile_start, wt) in items {
                    tiles.insert((gran, tile_start), wt);
                }
            }
        }
    }
    Ok(WindowData::new(chunks, tiles))
}
