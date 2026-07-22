//! KV IO: execute a [`LoadPlan`] → [`KeyBatch`] / [`WindowView`].

use std::sync::Arc;

use anyhow::Result;
use datafusion::physical_plan::WindowExpr;
use futures::future::{BoxFuture, FutureExt};

use crate::common::Key;
use crate::runtime::operators::window::state::tile::{
    merge_tile_runs, project_tiles, Tile, TimeGranularity, WindowTiles,
};
use crate::runtime::operators::window::store::event_store::StoredRow;
use crate::runtime::operators::window::store::row_nav::RowNav;
use crate::runtime::operators::window::store::WindowStateStore;
use crate::runtime::operators::window::window_operator_state::WindowId;

use super::plan::{LoadPlan, TileMap};

/// Raw rows + packed tiles for one key after [`load`].
#[derive(Debug, Clone)]
pub(crate) struct KeyBatch {
    rows: Arc<[StoredRow]>,
    tile_map: TileMap,
}

impl KeyBatch {
    pub fn new(rows: Vec<StoredRow>, tile_map: TileMap) -> Self {
        Self {
            rows: rows.into(),
            tile_map,
        }
    }

    pub fn rows(&self) -> &[StoredRow] {
        &self.rows
    }

    pub(crate) fn tile_map(&self) -> &TileMap {
        &self.tile_map
    }

    /// Merge extra rows (e.g. peeked emit band) into a new batch; tiles unchanged.
    pub fn with_merged_rows(self, extra: Vec<StoredRow>) -> Self {
        if extra.is_empty() {
            return self;
        }
        let mut rows: Vec<StoredRow> = self.rows.iter().cloned().collect();
        rows.extend(extra);
        rows.sort_by_key(|r| r.cursor);
        rows.dedup_by(|a, b| a.cursor == b.cursor);
        Self {
            rows: rows.into(),
            tile_map: self.tile_map,
        }
    }

    /// Project tiles + build nav (args) for one window. Shares row Arc across windows.
    pub fn for_window(
        &self,
        window_id: WindowId,
        window_expr: &Arc<dyn WindowExpr>,
    ) -> WindowView {
        WindowView {
            nav: RowNav::from_stored_with_args(Arc::clone(&self.rows), window_expr),
            tiles: project_tiles(&self.tile_map, window_id).into(),
        }
    }
}

/// Per-window view used by rebuild / slide / coverage.
#[derive(Debug)]
pub(crate) struct WindowView {
    pub nav: RowNav,
    pub tiles: Arc<[Tile]>,
}

impl WindowView {
    pub fn empty(window_expr: &Arc<dyn WindowExpr>) -> Self {
        Self {
            nav: RowNav::from_stored_with_args(Vec::<StoredRow>::new(), window_expr),
            tiles: Arc::from([]),
        }
    }

    pub fn from_rows(
        rows: Vec<StoredRow>,
        tiles: impl Into<Arc<[Tile]>>,
        window_expr: &Arc<dyn WindowExpr>,
    ) -> Self {
        Self {
            nav: RowNav::from_stored_with_args(rows, window_expr),
            tiles: tiles.into(),
        }
    }
}

/// Load raw rows + tiles for `plan` into a shared key batch.
pub(crate) async fn load(
    store: &WindowStateStore,
    key: &Key,
    plan: LoadPlan,
) -> Result<KeyBatch> {
    enum Part {
        Raw(Vec<StoredRow>),
        Tile(TimeGranularity, Vec<(i64, WindowTiles)>),
    }

    let mut futs: Vec<BoxFuture<'_, Result<Part>>> = Vec::new();
    for run in &plan.raw_runs {
        let from = run.from;
        let to = run.to;
        futs.push(
            async move { Ok(Part::Raw(store.events.scan(key, from, to).await?)) }.boxed(),
        );
    }
    for run in merge_tile_runs(plan.tile_runs) {
        let gran = run.granularity;
        let start_ts = run.start_ts;
        let end_ts = run.end_ts_exclusive;
        futs.push(
            async move {
                Ok(Part::Tile(
                    gran,
                    store.tiles.scan(key, gran, start_ts, end_ts).await?,
                ))
            }
            .boxed(),
        );
    }

    let mut rows = Vec::new();
    let mut tiles = TileMap::new();
    for part in futures::future::try_join_all(futs).await? {
        match part {
            Part::Raw(chunk) => rows.extend(chunk),
            Part::Tile(gran, items) => {
                for (tile_start, window_tiles) in items {
                    tiles.insert((gran, tile_start), window_tiles);
                }
            }
        }
    }
    rows.sort_by_key(|r| r.cursor);
    rows.dedup_by(|a, b| a.cursor == b.cursor);
    Ok(KeyBatch::new(rows, tiles))
}

/// Plan → KV → per-window view.
pub(crate) async fn load_window(
    store: &WindowStateStore,
    key: &Key,
    plan: LoadPlan,
    window_id: WindowId,
    window_expr: &Arc<dyn WindowExpr>,
) -> Result<WindowView> {
    Ok(load(store, key, plan)
        .await?
        .for_window(window_id, window_expr))
}
