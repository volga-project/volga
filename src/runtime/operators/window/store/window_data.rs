//! Materialized raw and tile data for window evaluation.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::physical_plan::WindowExpr;

use crate::runtime::operators::window::state::tile::{
    project_tiles, Tile, TimeGranularity, WindowTiles,
};
use crate::runtime::operators::window::store::row_nav::RowNav;
use crate::runtime::operators::window::window_operator_state::WindowId;

pub type TileMap = BTreeMap<(TimeGranularity, i64), WindowTiles>;

/// Raw batches and tiles loaded for one partition.
#[derive(Debug)]
pub struct WindowData {
    raw_batches: Vec<RecordBatch>,
    tile_map: TileMap,
}

impl WindowData {
    pub fn new(raw_batches: Vec<RecordBatch>, tile_map: TileMap) -> Self {
        Self {
            raw_batches,
            tile_map,
        }
    }

    pub fn raw_batches(&self) -> &[RecordBatch] {
        &self.raw_batches
    }

    pub fn for_window(
        &self,
        window_id: WindowId,
        ts_column_index: usize,
        window_expr: &Arc<dyn WindowExpr>,
    ) -> WindowView {
        WindowView {
            nav: RowNav::from_batches(&self.raw_batches, ts_column_index, window_expr),
            tiles: project_tiles(&self.tile_map, window_id),
        }
    }
}

#[derive(Debug)]
pub struct WindowView {
    pub nav: RowNav,
    pub tiles: Vec<Tile>,
}
