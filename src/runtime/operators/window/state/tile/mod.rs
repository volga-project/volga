//! Window tiling: time-bucket pre-aggregates for long RANGE windows.
//!
//! - [`Tile`] / [`TileState`] / [`WindowTiles`] / [`TileConfig`]
//! - [`plan_coverage`] / [`CoveragePlan`] — pure geometry (missing KV tile ⇒ empty)
//! - In-mem ingest merge: [`update`]
//! - Persistence: [`crate::runtime::operators::window::store::TileStore`]

mod granularity;
mod plan;
mod tile;
pub mod update;

#[cfg(test)]
mod tests;

pub use granularity::{TileConfig, TimeGranularity, Timestamp};
pub use plan::{
    merge_tile_runs, plan_coverage, plan_time_range, plan_update_runs, CoveragePlan, RawGap,
    TileScanRun,
};
pub use tile::{project_tiles, Tile, TileState, WindowTiles};
