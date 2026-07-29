//! Window tiling: fixed-time pre-aggregates for long RANGE windows.
//!
//! - [`Tile`] / [`TileState`] / [`WindowTiles`] / [`TileConfig`]
//! - [`plan_coverage`] / [`CoveragePlan`] — pure geometry (missing tile ⇒ empty)
//! - In-mem ingest merge: [`update`]
//! - Persistence through the window-domain store

mod granularity;
mod plan;
mod tile;
pub mod update;

#[cfg(test)]
mod tests;

pub use granularity::{TileConfig, TimeGranularity, Timestamp};
pub use plan::{
    merge_raw_runs, merge_tile_runs, plan_coverage, plan_time_range, plan_update_runs,
    CoveragePlan, RawRun, TileRun,
};
pub use tile::{project_tiles, Tile, TileState, WindowTiles};
