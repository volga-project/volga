pub mod operator;
pub mod request;
pub mod spec;

pub mod aggs;
pub mod cate;
pub mod config;
pub mod eval;
pub mod frame_utils;
pub mod metrics;
pub mod model;
pub mod state;
pub mod store;
pub mod tile;
pub mod top;

#[cfg(test)]
mod tests;

pub use config::{BuiltWindows, WindowConfig};
pub use model::{Cursor, Tile, TimeGranularity};
pub use operator::{
    WindowOperator, WindowOutputMode, TASK_METADATA_ROWS_ACCEPTED, TASK_METADATA_ROWS_DROPPED_LATE,
};
pub use request::WindowRequestOperator;
pub use store::{InMemWindowStore, StateNamespace, WindowOperatorStore, WindowRequestStore};
pub use tile::TileConfig;

pub const SEQ_NO_COLUMN_NAME: &str = "__seq_no";

/// Concurrent per-key store ops for one ingest/request batch (remote RTT overlap).
pub(crate) const PARTITION_IO_CONCURRENCY: usize = 32;

pub use aggs::{
    create_window_accumulator, merge_accumulator_state, retract_accumulator_state,
    window_supports_tile_slide, AccumulatorType,
};
