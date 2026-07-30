pub mod operator;
pub mod request;
pub mod spec;

pub mod aggs;
pub mod cate;
pub mod config;
pub mod eval;
pub mod frame_utils;
pub mod model;
pub mod state;
pub mod store;
pub mod tile;
pub mod top;

#[cfg(test)]
mod tests;

pub use config::{BuiltWindows, WindowConfig};
pub use model::{Cursor, Tile, TimeGranularity};
pub use operator::{WindowOperator, WindowOutputMode};
pub use request::WindowRequestOperator;
pub use store::{InMemWindowStore, StateNamespace, WindowOperatorStore, WindowRequestStore};
pub use tile::TileConfig;

pub const SEQ_NO_COLUMN_NAME: &str = "__seq_no";

pub use aggs::{
    create_window_accumulator, merge_accumulator_state, retract_accumulator_state,
    window_supports_tile_slide, AccumulatorType,
};
