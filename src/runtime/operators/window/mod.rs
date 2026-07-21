pub mod window_operator;
pub mod window_request_operator;
pub mod window_tuning;

pub mod aggregates;
pub mod cate;
pub mod top;
pub mod shared;
pub mod state;
pub mod store;
pub mod cursor;
pub mod frame_utils;
pub mod evaluate;

#[cfg(test)]
mod window_operator_tests;

pub use window_operator::{WindowOperator, WindowEmitMode};
pub use window_request_operator::WindowRequestOperator;
pub use shared::WindowConfig;
pub use state::tile::{Tile, TileConfig, TimeGranularity};
pub use state::window_operator_state;
pub use cursor::Cursor;
pub use store::{StateNamespace, WindowStateStore};

pub const SEQ_NO_COLUMN_NAME: &str = "__seq_no";

pub use aggregates::{
    AggregatorType, create_window_aggregator, VirtualPoint, merge_accumulator_state,
    retract_accumulator_state, window_supports_tile_slide,
};
