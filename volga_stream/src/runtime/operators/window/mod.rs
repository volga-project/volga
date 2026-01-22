pub mod window_operator;
pub mod window_request_operator;
pub mod window_tuning;
pub mod window_storage_spec;

pub mod aggregates;
pub mod cate;
pub mod top;
pub mod shared;
pub mod state;

#[cfg(test)]
mod window_operator_tests;

pub use window_operator::WindowOperator;
pub use window_request_operator::WindowRequestOperator;
pub use shared::WindowConfig;
pub use state::tiles::{Tiles, TileConfig, TimeGranularity};
pub use state::window_operator_state;

pub const SEQ_NO_COLUMN_NAME: &str = "__seq_no";

pub use crate::storage::index::{BucketIndex, Cursor, RowPtr};

pub use aggregates::{AggregatorType, Evaluator, AggregateRegistry, WindowAggregator, create_window_aggregator, get_aggregate_registry, Aggregation};
pub use aggregates::plain::PlainAggregation;
pub use aggregates::retractable::RetractableAggregation;