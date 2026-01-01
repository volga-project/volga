pub mod window_operator;
pub mod window_request_operator;
// pub mod time_entries;
pub mod tiles;
pub mod aggregates;
pub mod window_operator_state;
pub mod index;
pub mod data_loader;

pub use window_operator::WindowOperator;
pub use window_request_operator::WindowRequestOperator;
pub use tiles::{Tiles, TileConfig, TimeGranularity};

pub const SEQ_NO_COLUMN_NAME: &str = "__seq_no";

pub use index::{BucketIndex, Cursor, RowPtr};

/// Backwards-compat: old name used across the codebase.
// pub type RowPos = Cursor;

pub use aggregates::{AggregatorType, Evaluator, AggregateRegistry, WindowAggregator, create_window_aggregator, get_aggregate_registry, Aggregation};
pub use aggregates::plain::PlainAggregation;
pub use aggregates::retractable::RetractableAggregation;