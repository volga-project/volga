pub mod window_operator;
pub mod window_request_operator;

pub mod aggregates;
pub mod state;

pub use window_operator::WindowOperator;
pub use window_request_operator::WindowRequestOperator;
pub use state::tiles::{Tiles, TileConfig, TimeGranularity};
pub use state::window_operator_state;

pub const SEQ_NO_COLUMN_NAME: &str = "__seq_no";

pub use state::index::{BucketIndex, Cursor, RowPtr};

pub use aggregates::{AggregatorType, Evaluator, AggregateRegistry, WindowAggregator, create_window_aggregator, get_aggregate_registry, Aggregation};
pub use aggregates::plain::PlainAggregation;
pub use aggregates::retractable::RetractableAggregation;