//! RANGE window evaluate / advance over SortedKV-backed rows.

mod advance;
mod plain;
mod points;
mod load_plan;
mod retractable;
mod row_fold;
mod tiles;

pub use advance::advance_key;
pub use points::evaluate_range_points;
