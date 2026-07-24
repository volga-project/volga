//! Window evaluate: **slide** or **rebuild** for WO + WRO.
//!
//! ```text
//! estimate envelope (no meta) → load_envelope (meta+data, one snap)
//!   can_slide → leave+add (produce_slide / slide_to_point)
//!   else      → rebuild from tiles+raw
//! ```

mod advance;
mod emit;
mod envelope;
mod output;
mod primitives;
mod rebuild;
mod slide;
mod wro;

pub use advance::advance_key;
pub use output::assemble_window_batch;
pub use wro::evaluate_points;

use crate::runtime::operators::window::config::WindowConfig;
use crate::runtime::operators::window::AggregatorType;

/// True when this window can leave+add (retractable acc).
pub(crate) fn can_slide(cfg: &WindowConfig) -> bool {
    cfg.aggregator_type == AggregatorType::RetractableAccumulator
}
