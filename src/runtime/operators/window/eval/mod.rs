//! Window evaluate: WO slide/rebuild and WRO exact rebuild.
//!
//! ```text
//! WO: meta → exact ranges → slide | rebuild
//! WRO: exact point plans → coherent snapshot read → rebuild
//! ```

mod accumulate;
mod advance;
pub(crate) mod coverage_plan;
mod emit;
mod eval_plan;
mod output;
mod rebuild;
mod slide;
mod wro;

pub use advance::advance_due_work;
pub use output::assemble_window_batch;
pub use wro::evaluate_points;
