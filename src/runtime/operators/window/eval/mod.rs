//! Window evaluate: WO slide/rebuild and WRO exact rebuild.
//!
//! ```text
//! WO: meta → exact ranges → slide | rebuild
//! WRO: exact point plans → coherent snapshot read → rebuild
//! ```

mod advance;
mod emit;
mod output;
mod primitives;
mod rebuild;
mod slide;
mod wro;

pub use advance::advance_key;
pub use output::assemble_window_batch;
pub use wro::evaluate_points;
