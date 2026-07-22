//! RANGE window evaluate over SortedKV-backed rows + tiles.
//!
//! ## Execution matrix
//!
//! |            | Plain                                      | Retractable                                                         |
//! |------------|--------------------------------------------|---------------------------------------------------------------------|
//! | **WO**     | [`rebuild`] each emit                      | no acc: seed/rebuild then slide; warm: [`slide`] leave+add          |
//! | **WRO**    | [`rebuild`] (+ request row if !exclude)    | `T < P` / no acc / window jumped: [`rebuild`]; else [`slide`]       |
//!
//! Cold WO (no `processed_pos`) uses `first_ingested` only to close the load
//! lower bound — same rebuild/slide code paths, not a separate cold module.
//!
//! WRO retractable fast path: load meta only if any window is retractable.
//!
//! ## WRO exclude / include
//!
//! Window bounds always use request `T` (store `ts ∈ [T−wl, T]`). Exclude only
//! controls whether the virtual request **value** is folded in:
//! - **Include** (`!exclude`): apply request args on top of store through `T`
//! - **Exclude**: same store window; skip request args (lookup time only)
//!
//! If include and store already has rows at `T`, the virtual value double-counts —
//! use exclude when you want bounds-only.
//!
//! ## Layout
//!
//! ```text
//! evaluate/
//!   primitives/   — plan, load, coverage
//!   strategies/   — rebuild, slide
//!   wo/           — advance, plan, produce
//!   wro/          — points, load
//!   output.rs     — assemble_window_batch
//! ```

mod output;
mod primitives;
mod strategies;
mod wo;
mod wro;

pub use output::assemble_window_batch;
pub use wo::advance_key;
pub use wro::evaluate_range_points;
