//! Shared evaluate primitives: planning, KV load, coverage apply.

mod coverage;
mod load;
mod plan;

pub(super) use coverage::{
    apply_row, rebuild_from_coverage, retract_from_coverage, retract_row, seed_from_coverage,
};
pub(super) use load::{load, load_window, WindowView};
pub(super) use plan::{
    coverage_end_exclusive, cursor_next, plan_gap_only_ends, plan_slide, LoadPlan, RawScanRun,
};
