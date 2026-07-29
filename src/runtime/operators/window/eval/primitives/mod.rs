//! Shared evaluate primitives: CPU coverage plan, acc update/retract.

mod acc;
mod plan;

pub(crate) use acc::{apply_request_args, retract_acc_from_plan, update_acc_from_plan, update_row};
pub(crate) use plan::{
    append_coverage_runs, append_rebuild_runs, plan_rebuilds, plan_slides, EvalPlan,
};
