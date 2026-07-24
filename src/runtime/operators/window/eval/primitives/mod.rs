//! Shared evaluate primitives: CPU coverage plan, acc update/retract.

mod acc;
mod plan;

pub(crate) use acc::{
    apply_request_row, retract_acc_from_plan, retract_row, update_acc_from_plan, update_row,
};
pub(crate) use plan::{plan_rebuild_prep, EvalUnit};
