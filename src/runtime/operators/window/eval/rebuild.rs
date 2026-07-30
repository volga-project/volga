//! Rebuild: per-emit produce + shared [`eval_rebuild`] kernel.

use std::sync::Arc;

use arrow::array::ArrayRef;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggs::{apply_request_args, create_window_accumulator};
use crate::runtime::operators::window::store::data::RowIdx;
use crate::runtime::operators::window::store::WindowView;

use super::accumulate::update_acc_from_plan;
use super::coverage_plan::CoveragePlan;
use super::eval_plan::EvalPlan;

/// Rebuild each result from its planned tile and raw coverage.
pub(super) fn produce_rebuild(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    plans: &[EvalPlan],
) -> Vec<ScalarValue> {
    let mut values = Vec::with_capacity(plans.len());
    for plan in plans {
        let idx = view
            .nav
            .seek_ge(plan.end)
            .filter(|&idx| view.nav.cursor(idx) == plan.end)
            .expect("planned rebuild end must be loaded");
        values.push(eval_rebuild(
            window_expr,
            view,
            Some(idx),
            plan.coverage.as_ref().expect("rebuild coverage"),
            None,
        ));
    }
    values
}

/// One window answer from planned tiles/raw rows, then an optional request row.
pub(crate) fn eval_rebuild(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    end_idx: Option<RowIdx>,
    coverage: &CoveragePlan,
    request_args: Option<&[ArrayRef]>,
) -> ScalarValue {
    let mut acc = create_window_accumulator(window_expr);
    update_acc_from_plan(view, acc.as_mut(), coverage, end_idx);
    apply_request_args(acc.as_mut(), request_args);
    acc.evaluate().expect("evaluate")
}
