//! WO leave+add slide.
//!
//! ```text
//! retract leave → add through target → evaluate
//! ```

use std::sync::Arc;

use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggs::{create_window_accumulator, merge_accumulator_state};
use crate::runtime::operators::window::model::{AccumulatorState, Cursor};
use crate::runtime::operators::window::store::WindowView;

use super::accumulate::{retract_acc_from_plan, update_row};
use super::eval_plan::EvalPlan;

/// WO: retract each preplanned leave band, add its end row, and evaluate.
pub(super) fn produce_slide(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    prev: Option<Cursor>,
    accumulator_state: Option<&AccumulatorState>,
    plans: &[EvalPlan],
) -> (Vec<ScalarValue>, Option<AccumulatorState>) {
    let mut acc = create_window_accumulator(window_expr);
    if let (Some(state), Some(_)) = (accumulator_state, prev) {
        merge_accumulator_state(acc.as_mut(), state);
    }
    let mut values = Vec::with_capacity(plans.len());
    for plan in plans {
        if let Some(coverage) = &plan.coverage {
            retract_acc_from_plan(window_expr, view, &mut acc, coverage);
        }
        let update_idx = view
            .nav
            .seek_ge(plan.end)
            .filter(|&idx| view.nav.cursor(idx) == plan.end)
            .expect("planned slide end must be loaded");
        update_row(&view.nav, update_idx, acc.as_mut());
        values.push(acc.evaluate().expect("eval"));
    }
    (values, Some(acc.state().expect("state")))
}
