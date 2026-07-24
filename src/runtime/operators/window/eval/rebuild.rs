//! Rebuild: per-emit produce + shared [`eval_rebuild`] kernel.

use std::sync::Arc;

use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggregates::{
    create_window_aggregator, VirtualPoint,
};
use crate::runtime::operators::window::frame_utils::get_window_length_ms;
use crate::runtime::operators::window::state::tile::CoveragePlan;
use crate::runtime::operators::window::store::row_nav::RowIdx;
use crate::runtime::operators::window::store::WindowView;

use super::primitives::{
    apply_request_row, update_acc_from_plan, update_row, EvalUnit,
};

/// Rebuild each unit (coverage preferred, else raw walk).
pub(super) fn produce_rebuild(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    units: &[EvalUnit],
) -> Vec<ScalarValue> {
    let wl = get_window_length_ms(window_expr.get_window_frame());
    let mut values = Vec::with_capacity(units.len());
    for unit in units {
        let Some(idx) = view.nav.seek_ge(unit.end) else {
            continue;
        };
        let end_c = view.nav.cursor(idx);
        if end_c != unit.end {
            continue;
        }
        values.push(eval_rebuild(
            window_expr,
            view,
            end_c.ts.saturating_sub(wl),
            Some(idx),
            unit.coverage.as_ref(),
            None,
        ));
    }
    values
}

/// One window answer: tiles+raw via `coverage` (preferred), else raw rows;
/// then optional request row.
pub(crate) fn eval_rebuild(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    start_ts: i64,
    end_idx: Option<RowIdx>,
    coverage: Option<&CoveragePlan>,
    request_row: Option<&VirtualPoint>,
) -> ScalarValue {
    if let Some(plan) = coverage {
        let mut acc = create_window_aggregator(window_expr);
        if update_acc_from_plan(window_expr, view, acc.as_mut(), plan, end_idx) {
            apply_request_row(acc.as_mut(), request_row);
            return acc.evaluate().expect("evaluate");
        }
    }
    update_raw_window(window_expr, view, start_ts, end_idx, request_row)
}

fn update_raw_window(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    start_ts: i64,
    end_idx: Option<RowIdx>,
    request_row: Option<&VirtualPoint>,
) -> ScalarValue {
    let Some(end_idx) = end_idx else {
        return eval_request_row_only(window_expr, request_row);
    };
    let start_idx = view.nav.seek_ts_ge(start_ts).unwrap_or(RowIdx(0));
    if start_idx > end_idx {
        return eval_request_row_only(window_expr, request_row);
    }

    let mut acc = create_window_aggregator(window_expr);
    let mut i = start_idx;
    loop {
        update_row(window_expr, &view.nav, i, acc.as_mut());
        if i == end_idx {
            break;
        }
        i = view.nav.next(i).expect("next");
    }
    apply_request_row(acc.as_mut(), request_row);
    acc.evaluate().expect("evaluate")
}

fn eval_request_row_only(
    window_expr: &Arc<dyn WindowExpr>,
    request_row: Option<&VirtualPoint>,
) -> ScalarValue {
    let Some(point) = request_row else {
        return ScalarValue::Null;
    };
    let Some(args) = &point.args else {
        return ScalarValue::Null;
    };
    let mut acc = create_window_aggregator(window_expr);
    acc.update_batch(args).expect("update request row");
    acc.evaluate().expect("eval")
}
