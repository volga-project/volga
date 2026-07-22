//! **Rebuild** — one RANGE window from scratch (shared WO/WRO kernel).
//!
//! Used by:
//! - **WO + plain**: each emit via [`crate::runtime::operators::window::evaluate::wo`]
//! - **WRO + plain**: each request point
//! - **WRO + retractable**: past `T`, missing acc, or window fully past `processed_pos`
//!
//! Path: threaded [`CoveragePlan`] → coverage rebuild on [`WindowView`] (else fold raw).
//! Optional request row when including the virtual value.

use std::sync::Arc;

use datafusion::logical_expr::Accumulator;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggregates::{
    create_window_aggregator, VirtualPoint,
};
use crate::runtime::operators::window::state::tile::CoveragePlan;
use crate::runtime::operators::window::store::row_nav::RowIdx;

use super::super::primitives::{apply_row, rebuild_from_coverage, WindowView};

/// One window answer: tiles+gaps via `coverage` (preferred), else fold raw;
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
        if let Some(mut acc) = rebuild_from_coverage(window_expr, view, plan, end_idx) {
            apply_request_row(acc.as_mut(), request_row);
            return acc.evaluate().expect("evaluate");
        }
    }

    apply_raw_window(window_expr, view, start_ts, end_idx, request_row)
}

fn apply_raw_window(
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
        apply_row(window_expr, &view.nav, i, acc.as_mut());
        if i == end_idx {
            break;
        }
        i = view.nav.next(i).expect("next");
    }
    apply_request_row(acc.as_mut(), request_row);
    acc.evaluate().expect("evaluate")
}

fn apply_request_row(acc: &mut dyn Accumulator, request_row: Option<&VirtualPoint>) {
    let Some(point) = request_row else {
        return;
    };
    if let Some(args) = &point.args {
        acc.update_batch(args).expect("update request row");
    }
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
