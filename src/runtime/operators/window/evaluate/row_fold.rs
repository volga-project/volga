//! Shared helpers: apply/retract one nav row into an accumulator.

use std::sync::Arc;

use datafusion::logical_expr::Accumulator;
use datafusion::physical_plan::WindowExpr;

use crate::runtime::operators::window::store::row_nav::{RowIdx, RowNav};

pub(super) fn apply_row(
    window_expr: &Arc<dyn WindowExpr>,
    nav: &RowNav,
    idx: RowIdx,
    acc: &mut dyn Accumulator,
) {
    if let Some(args) = nav.args(idx) {
        acc.update_batch(args.as_slice()).expect("update");
    } else {
        let args = window_expr
            .evaluate_args(nav.batch(idx))
            .expect("args");
        acc.update_batch(&args).expect("update");
    }
}

pub(super) fn retract_row(
    window_expr: &Arc<dyn WindowExpr>,
    nav: &RowNav,
    idx: RowIdx,
    acc: &mut dyn Accumulator,
) {
    if let Some(args) = nav.args(idx) {
        acc.retract_batch(args.as_slice()).expect("retract");
    } else {
        let args = window_expr
            .evaluate_args(nav.batch(idx))
            .expect("args");
        acc.retract_batch(&args).expect("retract");
    }
}
