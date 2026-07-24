//! Update / retract an [`Accumulator`] from a [`CoveragePlan`] + [`WindowView`].
//!
//! Coverage is CPU-only (store already loaded the envelope). Missing tile ⇒ empty bucket.

use std::sync::Arc;

use datafusion::logical_expr::Accumulator;
use datafusion::physical_plan::WindowExpr;

use crate::runtime::operators::window::aggregates::{
    merge_accumulator_state, retract_accumulator_state, VirtualPoint,
};
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::state::tile::{CoveragePlan, Tile};
use crate::runtime::operators::window::store::row_nav::{RowIdx, RowNav};

use crate::runtime::operators::window::store::WindowView;

/// Merge plan tiles and update raw-run rows into an existing accumulator.
///
/// Returns `false` when the plan has no tile runs (caller should use raw rows).
pub(crate) fn update_acc_from_plan(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    acc: &mut dyn Accumulator,
    plan: &CoveragePlan,
    end_idx: Option<RowIdx>,
) -> bool {
    if plan.tile_runs.is_empty() {
        return false;
    }
    for tile in select_tiles_for_plan(&view.tiles, plan) {
        if let Some(state) = &tile.state.accumulator_state {
            merge_accumulator_state(acc, state.as_ref());
        }
    }
    let Some(end_idx) = end_idx else {
        return true;
    };
    let end_c = view.nav.cursor(end_idx);
    for run in plan.raw_edges() {
        let to = if run.to.ts > end_c.ts
            || (run.to.ts == end_c.ts && run.to.seq_no > end_c.seq_no)
        {
            Cursor::new(end_c.ts, end_c.seq_no.saturating_add(1))
        } else {
            run.to
        };
        update_raw_run(window_expr, view, acc, run.from, to, end_idx);
    }
    true
}

/// Retract plan tiles + raw-run rows (half-open leave band from the plan).
///
/// Returns `false` when the plan has no tile runs (caller should retract raw rows).
pub(crate) fn retract_acc_from_plan(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    acc: &mut Box<dyn Accumulator>,
    plan: &CoveragePlan,
) -> bool {
    if plan.tile_runs.is_empty() {
        return false;
    }
    for tile in select_tiles_for_plan(&view.tiles, plan) {
        if let Some(state) = &tile.state.accumulator_state {
            retract_accumulator_state(window_expr, acc, state.as_ref());
        }
    }
    for run in plan.raw_edges() {
        retract_raw_run(window_expr, view, acc.as_mut(), run.from, run.to);
    }
    true
}

/// Update accumulator with one nav row (`Accumulator::update_batch`).
pub(crate) fn update_row(
    window_expr: &Arc<dyn WindowExpr>,
    nav: &RowNav,
    idx: RowIdx,
    acc: &mut dyn Accumulator,
) {
    if let Some(args) = nav.args(idx) {
        acc.update_batch(&args).expect("update");
    } else {
        let batch = nav.batch(idx);
        let args = window_expr.evaluate_args(&batch).expect("args");
        acc.update_batch(&args).expect("update");
    }
}

/// Retract one nav row from an accumulator (`Accumulator::retract_batch`).
pub(crate) fn retract_row(
    window_expr: &Arc<dyn WindowExpr>,
    nav: &RowNav,
    idx: RowIdx,
    acc: &mut dyn Accumulator,
) {
    if let Some(args) = nav.args(idx) {
        acc.retract_batch(&args).expect("retract");
    } else {
        let batch = nav.batch(idx);
        let args = window_expr.evaluate_args(&batch).expect("args");
        acc.retract_batch(&args).expect("retract");
    }
}

fn select_tiles_for_plan<'a>(tiles: &'a [Tile], plan: &CoveragePlan) -> Vec<&'a Tile> {
    tiles
        .iter()
        .filter(|t| {
            plan.tile_runs.iter().any(|r| {
                r.granularity == t.granularity
                    && t.tile_start >= r.start_ts
                    && t.tile_end <= r.end_ts_exclusive
            })
        })
        .collect()
}

fn update_raw_run(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    acc: &mut dyn Accumulator,
    from: Cursor,
    to: Cursor,
    end_idx: RowIdx,
) {
    let Some(mut i) = view.nav.seek_ge(from) else {
        return;
    };
    while i <= end_idx {
        let c = view.nav.cursor(i);
        if c >= to {
            break;
        }
        update_row(window_expr, &view.nav, i, acc);
        match view.nav.next(i) {
            Some(n) => i = n,
            None => break,
        }
    }
}

fn retract_raw_run(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    acc: &mut dyn Accumulator,
    from: Cursor,
    to: Cursor,
) {
    let Some(mut i) = view.nav.seek_ge(from) else {
        return;
    };
    loop {
        let c = view.nav.cursor(i);
        if c >= to {
            break;
        }
        retract_row(window_expr, &view.nav, i, acc);
        match view.nav.next(i) {
            Some(n) => i = n,
            None => break,
        }
    }
}

/// Optional virtual request row into an accumulator (WRO include).
pub(crate) fn apply_request_row(acc: &mut dyn Accumulator, request_row: Option<&VirtualPoint>) {
    let Some(point) = request_row else {
        return;
    };
    if let Some(args) = &point.args {
        acc.update_batch(args).expect("update request row");
    }
}
