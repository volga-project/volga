//! Apply / retract a [`CoveragePlan`] against a [`WindowView`].
//!
//! Callers plan once with [`plan_coverage`], load that plan, then pass the same
//! plan here (no re-plan). Missing KV tile ⇒ empty bucket.

use std::sync::Arc;

use datafusion::logical_expr::Accumulator;
use datafusion::physical_plan::WindowExpr;

use crate::runtime::operators::window::aggregates::{
    create_window_aggregator, merge_accumulator_state, retract_accumulator_state,
};
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::state::tile::{CoveragePlan, Tile};
use crate::runtime::operators::window::store::row_nav::{RowIdx, RowNav};

use super::load::WindowView;

/// Merge tiles + fold gap rows into a new accumulator.
///
/// Returns `None` when the plan has no tile runs (caller should fold raw).
pub(crate) fn rebuild_from_coverage(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    plan: &CoveragePlan,
    end_idx: Option<RowIdx>,
) -> Option<Box<dyn Accumulator>> {
    if plan.tile_runs.is_empty() {
        return None;
    }
    let mut acc = create_window_aggregator(window_expr);
    apply_plan(window_expr, view, acc.as_mut(), plan, end_idx);
    Some(acc)
}

/// Merge plan tiles and fold plan gaps into an existing accumulator.
pub(crate) fn seed_from_coverage(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    acc: &mut dyn Accumulator,
    plan: &CoveragePlan,
    end_idx: RowIdx,
) -> bool {
    if plan.tile_runs.is_empty() {
        return false;
    }
    apply_plan(window_expr, view, acc, plan, Some(end_idx));
    true
}

/// Retract plan tiles + gap rows (half-open leave band from the plan).
pub(crate) fn retract_from_coverage(
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
    for gap in &plan.raw_gaps {
        retract_gap(window_expr, view, acc.as_mut(), gap.from, gap.to);
    }
    true
}

/// Apply one nav row into an accumulator.
pub(crate) fn apply_row(
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

/// Retract one nav row from an accumulator.
pub(crate) fn retract_row(
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

fn apply_plan(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    acc: &mut dyn Accumulator,
    plan: &CoveragePlan,
    end_idx: Option<RowIdx>,
) {
    for tile in select_tiles_for_plan(&view.tiles, plan) {
        if let Some(state) = &tile.state.accumulator_state {
            merge_accumulator_state(acc, state.as_ref());
        }
    }
    let Some(end_idx) = end_idx else {
        return;
    };
    let end_c = view.nav.cursor(end_idx);
    for gap in &plan.raw_gaps {
        let gap_to = if gap.to.ts > end_c.ts
            || (gap.to.ts == end_c.ts && gap.to.seq_no > end_c.seq_no)
        {
            Cursor::new(end_c.ts, end_c.seq_no.saturating_add(1))
        } else {
            gap.to
        };
        apply_gap(window_expr, view, acc, gap.from, gap_to, end_idx);
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

fn apply_gap(
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
        apply_row(window_expr, &view.nav, i, acc);
        match view.nav.next(i) {
            Some(n) => i = n,
            None => break,
        }
    }
}

fn retract_gap(
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
