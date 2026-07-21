//! Preloaded tiles + merge/retract against a coverage plan.

use std::sync::Arc;

use datafusion::logical_expr::Accumulator;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggregates::{
    create_window_aggregator, merge_accumulator_state, retract_accumulator_state,
};
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::state::tile::{
    plan_coverage, CoveragePlan, Tile, TileConfig,
};
use crate::runtime::operators::window::store::row_nav::{RowIdx, RowNav};

use super::row_fold::{apply_row, retract_row};

pub(super) fn try_tiled_eval(
    window_expr: &Arc<dyn WindowExpr>,
    nav: &RowNav,
    config: &TileConfig,
    loaded: &[Tile],
    start_ts: i64,
    end_ts: i64,
    end_idx: RowIdx,
) -> Option<ScalarValue> {
    let end_c = nav.cursor(end_idx);
    let plan = plan_coverage(
        config,
        Cursor::new(start_ts, 0),
        Cursor::new(end_ts.saturating_add(1), 0),
    );
    if plan.tile_runs.is_empty() {
        return None;
    }

    let mut acc = create_window_aggregator(window_expr);

    merge_plan_tiles(acc.as_mut(), loaded, &plan);
    fold_plan_gaps(window_expr, nav, acc.as_mut(), &plan, end_c, end_idx);

    Some(acc.evaluate().expect("evaluate"))
}

/// Merge tiles + raw gaps into `acc` for `[win_start, end_idx]` (includes end row).
pub(super) fn seed_from_tiles(
    window_expr: &Arc<dyn WindowExpr>,
    nav: &RowNav,
    acc: &mut dyn Accumulator,
    config: &TileConfig,
    loaded: &[Tile],
    win_start: i64,
    end_idx: RowIdx,
) -> bool {
    let end_c = nav.cursor(end_idx);
    let plan = plan_coverage(
        config,
        Cursor::new(win_start, 0),
        Cursor::new(end_c.ts.saturating_add(1), 0),
    );
    if plan.tile_runs.is_empty() {
        return false;
    }
    merge_plan_tiles(acc, loaded, &plan);
    fold_plan_gaps(window_expr, nav, acc, &plan, end_c, end_idx);
    true
}

/// Retract tiles + raw gaps for `[leave_from_ts, leave_until_ts)`.
pub(super) fn try_tile_retract(
    window_expr: &Arc<dyn WindowExpr>,
    nav: &RowNav,
    acc: &mut Box<dyn Accumulator>,
    config: &TileConfig,
    loaded: &[Tile],
    leave_from_ts: i64,
    leave_until_ts: i64,
) -> bool {
    let plan = plan_coverage(
        config,
        Cursor::new(leave_from_ts, 0),
        Cursor::new(leave_until_ts, 0),
    );
    if plan.tile_runs.is_empty() {
        return false;
    }

    for tile in select_tiles_for_plan(loaded, &plan) {
        if let Some(state) = &tile.state.accumulator_state {
            retract_accumulator_state(window_expr, acc, state.as_ref());
        }
    }
    for gap in &plan.raw_gaps {
        retract_gap(window_expr, nav, acc.as_mut(), gap.from, gap.to);
    }
    true
}

fn merge_plan_tiles(acc: &mut dyn Accumulator, loaded: &[Tile], plan: &CoveragePlan) {
    for tile in select_tiles_for_plan(loaded, plan) {
        if let Some(state) = &tile.state.accumulator_state {
            merge_accumulator_state(acc, state.as_ref());
        }
    }
}

fn fold_plan_gaps(
    window_expr: &Arc<dyn WindowExpr>,
    nav: &RowNav,
    acc: &mut dyn Accumulator,
    plan: &CoveragePlan,
    end_c: Cursor,
    end_idx: RowIdx,
) {
    for gap in &plan.raw_gaps {
        let gap_to = if gap.to.ts > end_c.ts
            || (gap.to.ts == end_c.ts && gap.to.seq_no > end_c.seq_no)
        {
            Cursor::new(end_c.ts, end_c.seq_no.saturating_add(1))
        } else {
            gap.to
        };
        fold_gap(window_expr, nav, acc, gap.from, gap_to, end_idx);
    }
}

fn select_tiles_for_plan<'a>(loaded: &'a [Tile], plan: &CoveragePlan) -> Vec<&'a Tile> {
    loaded
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

fn fold_gap(
    window_expr: &Arc<dyn WindowExpr>,
    nav: &RowNav,
    acc: &mut dyn Accumulator,
    from: Cursor,
    to: Cursor,
    end_idx: RowIdx,
) {
    let Some(mut i) = nav.seek_ge(from) else {
        return;
    };
    while i <= end_idx {
        let c = nav.cursor(i);
        if c >= to {
            break;
        }
        apply_row(window_expr, nav, i, acc);
        match nav.next(i) {
            Some(n) => i = n,
            None => break,
        }
    }
}

fn retract_gap(
    window_expr: &Arc<dyn WindowExpr>,
    nav: &RowNav,
    acc: &mut dyn Accumulator,
    from: Cursor,
    to: Cursor,
) {
    let Some(mut i) = nav.seek_ge(from) else {
        return;
    };
    loop {
        let c = nav.cursor(i);
        if c >= to {
            break;
        }
        retract_row(window_expr, nav, i, acc);
        match nav.next(i) {
            Some(n) => i = n,
            None => break,
        }
    }
}
