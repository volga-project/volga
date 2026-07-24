//! Leave+add slide — shared by WO (multi-emit) and WRO (one-shot → `T`).
//!
//! ```text
//! retract leave → add through target → evaluate
//! ```
//! WO: evaluate after each add. WRO: evaluate once (+ optional request row).

use std::sync::Arc;

use datafusion::logical_expr::Accumulator;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggregates::{
    create_window_aggregator, merge_accumulator_state, window_supports_tile_slide, VirtualPoint,
};
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::state::tile::{
    plan_coverage, plan_time_range, TileConfig,
};
use crate::runtime::operators::window::store::row_nav::RowIdx;
use crate::runtime::operators::window::store::WindowView;
use crate::runtime::operators::window::window_operator_state::AccumulatorState;

use super::primitives::{
    apply_request_row, retract_acc_from_plan, retract_row, update_acc_from_plan, update_row,
};

/// WO: seed if needed, then each-row slide through `advance_to`.
pub(super) fn produce_slide(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    prev: Option<Cursor>,
    advance_to: Cursor,
    accumulator_state: Option<&AccumulatorState>,
    window_length: i64,
    tile_cfg: Option<&TileConfig>,
) -> (Vec<ScalarValue>, Option<AccumulatorState>) {
    let mut acc = create_window_aggregator(window_expr);
    let (prev, seed_tiles) = match (accumulator_state, prev) {
        (Some(state), Some(prev_c)) => {
            merge_accumulator_state(acc.as_mut(), state);
            (Some(prev_c), false)
        }
        _ => (prev, true),
    };
    slide_each_row(
        window_expr,
        view,
        acc,
        prev,
        advance_to,
        window_length,
        tile_cfg,
        seed_tiles,
    )
}

/// WRO one-shot slide to request time.
pub(super) fn slide_to_point(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    prev: Cursor,
    target_ts: i64,
    accumulator_state: &AccumulatorState,
    window_length: i64,
    tile_cfg: Option<&TileConfig>,
    request_row: Option<&VirtualPoint>,
) -> ScalarValue {
    let mut acc = create_window_aggregator(window_expr);
    merge_accumulator_state(acc.as_mut(), accumulator_state);

    let old_start = prev.ts.saturating_sub(window_length);
    let new_start = target_ts.saturating_sub(window_length);
    if new_start > old_start {
        retract_rows(window_expr, view, &mut acc, old_start, new_start, tile_cfg);
    }
    add_rows_after(
        window_expr,
        view,
        acc.as_mut(),
        prev,
        Cursor::new(target_ts, u64::MAX),
    );
    apply_request_row(acc.as_mut(), request_row);
    acc.evaluate().expect("eval")
}

/// Same request ts as `prev`: no leave/add from store; optional request-row args only.
pub(super) fn slide_same_ts(
    window_expr: &Arc<dyn WindowExpr>,
    accumulator_state: &AccumulatorState,
    request_row: Option<&VirtualPoint>,
) -> ScalarValue {
    let mut acc = create_window_aggregator(window_expr);
    merge_accumulator_state(acc.as_mut(), accumulator_state);
    apply_request_row(acc.as_mut(), request_row);
    acc.evaluate().expect("eval")
}

/// Each-row leave+add loop. Optional cold tile seed for the first emit.
fn slide_each_row(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    mut acc: Box<dyn Accumulator>,
    prev: Option<Cursor>,
    advance_to: Cursor,
    window_length: i64,
    tile_cfg: Option<&TileConfig>,
    seed_tiles: bool,
) -> (Vec<ScalarValue>, Option<AccumulatorState>) {
    let Some(mut update_idx) = view.nav.first_update_idx(prev) else {
        return (vec![], Some(acc.state().expect("state")));
    };
    let Some(end_idx) = view.nav.seek_le(advance_to) else {
        return (vec![], Some(acc.state().expect("state")));
    };

    let mut win_start = match prev {
        Some(p) => p.ts.saturating_sub(window_length),
        None => view.nav.cursor(update_idx).ts.saturating_sub(window_length),
    };
    let mut values = Vec::new();

    if seed_tiles {
        if let Some(seeded) = try_tile_seed(
            window_expr,
            view,
            &mut acc,
            update_idx,
            window_length,
            tile_cfg,
        ) {
            win_start = seeded.win_start;
            values.push(seeded.value);
            if update_idx == end_idx {
                return (values, Some(acc.state().expect("state")));
            }
            update_idx = view.nav.next(update_idx).expect("next");
        }
    }

    loop {
        let update_c = view.nav.cursor(update_idx);
        let new_start = update_c.ts.saturating_sub(window_length);
        if new_start > win_start {
            retract_rows(window_expr, view, &mut acc, win_start, new_start, tile_cfg);
            win_start = new_start;
        }
        update_row(window_expr, &view.nav, update_idx, acc.as_mut());
        values.push(acc.evaluate().expect("eval"));
        if update_idx == end_idx {
            break;
        }
        update_idx = view.nav.next(update_idx).expect("next");
    }
    (values, Some(acc.state().expect("state")))
}

struct TileSeed {
    win_start: i64,
    value: ScalarValue,
}

fn try_tile_seed(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    acc: &mut Box<dyn Accumulator>,
    update_idx: RowIdx,
    window_length: i64,
    tile_cfg: Option<&TileConfig>,
) -> Option<TileSeed> {
    let config = tile_cfg?;
    if !window_supports_tile_slide(window_expr) {
        return None;
    }
    let min_gran_ms = config.min_granularity().to_millis();
    let first_c = view.nav.cursor(update_idx);
    let win_start = first_c.ts.saturating_sub(window_length);
    if first_c.ts.saturating_sub(win_start) < min_gran_ms {
        return None;
    }
    let plan = plan_coverage(
        config,
        Cursor::new(win_start, 0),
        Cursor::new(first_c.ts.saturating_add(1), 0),
    );
    if !update_acc_from_plan(window_expr, view, acc.as_mut(), &plan, Some(update_idx)) {
        return None;
    }
    Some(TileSeed {
        win_start,
        value: acc.evaluate().expect("eval"),
    })
}

fn add_rows_after(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    acc: &mut dyn Accumulator,
    prev: Cursor,
    target: Cursor,
) {
    let Some(mut i) = view.nav.first_update_idx(Some(prev)) else {
        return;
    };
    loop {
        let c = view.nav.cursor(i);
        if c > target {
            break;
        }
        update_row(window_expr, &view.nav, i, acc);
        match view.nav.next(i) {
            Some(n) => i = n,
            None => break,
        }
    }
}

fn retract_rows(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    acc: &mut Box<dyn Accumulator>,
    old_start: i64,
    new_start: i64,
    tile_cfg: Option<&TileConfig>,
) {
    let leave_span = new_start.saturating_sub(old_start);
    if let Some(config) = tile_cfg {
        if window_supports_tile_slide(window_expr)
            && leave_span >= config.min_granularity().to_millis()
        {
            let plan = plan_time_range(config, old_start, new_start);
            if retract_acc_from_plan(window_expr, view, acc, &plan) {
                return;
            }
        }
    }
    let mut idx = view.nav.seek_ts_ge(old_start);
    while let Some(ridx) = idx {
        if view.nav.cursor(ridx).ts >= new_start {
            break;
        }
        retract_row(window_expr, &view.nav, ridx, acc.as_mut());
        idx = view.nav.next(ridx);
    }
}
