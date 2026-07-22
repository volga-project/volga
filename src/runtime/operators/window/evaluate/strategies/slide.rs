//! **Slide** — retractable single-shot kernel (shared WO/WRO).
//!
//! Used by:
//! - **WRO + retractable**: as-of request `T` when `T >= processed_pos`
//! - **WO + retractable**: multi-emit via [`crate::runtime::operators::window::evaluate::wo`]
//!
//! Cold retractable (no acc) and past `T` use [`super::rebuild`].
//!
//! WRO threads the leave [`CoveragePlan`] from [`super::super::primitives::plan_slide`]
//! into [`eval_slide_as_of`].

use std::sync::Arc;

use datafusion::logical_expr::Accumulator;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggregates::{
    create_window_aggregator, merge_accumulator_state, window_supports_tile_slide, VirtualPoint,
};
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::state::tile::{plan_coverage, CoveragePlan, TileConfig};
use crate::runtime::operators::window::window_operator_state::AccumulatorState;

use super::super::primitives::{apply_row, retract_from_coverage, retract_row, WindowView};

/// Slide meta acc from `prev` to as-of `target_ts` (+ optional request row).
///
/// `leave_coverage` should be the plan used for load when tiled leave applies.
pub(crate) fn eval_slide_as_of(
    window_expr: &Arc<dyn WindowExpr>,
    accumulator_state: &AccumulatorState,
    prev: Cursor,
    target_ts: i64,
    view: &WindowView,
    window_length: i64,
    tile_cfg: Option<&TileConfig>,
    leave_coverage: Option<&CoveragePlan>,
    request_row: Option<&VirtualPoint>,
) -> ScalarValue {
    let mut acc = create_window_aggregator(window_expr);
    merge_accumulator_state(acc.as_mut(), accumulator_state);

    let old_start = prev.ts.saturating_sub(window_length);
    let new_start = target_ts.saturating_sub(window_length);
    debug_assert!(new_start <= prev.ts, "full window replace should rebuild");

    let use_tile_slide = tile_cfg.is_some() && window_supports_tile_slide(window_expr);
    let min_gran_ms = tile_cfg
        .map(|c| c.min_granularity().to_millis())
        .unwrap_or(i64::MAX);

    if new_start > old_start {
        let leave_span = new_start.saturating_sub(old_start);
        let mut did_tile = false;
        if use_tile_slide && leave_span >= min_gran_ms {
            if let Some(plan) = leave_coverage {
                did_tile = retract_from_coverage(window_expr, view, &mut acc, plan);
            } else if let Some(config) = tile_cfg {
                let plan = plan_coverage(
                    config,
                    Cursor::new(old_start, 0),
                    Cursor::new(new_start, 0),
                );
                did_tile = retract_from_coverage(window_expr, view, &mut acc, &plan);
            }
        }
        if !did_tile {
            retract_rows_before(window_expr, view, &mut acc, new_start);
        }
    }

    if let Some(mut i) = view.nav.first_update_idx(Some(prev)) {
        loop {
            let c = view.nav.cursor(i);
            if c.ts > target_ts {
                break;
            }
            apply_row(window_expr, &view.nav, i, acc.as_mut());
            match view.nav.next(i) {
                Some(n) => i = n,
                None => break,
            }
        }
    }

    if let Some(point) = request_row {
        if let Some(args) = &point.args {
            acc.update_batch(args).expect("update request row");
        }
    }

    acc.evaluate().expect("eval")
}

fn retract_rows_before(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    acc: &mut Box<dyn Accumulator>,
    before_ts: i64,
) {
    let Some(mut i) = view.nav.first_idx() else {
        return;
    };
    loop {
        if view.nav.cursor(i).ts >= before_ts {
            break;
        }
        retract_row(window_expr, &view.nav, i, acc.as_mut());
        match view.nav.next(i) {
            Some(n) => i = n,
            None => break,
        }
    }
}
