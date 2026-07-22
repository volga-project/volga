//! WO multi-emit producers (plain rebuild / retractable leave+add).

use std::sync::Arc;

use datafusion::logical_expr::Accumulator;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggregates::{
    create_window_aggregator, merge_accumulator_state, window_supports_tile_slide,
};
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::frame_utils::get_window_length_ms;
use crate::runtime::operators::window::state::tile::{plan_coverage, TileConfig};
use crate::runtime::operators::window::store::row_nav::RowIdx;
use crate::runtime::operators::window::window_operator_state::AccumulatorState;

use super::super::primitives::{
    apply_row, coverage_end_exclusive, retract_from_coverage, retract_row, seed_from_coverage,
    WindowView,
};
use super::super::strategies::eval_rebuild;

/// Plain: rebuild every emit in `(prev, advance_to]`.
pub(super) fn produce_plain_range(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    prev: Option<Cursor>,
    advance_to: Cursor,
    tile_cfg: Option<&TileConfig>,
) -> (Vec<ScalarValue>, Option<Cursor>, Option<AccumulatorState>) {
    let wl = get_window_length_ms(window_expr.get_window_frame());
    let Some(mut idx) = view.nav.first_update_idx(prev) else {
        return (vec![], None, None);
    };
    let Some(end) = view.nav.seek_le(advance_to) else {
        return (vec![], None, None);
    };

    let mut values = Vec::new();
    let mut last;
    loop {
        let end_c = view.nav.cursor(idx);
        let start_ts = end_c.ts.saturating_sub(wl);
        // Plan once per emit and pass through — no re-plan inside eval_rebuild.
        let coverage = tile_cfg.map(|c| {
            plan_coverage(
                c,
                Cursor::new(start_ts, 0),
                coverage_end_exclusive(end_c.ts),
            )
        });
        values.push(eval_rebuild(
            window_expr,
            view,
            start_ts,
            Some(idx),
            coverage.as_ref(),
            None,
        ));
        last = Some(end_c);
        if idx == end {
            break;
        }
        idx = view.nav.next(idx).expect("end");
    }
    (values, last, None)
}

/// Retractable: seed (cold) then leave+add per emit; persist acc.
///
/// Per-emit leave re-plans coverage (different band each emit); load already has
/// the full leave span — re-plan ⊂ loaded (else row-retract fallback).
pub(super) fn produce_retractable_range(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    prev: Option<Cursor>,
    advance_to: Cursor,
    accumulator_state: Option<&AccumulatorState>,
    window_length: i64,
    tile_cfg: Option<&TileConfig>,
) -> (Vec<ScalarValue>, Option<Cursor>, Option<AccumulatorState>) {
    let mut acc = create_window_aggregator(window_expr);
    if let Some(state) = accumulator_state {
        merge_accumulator_state(acc.as_mut(), state);
    }

    let Some(mut update_idx) = view.nav.first_update_idx(prev) else {
        return (vec![], None, Some(acc.state().expect("state")));
    };
    let Some(end_idx) = view.nav.seek_le(advance_to) else {
        return (vec![], None, Some(acc.state().expect("state")));
    };

    let mut retract_idx = initial_retract_idx(view, prev, window_length);
    let use_tile_slide = tile_cfg.is_some() && window_supports_tile_slide(window_expr);
    let min_gran_ms = tile_cfg
        .map(|c| c.min_granularity().to_millis())
        .unwrap_or(i64::MAX);

    let mut values = Vec::new();

    if accumulator_state.is_none() && use_tile_slide {
        if let Some(config) = tile_cfg {
            let first_c = view.nav.cursor(update_idx);
            let win_start = first_c.ts.saturating_sub(window_length);
            let plan = plan_coverage(
                config,
                Cursor::new(win_start, 0),
                Cursor::new(first_c.ts.saturating_add(1), 0),
            );
            if first_c.ts.saturating_sub(win_start) >= min_gran_ms
                && seed_from_coverage(window_expr, view, acc.as_mut(), &plan, update_idx)
            {
                retract_idx = view.nav.seek_ts_ge(win_start);
                values.push(acc.evaluate().expect("eval"));
                if update_idx == end_idx {
                    return (
                        values,
                        Some(first_c),
                        Some(acc.state().expect("state")),
                    );
                }
                update_idx = view.nav.next(update_idx).expect("next");
            }
        }
    }

    let mut last;
    loop {
        let update_c = view.nav.cursor(update_idx);
        let new_start_ts = update_c.ts.saturating_sub(window_length);

        retract_idx = retract_leaving(
            window_expr,
            view,
            &mut acc,
            retract_idx,
            new_start_ts,
            tile_cfg,
            use_tile_slide,
            min_gran_ms,
        );
        apply_row(window_expr, &view.nav, update_idx, acc.as_mut());
        values.push(acc.evaluate().expect("eval"));
        last = Some(update_c);

        if update_idx == end_idx {
            break;
        }
        update_idx = view.nav.next(update_idx).expect("next");
    }

    (values, last, Some(acc.state().expect("state")))
}

fn initial_retract_idx(
    view: &WindowView,
    prev: Option<Cursor>,
    window_length: i64,
) -> Option<RowIdx> {
    match prev {
        None => view.nav.first_idx(),
        Some(prev_c) => view.nav.seek_ts_ge(prev_c.ts.saturating_sub(window_length)),
    }
}

/// Retract leave band for one emit. Re-plans coverage per emit (see module docs).
fn retract_leaving(
    window_expr: &Arc<dyn WindowExpr>,
    view: &WindowView,
    acc: &mut Box<dyn Accumulator>,
    mut retract_idx: Option<RowIdx>,
    new_start_ts: i64,
    tile_cfg: Option<&TileConfig>,
    use_tile_slide: bool,
    min_gran_ms: i64,
) -> Option<RowIdx> {
    let Some(r0) = retract_idx else {
        return None;
    };
    let leave_from_ts = view.nav.cursor(r0).ts;
    if leave_from_ts >= new_start_ts {
        return retract_idx;
    }

    let leave_span = new_start_ts.saturating_sub(leave_from_ts);
    if use_tile_slide && leave_span >= min_gran_ms {
        if let Some(config) = tile_cfg {
            let plan = plan_coverage(
                config,
                Cursor::new(leave_from_ts, 0),
                Cursor::new(new_start_ts, 0),
            );
            if retract_from_coverage(window_expr, view, acc, &plan) {
                return view.nav.seek_ts_ge(new_start_ts);
            }
        }
    }

    while let Some(ridx) = retract_idx {
        if view.nav.cursor(ridx).ts >= new_start_ts {
            break;
        }
        retract_row(window_expr, &view.nav, ridx, acc.as_mut());
        retract_idx = view.nav.next(ridx);
    }
    retract_idx
}
