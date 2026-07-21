//! Retractable (sliding) RANGE advance, with optional tile retract/seed.

use std::sync::Arc;

use datafusion::logical_expr::Accumulator;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggregates::{
    create_window_aggregator, merge_accumulator_state, window_supports_tile_slide,
};
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::state::tile::{Tile, TileConfig};
use crate::runtime::operators::window::store::row_nav::{RowIdx, RowNav};
use crate::runtime::operators::window::window_operator_state::AccumulatorState;

use super::row_fold::{apply_row, retract_row};
use super::tiles::{seed_from_tiles, try_tile_retract};

pub(super) fn produce_retractable_range(
    window_expr: &Arc<dyn WindowExpr>,
    nav: &RowNav,
    prev: Option<Cursor>,
    advance_to: Cursor,
    accumulator_state: Option<&AccumulatorState>,
    window_length: i64,
    tile_cfg: Option<&TileConfig>,
    loaded_tiles: &[Tile],
) -> (Vec<ScalarValue>, Option<Cursor>, Option<AccumulatorState>) {
    let mut acc = create_window_aggregator(window_expr);
    if let Some(state) = accumulator_state {
        merge_accumulator_state(acc.as_mut(), state);
    }

    let Some(mut update_idx) = nav.first_update_idx(prev) else {
        return (vec![], None, Some(acc.state().expect("state")));
    };
    let Some(end_idx) = nav.seek_le(advance_to) else {
        return (vec![], None, Some(acc.state().expect("state")));
    };

    let mut retract_idx = initial_retract_idx(nav, prev, window_length);
    let use_tile_slide = tile_cfg.is_some() && window_supports_tile_slide(window_expr);
    let min_gran_ms = tile_cfg
        .map(|c| c.min_granularity().to_millis())
        .unwrap_or(i64::MAX);

    let mut values = Vec::new();

    // Cold start: seed first window from tiles when the range is large enough.
    if accumulator_state.is_none() && use_tile_slide {
        if let Some(config) = tile_cfg {
            let first_c = nav.cursor(update_idx);
            let win_start = first_c.ts.saturating_sub(window_length);
            if first_c.ts.saturating_sub(win_start) >= min_gran_ms
                && seed_from_tiles(
                    window_expr,
                    nav,
                    acc.as_mut(),
                    config,
                    loaded_tiles,
                    win_start,
                    update_idx,
                )
            {
                retract_idx = nav.seek_ts_ge(win_start);
                values.push(acc.evaluate().expect("eval"));
                if update_idx == end_idx {
                    return (
                        values,
                        Some(first_c),
                        Some(acc.state().expect("state")),
                    );
                }
                update_idx = nav.next(update_idx).expect("next");
            }
        }
    }

    let mut last;
    loop {
        let update_c = nav.cursor(update_idx);
        let new_start_ts = update_c.ts.saturating_sub(window_length);

        retract_idx = retract_leaving(
            window_expr,
            nav,
            &mut acc,
            retract_idx,
            new_start_ts,
            tile_cfg,
            loaded_tiles,
            use_tile_slide,
            min_gran_ms,
        );
        apply_row(window_expr, nav, update_idx, acc.as_mut());
        values.push(acc.evaluate().expect("eval"));
        last = Some(update_c);

        if update_idx == end_idx {
            break;
        }
        update_idx = nav.next(update_idx).expect("next");
    }

    (values, last, Some(acc.state().expect("state")))
}

fn initial_retract_idx(nav: &RowNav, prev: Option<Cursor>, window_length: i64) -> Option<RowIdx> {
    match prev {
        None => nav.first_idx(),
        Some(prev_c) => nav.seek_ts_ge(prev_c.ts.saturating_sub(window_length)),
    }
}

/// Retract rows with `ts < new_start_ts`. Uses tiles when the leaving span is ≥ min gran.
fn retract_leaving(
    window_expr: &Arc<dyn WindowExpr>,
    nav: &RowNav,
    acc: &mut Box<dyn Accumulator>,
    mut retract_idx: Option<RowIdx>,
    new_start_ts: i64,
    tile_cfg: Option<&TileConfig>,
    loaded_tiles: &[Tile],
    use_tile_slide: bool,
    min_gran_ms: i64,
) -> Option<RowIdx> {
    let Some(r0) = retract_idx else {
        return None;
    };
    let leave_from_ts = nav.cursor(r0).ts;
    if leave_from_ts >= new_start_ts {
        return retract_idx;
    }

    let leave_span = new_start_ts.saturating_sub(leave_from_ts);
    if use_tile_slide && leave_span >= min_gran_ms {
        if let Some(config) = tile_cfg {
            if try_tile_retract(
                window_expr,
                nav,
                acc,
                config,
                loaded_tiles,
                leave_from_ts,
                new_start_ts,
            ) {
                return nav.seek_ts_ge(new_start_ts);
            }
        }
    }

    while let Some(ridx) = retract_idx {
        if nav.cursor(ridx).ts >= new_start_ts {
            break;
        }
        retract_row(window_expr, nav, ridx, acc.as_mut());
        retract_idx = nav.next(ridx);
    }
    retract_idx
}
