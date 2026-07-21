//! Plain (non-sliding) RANGE evaluate over a nav span.

use std::sync::Arc;

use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggregates::create_window_aggregator;
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::frame_utils::get_window_length_ms;
use crate::runtime::operators::window::state::tile::{Tile, TileConfig};
use crate::runtime::operators::window::store::row_nav::{RowIdx, RowNav};
use crate::runtime::operators::window::window_operator_state::AccumulatorState;

use super::row_fold::apply_row;
use super::tiles::try_tiled_eval;

pub(super) fn produce_plain_range(
    window_expr: &Arc<dyn WindowExpr>,
    nav: &RowNav,
    prev: Option<Cursor>,
    advance_to: Cursor,
    tile_cfg: Option<&TileConfig>,
    loaded_tiles: &[Tile],
) -> (Vec<ScalarValue>, Option<Cursor>, Option<AccumulatorState>) {
    let wl = get_window_length_ms(window_expr.get_window_frame());
    let Some(mut idx) = nav.first_update_idx(prev) else {
        return (vec![], None, None);
    };
    let Some(end) = nav.seek_le(advance_to) else {
        return (vec![], None, None);
    };

    let mut values = Vec::new();
    let mut last;
    // Per-event emit: each call rebuilds that event's window (tiles or full raw fold).
    // TODO: consecutive emits overlap heavily — measure N×wl cost on large watermarks;
    // explore plan/tile-merge reuse/caching.
    loop {
        let end_c = nav.cursor(idx);
        let start_ts = end_c.ts.saturating_sub(wl);
        values.push(eval_plain_window(
            window_expr,
            nav,
            start_ts,
            idx,
            tile_cfg,
            loaded_tiles,
        ));
        last = Some(end_c);
        if idx == end {
            break;
        }
        idx = nav.next(idx).expect("end");
    }
    (values, last, None)
}

fn eval_plain_window(
    window_expr: &Arc<dyn WindowExpr>,
    nav: &RowNav,
    start_ts: i64,
    end_idx: RowIdx,
    tile_cfg: Option<&TileConfig>,
    loaded_tiles: &[Tile],
) -> ScalarValue {
    let start_idx = nav.seek_ts_ge(start_ts).unwrap_or(RowIdx(0));
    if start_idx > end_idx {
        return ScalarValue::Null;
    }

    if let Some(config) = tile_cfg {
        let end_ts = nav.cursor(end_idx).ts;
        if let Some(v) = try_tiled_eval(
            window_expr,
            nav,
            config,
            loaded_tiles,
            start_ts,
            end_ts,
            end_idx,
        ) {
            return v;
        }
    }

    let mut acc = create_window_aggregator(window_expr);
    let mut i = start_idx;
    loop {
        apply_row(window_expr, nav, i, acc.as_mut());
        if i == end_idx {
            break;
        }
        i = nav.next(i).expect("next");
    }
    acc.evaluate().expect("evaluate")
}
