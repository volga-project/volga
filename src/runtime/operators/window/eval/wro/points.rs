//! WRO point-lookup entrypoint.
//!
//! Same slide | rebuild model as WO:
//! - [`can_slide`] + slideable from meta → [`slide_to_point`] (leave+add, one eval)
//! - else → rebuild (in-memory per point)
//!
//! Estimate envelope without meta → one `load_envelope` → branch.
//! Read-only: never writes meta.

use std::collections::BTreeMap;

use anyhow::Result;
use datafusion::scalar::ScalarValue;

use crate::common::Key;
use crate::runtime::operators::window::aggregates::VirtualPoint;
use crate::runtime::operators::window::config::WindowConfig;
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::frame_utils::{get_window_length_ms, require_range_frame};
use crate::runtime::operators::window::state::tile::TileConfig;
use crate::runtime::operators::window::store::meta_store::KeyState;
use crate::runtime::operators::window::store::{WindowData, WindowStateStore, WindowView};
use crate::runtime::operators::window::window_operator_state::{AccumulatorState, WindowId};

use super::super::can_slide;
use super::super::envelope::{estimate_wro, WroWindowBound};
use super::super::primitives::plan_rebuild_prep;
use super::super::rebuild::eval_rebuild;
use super::super::slide::{slide_same_ts, slide_to_point};

pub async fn evaluate_points(
    store: &WindowStateStore,
    key: &Key,
    window_configs: &BTreeMap<WindowId, WindowConfig>,
    points: &[VirtualPoint],
    bucket_ms: i64,
) -> Result<Vec<Vec<ScalarValue>>> {
    if points.is_empty() {
        return Ok(window_configs.keys().map(|_| Vec::new()).collect());
    }

    let point_ts: Vec<i64> = points.iter().map(|p| p.ts).collect();
    let bounds: Vec<WroWindowBound> = window_configs
        .values()
        .map(|cfg| {
            require_range_frame(cfg.window_expr.get_window_frame());
            WroWindowBound {
                wl: get_window_length_ms(cfg.window_expr.get_window_frame()),
                slide_capable: can_slide(cfg),
            }
        })
        .collect();

    let env = estimate_wro(&point_ts, &bounds).expect("non-empty points + windows");
    let tile_cfgs = collect_tile_cfgs(window_configs);
    let loaded = store
        .load_envelope(key, env.from, env.to, bucket_ms, &tile_cfgs)
        .await?;
    let key_state = loaded.key_state;
    let data = loaded.data;

    let mut by_window: BTreeMap<WindowId, Vec<ScalarValue>> = BTreeMap::new();
    for (window_id, cfg) in window_configs {
        let wl = get_window_length_ms(cfg.window_expr.get_window_frame());
        let exclude = cfg.exclude_current_row.unwrap_or(false);
        let tile_cfg = cfg.tiling.as_ref();
        let vals = if can_slide(cfg) {
            eval_window_slide(
                *window_id,
                cfg,
                points,
                exclude,
                wl,
                tile_cfg,
                &key_state,
                &data,
            )?
        } else {
            eval_window_rebuild(*window_id, cfg, points, exclude, wl, tile_cfg, &data)?
        };
        by_window.insert(*window_id, vals);
    }

    Ok(window_configs
        .keys()
        .map(|wid| by_window.remove(wid).unwrap_or_default())
        .collect())
}

fn eval_window_rebuild(
    window_id: WindowId,
    cfg: &WindowConfig,
    points: &[VirtualPoint],
    exclude: bool,
    wl: i64,
    tile_cfg: Option<&TileConfig>,
    data: &WindowData,
) -> Result<Vec<ScalarValue>> {
    let view = data.for_window(window_id, &cfg.window_expr);
    let idxs: Vec<usize> = (0..points.len()).collect();
    Ok(rebuild_points(cfg, &view, points, &idxs, exclude, wl, tile_cfg))
}

fn eval_window_slide(
    window_id: WindowId,
    cfg: &WindowConfig,
    points: &[VirtualPoint],
    exclude: bool,
    wl: i64,
    tile_cfg: Option<&TileConfig>,
    key_state: &KeyState,
    data: &WindowData,
) -> Result<Vec<ScalarValue>> {
    let prev = key_state.processed_pos;
    let acc = key_state.accumulators.get(&window_id);
    let view = data.for_window(window_id, &cfg.window_expr);

    let mut vals: Vec<Option<ScalarValue>> = vec![None; points.len()];
    let mut rebuild_idxs: Vec<usize> = Vec::new();

    for (i, p) in points.iter().enumerate() {
        let request_row = (!exclude).then_some(p);
        match slide_base(prev, acc, p.ts, wl) {
            Some((prev_c, acc_state)) if p.ts == prev_c.ts => {
                vals[i] = Some(slide_same_ts(&cfg.window_expr, acc_state, request_row));
            }
            Some((prev_c, acc_state)) => {
                vals[i] = Some(slide_to_point(
                    &cfg.window_expr,
                    &view,
                    prev_c,
                    p.ts,
                    acc_state,
                    wl,
                    tile_cfg,
                    request_row,
                ));
            }
            None => rebuild_idxs.push(i),
        }
    }

    if !rebuild_idxs.is_empty() {
        for (i, v) in rebuild_idxs
            .iter()
            .copied()
            .zip(rebuild_points(
                cfg,
                &view,
                points,
                &rebuild_idxs,
                exclude,
                wl,
                tile_cfg,
            ))
        {
            vals[i] = Some(v);
        }
    }

    Ok(vals.into_iter().map(|v| v.expect("filled")).collect())
}

fn rebuild_points(
    cfg: &WindowConfig,
    view: &WindowView,
    points: &[VirtualPoint],
    idxs: &[usize],
    exclude: bool,
    wl: i64,
    tile_cfg: Option<&TileConfig>,
) -> Vec<ScalarValue> {
    let ends: Vec<Cursor> = idxs
        .iter()
        .map(|&i| Cursor::new(points[i].ts, u64::MAX))
        .collect();
    let units = plan_rebuild_prep(&ends, wl, tile_cfg, None);
    idxs.iter()
        .zip(units.iter())
        .map(|(&i, unit)| {
            eval_rebuild(
                &cfg.window_expr,
                view,
                unit.end.ts.saturating_sub(wl),
                view.nav.seek_le(unit.end),
                unit.coverage.as_ref(),
                (!exclude).then_some(&points[i]),
            )
        })
        .collect()
}

fn collect_tile_cfgs(window_configs: &BTreeMap<WindowId, WindowConfig>) -> Vec<&TileConfig> {
    window_configs
        .values()
        .filter_map(|c| c.tiling.as_ref())
        .collect()
}

/// Meta acc can slide to `request_ts` when in range of `prev`.
fn slide_base<'a>(
    prev: Option<Cursor>,
    acc: Option<&'a AccumulatorState>,
    request_ts: i64,
    wl: i64,
) -> Option<(Cursor, &'a AccumulatorState)> {
    let prev = prev?;
    let acc = acc?;
    if request_ts < prev.ts {
        return None;
    }
    // Fully replaced window (no overlap with acc at prev).
    if request_ts.saturating_sub(wl) > prev.ts {
        return None;
    }
    Some((prev, acc))
}
