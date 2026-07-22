//! WRO point-lookup entrypoint.
//!
//! ```text
//! All plain                         → no meta; rebuild each point
//! Retractable + can slide from meta → slide (T == P ⇒ acc [+ request row]; else leave/add)
//! Otherwise                         → rebuild
//! ```
//!
//! Slide when: acc present, `T >= processed_pos`, and `T − wl <= processed_pos`
//! (old window not fully replaced).
//!
//! ## Exclude / include
//!
//! Window bounds always include store through request `T`.
//! - **Include** (`!exclude`): also apply request args (virtual value)
//! - **Exclude**: lookup time only — no request args
//!
//! Read-only: never writes meta.

use std::collections::BTreeMap;

use anyhow::Result;
use datafusion::scalar::ScalarValue;

use crate::common::Key;
use crate::runtime::operators::window::aggregates::VirtualPoint;
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::frame_utils::{get_window_length_ms, require_range_frame};
use crate::runtime::operators::window::config::WindowConfig;
use crate::runtime::operators::window::state::tile::TileConfig;
use crate::runtime::operators::window::store::meta_store::KeyState;
use crate::runtime::operators::window::store::WindowStateStore;
use crate::runtime::operators::window::window_operator_state::{AccumulatorState, WindowId};
use crate::runtime::operators::window::AggregatorType;

use super::super::primitives::{load_window, plan_slide, WindowView};
use super::super::strategies::{eval_rebuild, eval_slide_as_of};
use super::load::{load_rebuild_points, PointRebuildLoad};

pub async fn evaluate_range_points(
    store: &WindowStateStore,
    key: &Key,
    window_configs: &BTreeMap<WindowId, WindowConfig>,
    points: &[VirtualPoint],
    exclude_current_row: bool,
) -> Result<Vec<Vec<ScalarValue>>> {
    let any_retractable = window_configs
        .values()
        .any(|c| c.aggregator_type == AggregatorType::RetractableAccumulator);
    let key_state = if any_retractable {
        Some(store.meta.get_key(key).await?)
    } else {
        None
    };

    let mut by_window = Vec::new();
    for (window_id, cfg) in window_configs {
        require_range_frame(cfg.window_expr.get_window_frame());
        let wl = get_window_length_ms(cfg.window_expr.get_window_frame());
        let exclude = cfg.exclude_current_row.unwrap_or(exclude_current_row);
        let tile_cfg = cfg.tiling.as_ref();

        let vals = match cfg.aggregator_type {
            AggregatorType::PlainAccumulator => {
                eval_window_rebuild(store, key, *window_id, cfg, points, exclude, wl, tile_cfg)
                    .await?
            }
            AggregatorType::RetractableAccumulator => {
                eval_window_retractable(
                    store,
                    key,
                    *window_id,
                    cfg,
                    points,
                    exclude,
                    wl,
                    tile_cfg,
                    key_state.as_ref(),
                )
                .await?
            }
        };
        by_window.push(vals);
    }
    Ok(by_window)
}

async fn eval_window_rebuild(
    store: &WindowStateStore,
    key: &Key,
    window_id: WindowId,
    cfg: &WindowConfig,
    points: &[VirtualPoint],
    exclude: bool,
    wl: i64,
    tile_cfg: Option<&TileConfig>,
) -> Result<Vec<ScalarValue>> {
    let point_ts: Vec<i64> = points.iter().map(|p| p.ts).collect();
    let per_point = load_rebuild_points(
        store,
        key,
        window_id,
        &cfg.window_expr,
        &point_ts,
        wl,
        tile_cfg,
    )
    .await?;
    Ok(points
        .iter()
        .zip(per_point.iter())
        .map(|(p, loaded)| eval_rebuild_point(cfg, loaded, p, exclude, wl))
        .collect())
}

async fn eval_window_retractable(
    store: &WindowStateStore,
    key: &Key,
    window_id: WindowId,
    cfg: &WindowConfig,
    points: &[VirtualPoint],
    exclude: bool,
    wl: i64,
    tile_cfg: Option<&TileConfig>,
    key_state: Option<&KeyState>,
) -> Result<Vec<ScalarValue>> {
    let prev = key_state.and_then(|s| s.processed_pos);
    let acc = key_state.and_then(|s| s.accumulators.get(&window_id));

    let mut vals = Vec::with_capacity(points.len());
    for p in points {
        // Include → fold virtual value; exclude → bounds only (store through T).
        let request_row = (!exclude).then_some(p);

        match slide_base(prev, acc, p.ts, wl) {
            Some((prev_c, acc_state)) if p.ts == prev_c.ts => {
                vals.push(eval_slide_as_of(
                    &cfg.window_expr,
                    acc_state,
                    prev_c,
                    p.ts,
                    &WindowView::empty(&cfg.window_expr),
                    wl,
                    tile_cfg,
                    None,
                    request_row,
                ));
            }
            Some((prev_c, acc_state)) => {
                vals.push(
                    slide_point(
                        store,
                        key,
                        window_id,
                        cfg,
                        p,
                        prev_c,
                        acc_state,
                        wl,
                        tile_cfg,
                        request_row,
                    )
                    .await?,
                );
            }
            None => {
                vals.push(
                    rebuild_point(store, key, window_id, cfg, p, exclude, wl, tile_cfg).await?,
                );
            }
        }
    }
    Ok(vals)
}

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
    if request_ts.saturating_sub(wl) > prev.ts {
        return None;
    }
    Some((prev, acc))
}

async fn slide_point(
    store: &WindowStateStore,
    key: &Key,
    window_id: WindowId,
    cfg: &WindowConfig,
    point: &VirtualPoint,
    prev: Cursor,
    acc_state: &AccumulatorState,
    wl: i64,
    tile_cfg: Option<&TileConfig>,
    request_row: Option<&VirtualPoint>,
) -> Result<ScalarValue> {
    let (plan, leave_coverage) = plan_slide(cfg, prev, Cursor::new(point.ts, u64::MAX), wl);
    let view = load_window(store, key, plan, window_id, &cfg.window_expr).await?;
    Ok(eval_slide_as_of(
        &cfg.window_expr,
        acc_state,
        prev,
        point.ts,
        &view,
        wl,
        tile_cfg,
        leave_coverage.as_ref(),
        request_row,
    ))
}

async fn rebuild_point(
    store: &WindowStateStore,
    key: &Key,
    window_id: WindowId,
    cfg: &WindowConfig,
    point: &VirtualPoint,
    exclude: bool,
    wl: i64,
    tile_cfg: Option<&TileConfig>,
) -> Result<ScalarValue> {
    let mut per_point = load_rebuild_points(
        store,
        key,
        window_id,
        &cfg.window_expr,
        &[point.ts],
        wl,
        tile_cfg,
    )
    .await?;
    let loaded = per_point.pop().unwrap_or_else(|| PointRebuildLoad {
        view: WindowView::empty(&cfg.window_expr),
        coverage: None,
    });
    Ok(eval_rebuild_point(cfg, &loaded, point, exclude, wl))
}

fn eval_rebuild_point(
    cfg: &WindowConfig,
    point_load: &PointRebuildLoad,
    point: &VirtualPoint,
    exclude: bool,
    wl: i64,
) -> ScalarValue {
    eval_rebuild(
        &cfg.window_expr,
        &point_load.view,
        point.ts.saturating_sub(wl),
        point_load.view.nav.last_idx(),
        point_load.coverage.as_ref(),
        (!exclude).then_some(point),
    )
}
