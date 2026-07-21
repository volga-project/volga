//! Read-only point evaluation for WRO.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use futures::future::{BoxFuture, FutureExt};

use crate::common::Key;
use crate::runtime::operators::window::aggregates::{
    create_window_aggregator, VirtualPoint,
};
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::frame_utils::{get_window_length_ms, require_range_frame};
use crate::runtime::operators::window::shared::WindowConfig;
use crate::runtime::operators::window::state::tile::{
    merge_tile_runs, plan_coverage, project_tiles, Tile, TileConfig, TimeGranularity,
    WindowTiles,
};
use crate::runtime::operators::window::store::event_store::StoredRow;
use crate::runtime::operators::window::store::row_nav::RowNav;
use crate::runtime::operators::window::store::WindowStateStore;
use crate::runtime::operators::window::window_operator_state::WindowId;
use crate::runtime::operators::window::AggregatorType;

use super::row_fold::apply_row;
use super::tiles::try_tiled_eval;

enum LoadPart {
    Tile {
        gran: TimeGranularity,
        items: Vec<(i64, WindowTiles)>,
    },
    Raw {
        point_idx: usize,
        rows: Vec<StoredRow>,
    },
}

pub async fn evaluate_range_points(
    store: &WindowStateStore,
    key: &Key,
    window_configs: &BTreeMap<WindowId, WindowConfig>,
    points: &[VirtualPoint],
    exclude_current_row: bool,
) -> Result<Vec<Vec<ScalarValue>>> {
    let mut by_window = Vec::new();
    for (window_id, cfg) in window_configs {
        require_range_frame(cfg.window_expr.get_window_frame());
        let wl = get_window_length_ms(cfg.window_expr.get_window_frame());
        let exclude = cfg.exclude_current_row.unwrap_or(exclude_current_row);

        let tile_cfg = cfg.tiling.as_ref();
        let tile_runs = tile_cfg.map(|tc| {
            let min_ts = points
                .iter()
                .map(|p| p.ts.saturating_sub(wl))
                .min()
                .unwrap_or(0);
            let max_ts = points.iter().map(|p| p.ts).max().unwrap_or(0);
            plan_coverage(
                tc,
                Cursor::new(min_ts, 0),
                Cursor::new(max_ts.saturating_add(1), 0),
            )
            .tile_runs
        });

        // One join: tile scans + per-point raw scans.
        let mut futs: Vec<BoxFuture<'_, Result<LoadPart>>> = Vec::new();
        if let Some(runs) = &tile_runs {
            for run in merge_tile_runs(runs.clone()) {
                let gran = run.granularity;
                let start_ts = run.start_ts;
                let end_ts = run.end_ts_exclusive;
                futs.push(
                    async move {
                        Ok(LoadPart::Tile {
                            gran,
                            items: store.tiles.scan(key, gran, start_ts, end_ts).await?,
                        })
                    }
                    .boxed(),
                );
            }
        }
        for (point_idx, p) in points.iter().enumerate() {
            let ts = p.ts;
            let from = Cursor::new(ts.saturating_sub(wl), 0);
            let to = Cursor::new(ts, u64::MAX);
            futs.push(
                async move {
                    let mut rows = store.events.scan(key, from, to).await?;
                    if exclude {
                        rows.retain(|r| r.cursor.ts != ts);
                    }
                    Ok(LoadPart::Raw { point_idx, rows })
                }
                .boxed(),
            );
        }
        let parts = futures::future::try_join_all(futs).await?;

        let mut tile_by_key = BTreeMap::new();
        let mut rows_per_point: Vec<Option<Vec<StoredRow>>> = vec![None; points.len()];
        for part in parts {
            match part {
                LoadPart::Tile { gran, items } => {
                    for (tile_start, window_tiles) in items {
                        tile_by_key.insert((gran, tile_start), window_tiles);
                    }
                }
                LoadPart::Raw { point_idx, rows } => {
                    rows_per_point[point_idx] = Some(rows);
                }
            }
        }
        let loaded_tiles = project_tiles(&tile_by_key, *window_id);

        let mut vals = Vec::with_capacity(points.len());
        for (p, rows) in points.iter().zip(rows_per_point) {
            let rows = rows.expect("point rows loaded");
            let v = match cfg.aggregator_type {
                AggregatorType::PlainAccumulator | AggregatorType::RetractableAccumulator => {
                    eval_point(
                        &cfg.window_expr,
                        rows,
                        p,
                        exclude,
                        tile_cfg,
                        &loaded_tiles,
                        wl,
                    )
                }
            };
            vals.push(v);
        }
        by_window.push(vals);
    }
    Ok(by_window)
}

fn eval_point(
    window_expr: &Arc<dyn WindowExpr>,
    rows: Vec<StoredRow>,
    point: &VirtualPoint,
    exclude_current_row: bool,
    tile_cfg: Option<&TileConfig>,
    loaded_tiles: &[Tile],
    wl: i64,
) -> ScalarValue {
    let nav = RowNav::from_stored_with_args(rows, window_expr);

    if nav.is_empty() {
        return eval_virtual_only(window_expr, point, exclude_current_row);
    }

    let start_idx = nav.first_idx().unwrap();
    let end_idx = nav.last_idx().unwrap();
    let start_ts = point.ts.saturating_sub(wl);
    let end_ts = nav.cursor(end_idx).ts;

    if tile_cfg.is_some() && (exclude_current_row || point.args.is_none()) {
        if let Some(config) = tile_cfg {
            if let Some(v) = try_tiled_eval(
                window_expr,
                &nav,
                config,
                loaded_tiles,
                start_ts,
                end_ts,
                end_idx,
            ) {
                return v;
            }
        }
    }

    let mut acc = create_window_aggregator(window_expr);
    let mut i = start_idx;
    loop {
        apply_row(window_expr, &nav, i, acc.as_mut());
        if i == end_idx {
            break;
        }
        i = nav.next(i).expect("next");
    }
    if !exclude_current_row {
        if let Some(args) = &point.args {
            acc.update_batch(args).expect("update virtual");
        }
    }
    acc.evaluate().expect("eval")
}

fn eval_virtual_only(
    window_expr: &Arc<dyn WindowExpr>,
    point: &VirtualPoint,
    exclude_current_row: bool,
) -> ScalarValue {
    if exclude_current_row {
        return ScalarValue::Null;
    }
    let Some(args) = &point.args else {
        return ScalarValue::Null;
    };
    let mut acc = create_window_aggregator(window_expr);
    acc.update_batch(args).expect("update virtual");
    acc.evaluate().expect("eval")
}
