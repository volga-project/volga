//! WRO-only: gap-only (or full raw) load per request point.

use std::sync::Arc;

use anyhow::Result;

use crate::common::Key;
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::state::tile::{merge_tile_runs, project_tiles, CoveragePlan, Tile};
use crate::runtime::operators::window::store::event_store::StoredRow;
use crate::runtime::operators::window::store::WindowStateStore;
use crate::runtime::operators::window::window_operator_state::WindowId;

use super::super::primitives::{load, plan_gap_only_ends, LoadPlan, RawScanRun, WindowView};

/// Per-point rebuild load: window view + coverage threaded into eval.
pub(super) struct PointRebuildLoad {
    pub view: WindowView,
    pub coverage: Option<CoveragePlan>,
}

/// Gap-only (or full raw) per point; shared tile union.
///
/// Exclude does **not** affect load bounds — window always includes store through `T`.
/// Exclude only skips applying request args at eval time.
pub(super) async fn load_rebuild_points(
    store: &WindowStateStore,
    key: &Key,
    window_id: WindowId,
    window_expr: &Arc<dyn datafusion::physical_plan::WindowExpr>,
    point_ts: &[i64],
    wl: i64,
    tile_cfg: Option<&crate::runtime::operators::window::state::tile::TileConfig>,
) -> Result<Vec<PointRebuildLoad>> {
    let ends: Vec<(i64, i64)> = point_ts
        .iter()
        .map(|&ts| (ts.saturating_sub(wl), ts))
        .collect();

    let (load_plan, coverages) = if let Some(tc) = tile_cfg {
        let (union_plan, covs) = plan_gap_only_ends(tc, ends.iter().copied());
        (
            LoadPlan {
                raw_runs: union_plan.raw_runs,
                tile_runs: merge_tile_runs(union_plan.tile_runs),
            },
            covs.into_iter().map(Some).collect::<Vec<_>>(),
        )
    } else {
        let plans: Vec<_> = ends
            .iter()
            .map(|&(win_start, ts)| LoadPlan {
                raw_runs: vec![RawScanRun {
                    from: Cursor::new(win_start, 0),
                    to: Cursor::new(ts, u64::MAX),
                }],
                tile_runs: vec![],
            })
            .collect();
        (LoadPlan::union(plans), vec![None; point_ts.len()])
    };

    let batch = load(store, key, load_plan).await?;
    let tiles: Arc<[Tile]> = if tile_cfg.is_some() {
        project_tiles(batch.tile_map(), window_id).into()
    } else {
        Arc::from([])
    };

    Ok(ends
        .iter()
        .zip(coverages)
        .map(|(&(win_start, ts), cov)| {
            let rows: Vec<StoredRow> = batch
                .rows()
                .iter()
                .filter(|r| match &cov {
                    Some(cov) => cov
                        .raw_gaps
                        .iter()
                        .any(|g| r.cursor >= g.from && r.cursor < g.to),
                    None => {
                        let from = Cursor::new(win_start, 0);
                        let to = Cursor::new(ts, u64::MAX);
                        r.cursor >= from && r.cursor <= to
                    }
                })
                .cloned()
                .collect();
            PointRebuildLoad {
                view: WindowView::from_rows(rows, Arc::clone(&tiles), window_expr),
                coverage: cov,
            }
        })
        .collect())
}
