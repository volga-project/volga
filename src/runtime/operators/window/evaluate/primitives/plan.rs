//! Shared load planning types and planners (WO + WRO).
//!
//! ## Bounds vocabulary
//!
//! - [`CoveragePlan`] gaps / tile ends: **half-open** `[from, to)`
//! - [`RawScanRun`]: **inclusive** `from..=to` (via [`gap_to_inclusive`])
//! - Window end at time `T`: coverage end [`coverage_end_exclusive`] so store through `T` is included

use std::collections::BTreeMap;

use crate::runtime::operators::window::aggregates::window_supports_tile_slide;
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::config::WindowConfig;
use crate::runtime::operators::window::state::tile::{
    plan_coverage, CoveragePlan, RawGap, TileConfig, TileScanRun, TimeGranularity, WindowTiles,
};

pub(crate) type TileMap = BTreeMap<(TimeGranularity, i64), WindowTiles>;

/// Inclusive EventStore scan (`from <= cursor <= to`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawScanRun {
    pub from: Cursor,
    pub to: Cursor,
}

#[derive(Debug, Default)]
pub(crate) struct LoadPlan {
    pub raw_runs: Vec<RawScanRun>,
    pub tile_runs: Vec<TileScanRun>,
}

impl LoadPlan {
    pub fn union(plans: impl IntoIterator<Item = LoadPlan>) -> Self {
        let mut raw_runs = Vec::new();
        let mut tile_runs = Vec::new();
        for plan in plans {
            raw_runs.extend(plan.raw_runs);
            tile_runs.extend(plan.tile_runs);
        }
        Self {
            raw_runs: coalesce_raw_runs(raw_runs),
            tile_runs,
        }
    }

    pub fn from_coverage(plan: &CoveragePlan) -> Self {
        let mut raw_runs = Vec::new();
        for gap in &plan.raw_gaps {
            if let Some(run) = gap_to_raw_run(gap.from, gap.to) {
                raw_runs.push(run);
            }
        }
        Self {
            raw_runs,
            tile_runs: plan.tile_runs.clone(),
        }
    }
}

/// Half-open coverage end so store rows with `ts == end_ts` are included.
pub(crate) fn coverage_end_exclusive(end_ts: i64) -> Cursor {
    Cursor::new(end_ts.saturating_add(1), 0)
}

/// Gap-only union of coverages for window ends through each `end_ts` (inclusive).
/// Shared by WO plain emit-band planning and WRO rebuild points.
pub(crate) fn plan_gap_only_ends(
    tile_cfg: &TileConfig,
    ends: impl IntoIterator<Item = (i64 /* win_start */, i64 /* end_ts */)>,
) -> (LoadPlan, Vec<CoveragePlan>) {
    let mut raw_runs = Vec::new();
    let mut tile_runs = Vec::new();
    let mut coverages = Vec::new();
    for (win_start, end_ts) in ends {
        let cov = plan_coverage(
            tile_cfg,
            Cursor::new(win_start, 0),
            coverage_end_exclusive(end_ts),
        );
        tile_runs.extend(cov.tile_runs.iter().cloned());
        for gap in &cov.raw_gaps {
            if let Some(run) = gap_to_raw_run(gap.from, gap.to) {
                raw_runs.push(run);
            }
        }
        coverages.push(cov);
    }
    (
        LoadPlan {
            raw_runs: coalesce_raw_runs(raw_runs),
            tile_runs,
        },
        coverages,
    )
}

/// Retractable warm / WRO as-of-`T` when sliding from `prev` toward `effective`.
///
/// Returns `(load_plan, leave_coverage)` — when tiled leave uses coverage, the same
/// plan is returned for apply/retract without re-planning.
pub(crate) fn plan_slide(
    cfg: &WindowConfig,
    prev: Cursor,
    effective: Cursor,
    wl: i64,
) -> (LoadPlan, Option<CoveragePlan>) {
    let win_start = prev.ts.saturating_sub(wl);
    let leave_until = effective.ts.saturating_sub(wl);

    let mut raw_runs = vec![RawScanRun {
        from: cursor_next(prev),
        to: effective,
    }];
    let mut tile_runs = Vec::new();
    let mut leave_coverage = None;

    if leave_until > win_start {
        let use_tiles = cfg.tiling.is_some() && window_supports_tile_slide(&cfg.window_expr);
        if use_tiles {
            let plan = plan_coverage(
                cfg.tiling.as_ref().unwrap(),
                Cursor::new(win_start, 0),
                Cursor::new(leave_until, 0),
            );
            tile_runs = plan.tile_runs.clone();
            for gap in &plan.raw_gaps {
                if let Some(run) = gap_to_raw_run(gap.from, gap.to) {
                    raw_runs.push(run);
                }
            }
            leave_coverage = Some(plan);
        } else {
            raw_runs.push(RawScanRun {
                from: Cursor::new(win_start, 0),
                to: Cursor::new(leave_until.saturating_sub(1), u64::MAX),
            });
        }
    }

    (
        LoadPlan {
            raw_runs,
            tile_runs,
        },
        leave_coverage,
    )
}

/// Inclusive scans for CoveragePlan raw gaps.
pub(crate) fn gap_to_inclusive(gap: RawGap) -> Option<(Cursor, Cursor)> {
    if gap.to <= gap.from {
        return None;
    }
    let to = if gap.to.seq_no == 0 {
        Cursor::new(gap.to.ts.saturating_sub(1), u64::MAX)
    } else {
        Cursor::new(gap.to.ts, gap.to.seq_no - 1)
    };
    if to < gap.from {
        return None;
    }
    Some((gap.from, to))
}

pub(crate) fn gap_to_raw_run(from: Cursor, to_exclusive: Cursor) -> Option<RawScanRun> {
    gap_to_inclusive(RawGap {
        from,
        to: to_exclusive,
    })
    .map(|(from, to)| RawScanRun { from, to })
}

pub(crate) fn cursor_next(c: Cursor) -> Cursor {
    if c.seq_no < u64::MAX {
        Cursor::new(c.ts, c.seq_no + 1)
    } else {
        Cursor::new(c.ts.saturating_add(1), 0)
    }
}

fn coalesce_raw_runs(mut runs: Vec<RawScanRun>) -> Vec<RawScanRun> {
    if runs.is_empty() {
        return runs;
    }
    runs.sort_by(|a, b| a.from.cmp(&b.from).then(a.to.cmp(&b.to)));
    let mut out: Vec<RawScanRun> = Vec::with_capacity(runs.len());
    for run in runs {
        if run.to < run.from {
            continue;
        }
        if let Some(last) = out.last_mut() {
            if run.from <= cursor_next(last.to) {
                if run.to > last.to {
                    last.to = run.to;
                }
                continue;
            }
        }
        out.push(run);
    }
    out
}
