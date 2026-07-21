//! Per-window load planning for advance: exact needs by agg type, then union.
//!
//! ## Warm vs cold (retractable only)
//!
//! - **Cold**: no slide state yet — `prev` is missing and/or no accumulator in meta.
//!   Must rebuild the window → load full `[prev−wl, effective]` (`plan_full_coverage`).
//! - **Warm**: have `prev` and an accumulator in meta. Slide only → load leave band +
//!   add band (`plan_retractable_warm`); rows still inside the window stay in the acc
//!   and are not reloaded.
//!
//! Plain aggs always use full coverage (no retractable slide).
//!
//! Retractable warm slide (acc in meta):
//! - **Retract delta** `[prev−wl, effective−wl)`: tiles where possible + raw only for
//!   planner edge gaps (or the whole band if tile-slide is off).
//! - **Add delta** `(prev, effective]`: raw only — each emit adds that row; no tile-add.
//! - Rows still inside the window (between leave-until and prev) stay in the acc and
//!   are **not** loaded.
//!
//! Plain / retractable cold: full `[prev−wl, effective]` coverage (recompute / seed).

use crate::runtime::operators::window::aggregates::window_supports_tile_slide;
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::frame_utils::get_window_length_ms;
use crate::runtime::operators::window::shared::WindowConfig;
use crate::runtime::operators::window::state::tile::{plan_coverage, TileScanRun};
use crate::runtime::operators::window::AggregatorType;

/// Inclusive EventStore scan segment (`from <= cursor <= to`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RawScanRun {
    pub from: Cursor,
    pub to: Cursor,
}

/// Raw + tile runs to load from the store (one window or union of windows).
#[derive(Debug, Default)]
pub(super) struct LoadPlan {
    pub raw_runs: Vec<RawScanRun>,
    pub tile_runs: Vec<TileScanRun>,
}

impl LoadPlan {
    /// Merge plans; coalesces overlapping/adjacent raw runs.
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
}

/// Plan store reads for one window for advance `(prev, effective]`.
pub(super) fn plan_window_load(
    cfg: &WindowConfig,
    prev: Option<Cursor>,
    effective: Cursor,
    has_accumulator: bool,
) -> LoadPlan {
    let wl = get_window_length_ms(cfg.window_expr.get_window_frame());
    let win_start = prev
        .map(|p| p.ts.saturating_sub(wl))
        .unwrap_or(i64::MIN);

    match cfg.aggregator_type {
        AggregatorType::PlainAccumulator => plan_full_coverage(cfg, win_start, effective),
        AggregatorType::RetractableAccumulator => {
            // Cold: no prev and/or no acc → full rebuild. Warm: slide leave+add only.
            let Some(prev_c) = prev else {
                return plan_full_coverage(cfg, win_start, effective);
            };
            if !has_accumulator {
                return plan_full_coverage(cfg, win_start, effective);
            }
            plan_retractable_warm(cfg, prev_c, effective, wl)
        }
    }
}

/// Plain / retractable **cold**: tiles for the span plus **full raw** `[win_start, effective]`.
///
/// Plain re-plans per emit; each emit can need different edge raw gaps inside the
/// span, so gap-only prefetch from one outer plan is not safe. Full raw is correct;
/// tiles still accelerate the interior at eval time.
fn plan_full_coverage(cfg: &WindowConfig, win_start: i64, effective: Cursor) -> LoadPlan {
    let mut tile_runs = Vec::new();
    if let Some(tile_cfg) = &cfg.tiling {
        tile_runs = plan_coverage(
            tile_cfg,
            Cursor::new(win_start, 0),
            Cursor::new(effective.ts.saturating_add(1), 0),
        )
        .tile_runs;
    }
    LoadPlan {
        raw_runs: vec![raw_time_to(effective, win_start)],
        tile_runs,
    }
}

/// Retractable **warm**: leave delta (tiles + gap raw) ∪ add delta (raw emits).
fn plan_retractable_warm(
    cfg: &WindowConfig,
    prev: Cursor,
    effective: Cursor,
    wl: i64,
) -> LoadPlan {
    let win_start = prev.ts.saturating_sub(wl);
    let leave_until = effective.ts.saturating_sub(wl);

    let mut raw_runs = Vec::new();
    let mut tile_runs = Vec::new();

    // Add / emit delta: new triggering rows only.
    raw_runs.push(RawScanRun {
        from: cursor_next(prev),
        to: effective,
    });

    let use_tiles = cfg.tiling.is_some() && window_supports_tile_slide(&cfg.window_expr);

    if leave_until > win_start {
        if use_tiles {
            let tile_cfg = cfg.tiling.as_ref().unwrap();
            let plan = plan_coverage(
                tile_cfg,
                Cursor::new(win_start, 0),
                Cursor::new(leave_until, 0),
            );
            tile_runs = plan.tile_runs;
            for gap in plan.raw_gaps {
                if let Some(run) = gap_to_raw_run(gap.from, gap.to) {
                    raw_runs.push(run);
                }
            }
        } else {
            // Row-wise retract: need the whole leave band.
            raw_runs.push(RawScanRun {
                from: Cursor::new(win_start, 0),
                to: Cursor::new(leave_until.saturating_sub(1), u64::MAX),
            });
        }
    }

    LoadPlan {
        raw_runs,
        tile_runs,
    }
}

fn raw_time_to(effective: Cursor, win_start: i64) -> RawScanRun {
    RawScanRun {
        from: Cursor::new(win_start, 0),
        to: effective,
    }
}

fn cursor_next(c: Cursor) -> Cursor {
    if c.seq_no < u64::MAX {
        Cursor::new(c.ts, c.seq_no + 1)
    } else {
        Cursor::new(c.ts.saturating_add(1), 0)
    }
}

/// `RawGap.to` is exclusive; EventStore scan is inclusive.
fn gap_to_raw_run(from: Cursor, to_exclusive: Cursor) -> Option<RawScanRun> {
    if to_exclusive <= from {
        return None;
    }
    let to = if to_exclusive.seq_no == 0 {
        Cursor::new(to_exclusive.ts.saturating_sub(1), u64::MAX)
    } else {
        Cursor::new(to_exclusive.ts, to_exclusive.seq_no - 1)
    };
    if to < from {
        return None;
    }
    Some(RawScanRun { from, to })
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
            // Overlap or adjacent (last.to + 1 >= run.from).
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::operators::window::state::tile::TimeGranularity;

    #[test]
    fn coalesce_merges_overlapping_raw() {
        let merged = coalesce_raw_runs(vec![
            RawScanRun {
                from: Cursor::new(0, 0),
                to: Cursor::new(10, 0),
            },
            RawScanRun {
                from: Cursor::new(5, 0),
                to: Cursor::new(20, 0),
            },
            RawScanRun {
                from: Cursor::new(100, 0),
                to: Cursor::new(110, 0),
            },
        ]);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].to, Cursor::new(20, 0));
        assert_eq!(merged[1].from, Cursor::new(100, 0));
    }

    #[test]
    fn load_plan_union_concat_tiles() {
        let u = LoadPlan::union([
            LoadPlan {
                raw_runs: vec![RawScanRun {
                    from: Cursor::new(50, 0),
                    to: Cursor::new(100, 0),
                }],
                tile_runs: vec![TileScanRun {
                    granularity: TimeGranularity::Minutes(1),
                    start_ts: 0,
                    end_ts_exclusive: 60_000,
                }],
            },
            LoadPlan {
                raw_runs: vec![RawScanRun {
                    from: Cursor::new(0, 0),
                    to: Cursor::new(60, 0),
                }],
                tile_runs: vec![],
            },
        ]);
        assert_eq!(u.raw_runs.len(), 1);
        assert_eq!(u.raw_runs[0].from, Cursor::new(0, 0));
        assert_eq!(u.raw_runs[0].to, Cursor::new(100, 0));
        assert_eq!(u.tile_runs.len(), 1);
    }
}
