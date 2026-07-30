//! Per-result coverage planning and load-run collection.

use crate::runtime::operators::window::model::Cursor;
use crate::runtime::operators::window::model::{RawRun, TileRun};
use crate::runtime::operators::window::tile::TileConfig;

use super::coverage_plan::{plan_coverage, plan_time_range, CoveragePlan};

/// One window result and the exact coverage used to load and evaluate it.
#[derive(Debug, Clone)]
pub(crate) struct EvalPlan {
    pub end: Cursor,
    pub coverage: Option<CoveragePlan>,
}

/// `start_floor`: lower bound on window start (WO cold/`first_ingested`); `None` = `end − wl`.
pub(crate) fn plan_rebuilds(
    ends: &[Cursor],
    wl: i64,
    tile_cfg: Option<&TileConfig>,
    start_floor: Option<i64>,
) -> Vec<EvalPlan> {
    ends.iter()
        .map(|&end| {
            let start = end.ts.saturating_sub(wl);
            let start = start_floor.map_or(start, |floor| start.max(floor));
            let from = Cursor::new(start, 0);
            let to = Cursor::after_timestamp(end.ts);
            EvalPlan {
                end,
                coverage: Some(tile_cfg.map_or_else(
                    || CoveragePlan {
                        tile_runs: Vec::new(),
                        raw_head: Some(RawRun { from, to }),
                        raw_tail: None,
                    },
                    |config| plan_coverage(config, from, to),
                )),
            }
        })
        .collect()
}

pub(crate) fn plan_slides(
    ends: &[Cursor],
    prev: Option<Cursor>,
    wl: i64,
    tile_cfg: Option<&TileConfig>,
) -> Vec<EvalPlan> {
    let Some(first) = ends.first() else {
        return Vec::new();
    };
    let mut window_start = prev.unwrap_or(*first).ts.saturating_sub(wl);
    ends.iter()
        .map(|&end| {
            let new_start = end.ts.saturating_sub(wl);
            let coverage = (new_start > window_start).then(|| {
                let span = new_start.saturating_sub(window_start);
                if let Some(config) =
                    tile_cfg.filter(|config| span >= config.min_granularity().to_millis())
                {
                    plan_time_range(config, window_start, new_start)
                } else {
                    CoveragePlan {
                        tile_runs: Vec::new(),
                        raw_head: Some(RawRun {
                            from: Cursor::new(window_start, 0),
                            to: Cursor::new(new_start, 0),
                        }),
                        raw_tail: None,
                    }
                }
            });
            window_start = new_start;
            EvalPlan { end, coverage }
        })
        .collect()
}

pub(crate) fn append_coverage_runs(
    plans: &[EvalPlan],
    raw_runs: &mut Vec<RawRun>,
    tile_runs: &mut Vec<TileRun>,
) {
    for coverage in plans.iter().filter_map(|plan| plan.coverage.as_ref()) {
        raw_runs.extend(coverage.raw_edges().cloned());
        tile_runs.extend(coverage.tile_runs.iter().cloned());
    }
}
