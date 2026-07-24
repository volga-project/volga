//! CPU coverage planning for rebuild units (no KV IO).

use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::state::tile::{plan_coverage, CoveragePlan, TileConfig};

/// One eval answer: window end + optional coverage (tiled).
#[derive(Debug, Clone)]
pub(crate) struct EvalUnit {
    pub end: Cursor,
    pub coverage: Option<CoveragePlan>,
}

/// Rebuild prepare from known ends: tiled → per-end coverage; else units without coverage.
///
/// `start_floor`: lower bound on window start (WO cold/`first_ingested`); `None` = `end − wl`.
pub(crate) fn plan_rebuild_prep(
    ends: &[Cursor],
    wl: i64,
    tile_cfg: Option<&TileConfig>,
    start_floor: Option<i64>,
) -> Vec<EvalUnit> {
    if ends.is_empty() {
        return vec![];
    }
    let ranges: Vec<(i64, Cursor)> = ends
        .iter()
        .map(|&end| {
            let start = end.ts.saturating_sub(wl);
            let start = match start_floor {
                Some(f) => start.max(f),
                None => start,
            };
            (start, end)
        })
        .collect();

    if let Some(tc) = tile_cfg {
        ranges
            .iter()
            .map(|(win_start, end)| EvalUnit {
                end: *end,
                coverage: Some(plan_coverage(
                    tc,
                    Cursor::new(*win_start, 0),
                    Cursor::new(end.ts.saturating_add(1), 0),
                )),
            })
            .collect()
    } else {
        ranges
            .iter()
            .map(|(_, end)| EvalUnit {
                end: *end,
                coverage: None,
            })
            .collect()
    }
}
