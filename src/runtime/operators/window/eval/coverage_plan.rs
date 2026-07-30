use crate::runtime::operators::window::model::{
    Cursor, RawRun, TileRun, TimeGranularity, Timestamp,
};
use crate::runtime::operators::window::tile::TileConfig;

/// Pure geometry: tile runs + optional raw head/tail.
///
/// At most one raw segment before tiles (`raw_head`) and one after (`raw_tail`).
/// All-raw windows (no tiles) use `raw_head` only. A missing tile inside a run
/// means that interval had no events.
#[derive(Debug, Clone, Default)]
pub struct CoveragePlan {
    pub tile_runs: Vec<TileRun>,
    pub raw_head: Option<RawRun>,
    pub raw_tail: Option<RawRun>,
}

impl CoveragePlan {
    /// Iterate raw edges in order (head then tail).
    pub fn raw_edges(&self) -> impl Iterator<Item = &RawRun> {
        self.raw_head.iter().chain(self.raw_tail.iter())
    }
}

/// Cursor-aware coverage. Safe tiles satisfy `tile_start > start.ts && tile_end <= end.ts`.
pub fn plan_coverage(config: &TileConfig, start: Cursor, end: Cursor) -> CoveragePlan {
    if end < start {
        return CoveragePlan::default();
    }
    if end.ts == start.ts {
        return CoveragePlan {
            tile_runs: vec![],
            raw_head: Some(RawRun {
                from: start,
                to: end,
            }),
            raw_tail: None,
        };
    }
    plan_interior(
        config,
        start,
        end,
        /*first_tile_start_exclusive_of*/ Some(start.ts),
        /*tile_end_max*/ end.ts,
    )
}

/// Half-open `[start_ts, end_ts)`. Tiles fully inside: `tile_start >= start_ts && tile_end <= end_ts`.
pub fn plan_time_range(
    config: &TileConfig,
    start_ts: Timestamp,
    end_ts: Timestamp,
) -> CoveragePlan {
    if start_ts >= end_ts {
        return CoveragePlan::default();
    }
    plan_interior(
        config,
        Cursor::new(start_ts, 0),
        Cursor::new(end_ts, 0),
        None,
        end_ts,
    )
}

/// Geometry only: raw head → coarsest-fitting tiles → raw tail.
///
/// Callers must pass closed bounds (never `i64::MIN`). Cold open loads raw first,
/// then plans tiles from `min_ts − wl`.
fn plan_interior(
    config: &TileConfig,
    start: Cursor,
    end: Cursor,
    first_tile_after_ts: Option<Timestamp>,
    tile_end_max: Timestamp,
) -> CoveragePlan {
    let min_gran = config.min_granularity();

    let mut t = match first_tile_after_ts {
        Some(excl) => next_aligned_strictly_after(min_gran, excl),
        None => first_aligned_at_or_after(min_gran, start.ts),
    };

    let mut tile_runs: Vec<TileRun> = Vec::new();

    let head_to = Cursor::new(t.min(tile_end_max), 0);
    let mut raw_head = if start < head_to {
        Some(RawRun {
            from: start,
            to: head_to,
        })
    } else {
        None
    };

    while t < tile_end_max {
        match pick_coarsest(config, t, tile_end_max) {
            Some((gran, tile_end)) => {
                push_coalesced(&mut tile_runs, gran, t, tile_end);
                t = tile_end;
            }
            None => break,
        }
    }

    if tile_runs.is_empty() {
        return CoveragePlan {
            tile_runs,
            raw_head: Some(RawRun {
                from: start,
                to: end,
            }),
            raw_tail: None,
        };
    }

    let mut raw_tail = if t < end.ts || (t == end.ts && end.seq_no > 0) {
        let from = cursor_at(start, t);
        if from < end {
            Some(RawRun { from, to: end })
        } else {
            None
        }
    } else {
        None
    };

    // Adjacent or overlapping edges become one raw head.
    match (raw_head.take(), raw_tail.take()) {
        (Some(h), Some(tail)) if h.to >= tail.from => {
            let to = if tail.to > h.to { tail.to } else { h.to };
            raw_head = Some(RawRun { from: h.from, to });
        }
        (h, t) => {
            raw_head = h;
            raw_tail = t;
        }
    }

    CoveragePlan {
        tile_runs,
        raw_head,
        raw_tail,
    }
}

/// Coarsest gran aligned at `t` whose tile fits entirely before `tile_end_max`.
fn pick_coarsest(
    config: &TileConfig,
    t: Timestamp,
    tile_end_max: Timestamp,
) -> Option<(TimeGranularity, Timestamp)> {
    for &gran in config.granularities.iter().rev() {
        if gran.start(t) != t {
            continue;
        }
        let tile_end = t + gran.to_millis();
        if tile_end <= tile_end_max {
            return Some((gran, tile_end));
        }
    }
    None
}

fn push_coalesced(
    runs: &mut Vec<TileRun>,
    gran: TimeGranularity,
    start_ts: Timestamp,
    end_ts_exclusive: Timestamp,
) {
    if let Some(last) = runs.last_mut() {
        if last.granularity == gran && last.end_ts_exclusive == start_ts {
            last.end_ts_exclusive = end_ts_exclusive;
            return;
        }
    }
    runs.push(TileRun {
        granularity: gran,
        start_ts,
        end_ts_exclusive,
    });
}

fn first_aligned_at_or_after(gran: TimeGranularity, t: Timestamp) -> Timestamp {
    let a = gran.start(t);
    if a >= t {
        a
    } else {
        gran.next_start(t)
    }
}

fn next_aligned_strictly_after(gran: TimeGranularity, t: Timestamp) -> Timestamp {
    if gran.start(t) == t {
        gran.next_start(t)
    } else {
        first_aligned_at_or_after(gran, t)
    }
}

fn cursor_at(window_start: Cursor, t: Timestamp) -> Cursor {
    if t <= window_start.ts {
        window_start
    } else {
        Cursor::new(t, 0)
    }
}

/// Merge scan runs by granularity, coalescing overlapping / adjacent ranges.
pub fn merge_tile_runs(mut runs: Vec<TileRun>) -> Vec<TileRun> {
    if runs.is_empty() {
        return runs;
    }
    runs.sort_by_key(|r| (r.granularity, r.start_ts, r.end_ts_exclusive));
    let mut out: Vec<TileRun> = Vec::with_capacity(runs.len());
    for run in runs {
        if let Some(last) = out.last_mut() {
            if last.granularity == run.granularity && run.start_ts <= last.end_ts_exclusive {
                last.end_ts_exclusive = last.end_ts_exclusive.max(run.end_ts_exclusive);
                continue;
            }
        }
        out.push(run);
    }
    out
}

/// Merge overlapping or adjacent half-open raw cursor ranges.
pub fn merge_raw_runs(mut runs: Vec<RawRun>) -> Vec<RawRun> {
    if runs.is_empty() {
        return runs;
    }
    runs.sort_by_key(|run| (run.from, run.to));
    let mut out: Vec<RawRun> = Vec::with_capacity(runs.len());
    for run in runs {
        if run.from >= run.to {
            continue;
        }
        if let Some(last) = out.last_mut() {
            if run.from <= last.to {
                last.to = last.to.max(run.to);
                continue;
            }
        }
        out.push(run);
    }
    out
}

#[cfg(test)]
mod plan_tests {
    use super::*;

    fn cfg(grans: Vec<TimeGranularity>) -> TileConfig {
        TileConfig::new(grans).unwrap()
    }

    #[test]
    fn prefers_coarse_without_existence_check() {
        let m1 = TimeGranularity::Minutes(1);
        let m5 = TimeGranularity::Minutes(5);
        let config = cfg(vec![m1, m5]);
        let plan = plan_time_range(&config, 0, 10 * 60_000);
        // Geometry only: two 5-min tiles, coalesced into one run.
        assert_eq!(plan.tile_runs.len(), 1);
        assert_eq!(plan.tile_runs[0].granularity, m5);
        assert_eq!(plan.tile_runs[0].start_ts, 0);
        assert_eq!(plan.tile_runs[0].end_ts_exclusive, 10 * 60_000);
        assert!(plan.raw_head.is_none());
        assert!(plan.raw_tail.is_none());
    }

    #[test]
    fn leftover_shorter_than_min_tile_is_raw_tail() {
        let m5 = TimeGranularity::Minutes(5);
        let config = cfg(vec![m5]);
        let plan = plan_time_range(&config, 0, 7 * 60_000);
        assert_eq!(plan.tile_runs.len(), 1);
        assert_eq!(plan.tile_runs[0].end_ts_exclusive, 5 * 60_000);
        assert!(plan.raw_head.is_none());
        let tail = plan.raw_tail.expect("raw_tail");
        assert_eq!(tail.from.ts, 5 * 60_000);
        assert_eq!(tail.to.ts, 7 * 60_000);
    }

    #[test]
    fn cursor_plan_raw_head_then_tiles() {
        let m1 = TimeGranularity::Minutes(1);
        let config = cfg(vec![m1]);
        let start = Cursor::new(0, 5);
        let end = Cursor::new(180_000, 0);
        let plan = plan_coverage(&config, start, end);

        let head = plan.raw_head.expect("raw_head");
        assert_eq!(head.from, start);
        assert_eq!(head.to.ts, 60_000);
        assert!(plan.raw_tail.is_none());
        assert_eq!(plan.tile_runs.len(), 1);
        assert_eq!(plan.tile_runs[0].start_ts, 60_000);
        assert_eq!(plan.tile_runs[0].end_ts_exclusive, 180_000);
    }

    #[test]
    fn mixes_gran_when_coarse_does_not_fit() {
        let m1 = TimeGranularity::Minutes(1);
        let m5 = TimeGranularity::Minutes(5);
        let config = cfg(vec![m1, m5]);
        // [0, 7min): one 5-min + two 1-min
        let plan = plan_time_range(&config, 0, 7 * 60_000);
        assert_eq!(plan.tile_runs.len(), 2);
        assert_eq!(plan.tile_runs[0].granularity, m5);
        assert_eq!(plan.tile_runs[0].end_ts_exclusive, 5 * 60_000);
        assert_eq!(plan.tile_runs[1].granularity, m1);
        assert_eq!(plan.tile_runs[1].start_ts, 5 * 60_000);
        assert_eq!(plan.tile_runs[1].end_ts_exclusive, 7 * 60_000);
    }

    #[test]
    fn same_ts_coverage_is_single_raw_run() {
        let config = cfg(vec![TimeGranularity::Minutes(1)]);
        let start = Cursor::new(60_000, 1);
        let end = Cursor::new(60_000, 5);
        let plan = plan_coverage(&config, start, end);
        assert!(plan.tile_runs.is_empty());
        assert_eq!(
            plan.raw_head,
            Some(RawRun {
                from: start,
                to: end
            })
        );
        assert!(plan.raw_tail.is_none());
    }

    #[test]
    fn window_shorter_than_min_gran_is_all_raw() {
        let config = cfg(vec![TimeGranularity::Minutes(5)]);
        let plan = plan_time_range(&config, 0, 60_000);
        assert!(plan.tile_runs.is_empty());
        let head = plan.raw_head.expect("raw_head");
        assert_eq!(head.from.ts, 0);
        assert_eq!(head.to.ts, 60_000);
        assert!(plan.raw_tail.is_none());
    }
}
