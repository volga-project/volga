use std::collections::BTreeMap;

use crate::runtime::operators::window::cursor::Cursor;

use super::granularity::{TileConfig, TimeGranularity, Timestamp};

/// Coalesced TileStore range scan at one granularity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TileScanRun {
    pub granularity: TimeGranularity,
    pub start_ts: Timestamp,
    pub end_ts_exclusive: Timestamp,
}

/// Cursor-bounded raw EventStore segment (window edges only).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawGap {
    pub from: Cursor,
    pub to: Cursor,
}

/// Pure geometry plan: which tile key ranges to scan + raw edges.
///
/// Callers load closed ranges only. A missing KV key inside a scanned range means
/// that bucket had no events (skip / identity). No existence map required.
#[derive(Debug, Clone, Default)]
pub struct CoveragePlan {
    pub tile_runs: Vec<TileScanRun>,
    pub raw_gaps: Vec<RawGap>,
}

/// Cursor-aware coverage. Safe tiles satisfy `tile_start > start.ts && tile_end <= end.ts`.
pub fn plan_coverage(config: &TileConfig, start: Cursor, end: Cursor) -> CoveragePlan {
    if end < start {
        return CoveragePlan::default();
    }
    if end.ts == start.ts {
        return CoveragePlan {
            tile_runs: vec![],
            raw_gaps: vec![RawGap { from: start, to: end }],
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
pub fn plan_time_range(config: &TileConfig, start_ts: Timestamp, end_ts: Timestamp) -> CoveragePlan {
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

    let mut runs: Vec<TileScanRun> = Vec::new();
    let mut gaps: Vec<RawGap> = Vec::new();

    let head_to = Cursor::new(t.min(tile_end_max), 0);
    if start < head_to {
        gaps.push(RawGap {
            from: start,
            to: head_to,
        });
    }

    while t < tile_end_max {
        match pick_coarsest(config, t, tile_end_max) {
            Some((gran, tile_end)) => {
                push_coalesced(&mut runs, gran, t, tile_end);
                t = tile_end;
            }
            None => break,
        }
    }

    if t < end.ts || (t == end.ts && end.seq_no > 0) {
        let from = cursor_at(start, t);
        if from < end {
            gaps.push(RawGap { from, to: end });
        }
    }

    if runs.is_empty() && gaps.is_empty() {
        gaps.push(RawGap {
            from: start,
            to: end,
        });
    }

    CoveragePlan {
        tile_runs: runs,
        raw_gaps: merge_adjacent_gaps(gaps),
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
    runs: &mut Vec<TileScanRun>,
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
    runs.push(TileScanRun {
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

fn merge_adjacent_gaps(mut gaps: Vec<RawGap>) -> Vec<RawGap> {
    if gaps.is_empty() {
        return gaps;
    }
    gaps.sort_by_key(|g| (g.from.ts, g.from.seq_no, g.to.ts, g.to.seq_no));
    let mut out: Vec<RawGap> = Vec::new();
    for g in gaps {
        if g.from >= g.to {
            continue;
        }
        if let Some(last) = out.last_mut() {
            if last.to >= g.from {
                if g.to > last.to {
                    last.to = g.to;
                }
                continue;
            }
        }
        out.push(g);
    }
    out
}

/// Merge scan runs by granularity, coalescing overlapping / adjacent ranges.
pub fn merge_tile_runs(mut runs: Vec<TileScanRun>) -> Vec<TileScanRun> {
    if runs.is_empty() {
        return runs;
    }
    runs.sort_by_key(|r| (r.granularity, r.start_ts, r.end_ts_exclusive));
    let mut out: Vec<TileScanRun> = Vec::with_capacity(runs.len());
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

/// Ingest update plan: one [`TileScanRun`] per `(granularity, tile_start)` touched by `timestamps`.
///
/// Unlike [`plan_coverage`] / [`plan_time_range`] (coarsest tiles for eval), this includes
/// **every** configured granularity so each tile that will be written is loaded.
/// Caller loads via parallel [`TileStore::get`] (or scans) then applies in memory.
pub fn plan_update_runs(
    config: &TileConfig,
    timestamps: impl IntoIterator<Item = Timestamp>,
) -> Vec<TileScanRun> {
    use std::collections::BTreeSet;

    let mut by_gran: BTreeMap<TimeGranularity, BTreeSet<Timestamp>> = BTreeMap::new();
    for ts in timestamps {
        for &gran in &config.granularities {
            by_gran.entry(gran).or_default().insert(gran.start(ts));
        }
    }

    let mut runs = Vec::new();
    for (gran, starts) in by_gran {
        let step = gran.to_millis();
        for start in starts {
            runs.push(TileScanRun {
                granularity: gran,
                start_ts: start,
                end_ts_exclusive: start.saturating_add(step),
            });
        }
    }
    runs
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
        assert!(plan.raw_gaps.is_empty());
    }

    #[test]
    fn leftover_shorter_than_min_tile_is_raw_tail() {
        let m5 = TimeGranularity::Minutes(5);
        let config = cfg(vec![m5]);
        let plan = plan_time_range(&config, 0, 7 * 60_000);
        assert_eq!(plan.tile_runs.len(), 1);
        assert_eq!(plan.tile_runs[0].end_ts_exclusive, 5 * 60_000);
        assert_eq!(plan.raw_gaps.len(), 1);
        assert_eq!(plan.raw_gaps[0].from.ts, 5 * 60_000);
        assert_eq!(plan.raw_gaps[0].to.ts, 7 * 60_000);
    }

    #[test]
    fn cursor_plan_raw_head_then_tiles() {
        let m1 = TimeGranularity::Minutes(1);
        let config = cfg(vec![m1]);
        let start = Cursor::new(0, 5);
        let end = Cursor::new(180_000, 0);
        let plan = plan_coverage(&config, start, end);

        assert_eq!(plan.raw_gaps[0].from, start);
        assert_eq!(plan.raw_gaps[0].to.ts, 60_000);
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
    fn same_ts_coverage_is_single_raw_gap() {
        let config = cfg(vec![TimeGranularity::Minutes(1)]);
        let start = Cursor::new(60_000, 1);
        let end = Cursor::new(60_000, 5);
        let plan = plan_coverage(&config, start, end);
        assert!(plan.tile_runs.is_empty());
        assert_eq!(plan.raw_gaps, vec![RawGap { from: start, to: end }]);
    }

    #[test]
    fn window_shorter_than_min_gran_is_all_raw() {
        let config = cfg(vec![TimeGranularity::Minutes(5)]);
        let plan = plan_time_range(&config, 0, 60_000);
        assert!(plan.tile_runs.is_empty());
        assert_eq!(plan.raw_gaps.len(), 1);
        assert_eq!(plan.raw_gaps[0].from.ts, 0);
        assert_eq!(plan.raw_gaps[0].to.ts, 60_000);
    }

    #[test]
    fn plan_update_runs_one_per_gran_per_bucket() {
        let m1 = TimeGranularity::Minutes(1);
        let m5 = TimeGranularity::Minutes(5);
        let config = cfg(vec![m1, m5]);

        assert!(plan_update_runs(&config, std::iter::empty::<i64>()).is_empty());

        let runs = plan_update_runs(&config, [90_000, 90_000, 6 * 60_000]);
        // Two buckets × two grans; not coalesced like eval coverage.
        assert_eq!(runs.len(), 4);
        assert!(runs.iter().any(|r| r.granularity == m1 && r.start_ts == 60_000));
        assert!(runs.iter().any(|r| r.granularity == m1 && r.start_ts == 6 * 60_000));
        assert!(runs.iter().any(|r| r.granularity == m5 && r.start_ts == 0));
        assert!(runs.iter().any(|r| r.granularity == m5 && r.start_ts == 5 * 60_000));
        for r in &runs {
            assert_eq!(
                r.end_ts_exclusive,
                r.start_ts + r.granularity.to_millis()
            );
        }
    }
}
