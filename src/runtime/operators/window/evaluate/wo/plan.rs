//! WO-only load planning for watermark advance.

use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::frame_utils::get_window_length_ms;
use crate::runtime::operators::window::config::WindowConfig;
use crate::runtime::operators::window::state::tile::plan_coverage;
use crate::runtime::operators::window::AggregatorType;

use super::super::primitives::{
    coverage_end_exclusive, plan_gap_only_ends, plan_slide, LoadPlan, RawScanRun,
};

/// Which raw/tile ranges to load for one window over `(prev, effective]`.
///
/// `first_ingested`: cold-plan origin from meta (ignored when `prev` is set).
/// `emit_ends`: cursors that will emit (rows in the advance band). Used for plain
/// gap-only union; ignored for warm slide / full retractable rebuild.
pub(super) fn plan_window_load(
    cfg: &WindowConfig,
    prev: Option<Cursor>,
    effective: Cursor,
    has_accumulator: bool,
    first_ingested: Option<Cursor>,
    emit_ends: &[Cursor],
) -> LoadPlan {
    let wl = get_window_length_ms(cfg.window_expr.get_window_frame());
    let win_start = match prev {
        Some(p) => p.ts.saturating_sub(wl),
        None => first_ingested
            .map(|f| f.ts.saturating_sub(wl))
            .unwrap_or(i64::MIN),
    };

    match cfg.aggregator_type {
        AggregatorType::PlainAccumulator => {
            plan_rebuild_plain(cfg, win_start, effective, wl, emit_ends)
        }
        AggregatorType::RetractableAccumulator => {
            let Some(prev_c) = prev else {
                return plan_rebuild_full(cfg, win_start, effective);
            };
            if !has_accumulator {
                return plan_rebuild_full(cfg, win_start, effective);
            }
            plan_slide(cfg, prev_c, effective, wl).0
        }
    }
}

/// Plain: gap-only union of per-emit coverages (caller merges emit-band rows).
fn plan_rebuild_plain(
    cfg: &WindowConfig,
    win_start: i64,
    effective: Cursor,
    wl: i64,
    emit_ends: &[Cursor],
) -> LoadPlan {
    if win_start == i64::MIN {
        return LoadPlan {
            raw_runs: vec![RawScanRun {
                from: Cursor::new(i64::MIN, 0),
                to: effective,
            }],
            tile_runs: vec![],
        };
    }
    let Some(tile_cfg) = &cfg.tiling else {
        return LoadPlan {
            raw_runs: vec![RawScanRun {
                from: Cursor::new(win_start, 0),
                to: effective,
            }],
            tile_runs: vec![],
        };
    };
    if emit_ends.is_empty() {
        return LoadPlan::default();
    }

    let ends = emit_ends.iter().map(|end| {
        let start_ts = end.ts.saturating_sub(wl).max(win_start);
        (start_ts, end.ts)
    });
    plan_gap_only_ends(tile_cfg, ends).0
}

/// Retractable cold / missing acc: full raw under tiles (leave rows during seed+slide).
fn plan_rebuild_full(cfg: &WindowConfig, win_start: i64, effective: Cursor) -> LoadPlan {
    let tile_runs = match (&cfg.tiling, win_start == i64::MIN) {
        (_, true) => vec![],
        (Some(tile_cfg), false) => plan_coverage(
            tile_cfg,
            Cursor::new(win_start, 0),
            coverage_end_exclusive(effective.ts),
        )
        .tile_runs,
        (None, false) => vec![],
    };
    LoadPlan {
        raw_runs: vec![RawScanRun {
            from: Cursor::new(win_start, 0),
            to: effective,
        }],
        tile_runs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::physical_plan::windows::BoundedWindowAggExec;
    use datafusion::prelude::SessionContext;

    use crate::api::planner::{Planner, PlanningContext};
    use crate::runtime::functions::key_by::key_by_function::extract_datafusion_window_exec;
    use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
    use crate::runtime::operators::window::aggregates::get_aggregate_type;
    use crate::runtime::operators::window::config::WindowConfig;
    use crate::runtime::operators::window::state::tile::{TileConfig, TimeGranularity};

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Float64, false),
            Field::new("partition_key", DataType::Utf8, false),
        ]))
    }

    async fn window_cfg(sql: &str, tiling: Option<TileConfig>) -> WindowConfig {
        let ctx = SessionContext::new();
        let mut planner = Planner::new(PlanningContext::new(ctx));
        planner.register_source(
            "test_table".to_string(),
            SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])),
            schema(),
        );
        let exec: Arc<BoundedWindowAggExec> =
            extract_datafusion_window_exec(sql, &mut planner).await;
        let expr = exec.window_expr()[0].clone();
        WindowConfig {
            window_id: 0,
            aggregator_type: get_aggregate_type(&expr),
            window_expr: expr,
            tiling,
            exclude_current_row: None,
        }
    }

    fn tiling_1m() -> TileConfig {
        TileConfig::new(vec![TimeGranularity::Minutes(1)]).unwrap()
    }

    const PREV_TS: i64 = 10 * 60_000;
    const EFF_TS: i64 = 12 * 60_000;
    const WL: i64 = 7 * 60_000;

    fn sum_sql() -> &'static str {
        "SELECT timestamp, value, partition_key, SUM(value) OVER w as s \
         FROM test_table \
         WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp \
           RANGE BETWEEN INTERVAL '7' MINUTE PRECEDING AND CURRENT ROW)"
    }

    fn min_sql() -> &'static str {
        "SELECT timestamp, value, partition_key, MIN(value) OVER w as m \
         FROM test_table \
         WINDOW w AS (PARTITION BY partition_key ORDER BY timestamp \
           RANGE BETWEEN INTERVAL '7' MINUTE PRECEDING AND CURRENT ROW)"
    }

    #[tokio::test]
    async fn rebuild_full_retractable_loads_raw_and_tiles() {
        let cfg = window_cfg(sum_sql(), Some(tiling_1m())).await;
        let prev = Cursor::new(PREV_TS, 0);
        let effective = Cursor::new(EFF_TS, 0);
        let plan = plan_window_load(&cfg, Some(prev), effective, false, None, &[]);
        assert_eq!(plan.raw_runs.len(), 1);
        assert_eq!(plan.raw_runs[0].from, Cursor::new(PREV_TS - WL, 0));
        assert!(!plan.tile_runs.is_empty());
    }

    #[tokio::test]
    async fn rebuild_plain_gap_union_uses_emit_ends() {
        let cfg = window_cfg(min_sql(), Some(tiling_1m())).await;
        let effective = Cursor::new(EFF_TS, 0);
        let emit_ends = vec![Cursor::new(EFF_TS, 0)];
        let plan = plan_window_load(
            &cfg,
            Some(Cursor::new(PREV_TS, 0)),
            effective,
            true,
            None,
            &emit_ends,
        );
        assert!(!plan.tile_runs.is_empty());
        assert!(
            plan.raw_runs
                .iter()
                .all(|r| r.from != Cursor::new(PREV_TS - WL, 0) || r.to != effective)
                || plan.raw_runs.iter().any(|r| r.from.ts > PREV_TS - WL),
            "plain rebuild should prefer coverage gaps over full span"
        );
    }

    #[tokio::test]
    async fn rebuild_cold_with_first_ingested_plans_tiles() {
        let cfg = window_cfg(sum_sql(), Some(tiling_1m())).await;
        let first = Cursor::new(3 * 60_000, 0);
        let effective = Cursor::new(EFF_TS, 0);
        let plan = plan_window_load(&cfg, None, effective, false, Some(first), &[]);
        assert_eq!(plan.raw_runs[0].from, Cursor::new(first.ts - WL, 0));
        assert!(
            !plan.tile_runs.is_empty(),
            "closed cold bound should plan tiles"
        );
    }

    #[tokio::test]
    async fn rebuild_cold_without_first_ingested_skips_tiles() {
        let cfg = window_cfg(sum_sql(), Some(tiling_1m())).await;
        let plan = plan_window_load(&cfg, None, Cursor::new(EFF_TS, 0), false, None, &[]);
        assert_eq!(plan.raw_runs[0].from.ts, i64::MIN);
        assert!(plan.tile_runs.is_empty());
    }

    #[tokio::test]
    async fn slide_leave_tiles_add_raw() {
        let cfg = window_cfg(sum_sql(), Some(tiling_1m())).await;
        let prev = Cursor::new(PREV_TS, 0);
        let effective = Cursor::new(EFF_TS, 0);
        let plan = plan_window_load(&cfg, Some(prev), effective, true, None, &[]);
        assert!(plan
            .raw_runs
            .iter()
            .any(|r| r.from == Cursor::new(PREV_TS, 1) && r.to == effective));
        assert_eq!(plan.tile_runs[0].start_ts, 3 * 60_000);
        assert_eq!(plan.tile_runs[0].end_ts_exclusive, 5 * 60_000);
        assert!(!plan.raw_runs.iter().any(|r| r.from.ts == PREV_TS - WL));
    }

    #[tokio::test]
    async fn slide_without_tiling_leave_raw() {
        let cfg = window_cfg(sum_sql(), None).await;
        let prev = Cursor::new(PREV_TS, 0);
        let effective = Cursor::new(EFF_TS, 0);
        let plan = plan_window_load(&cfg, Some(prev), effective, true, None, &[]);
        assert!(plan.tile_runs.is_empty());
        assert!(plan.raw_runs.iter().any(|r| {
            r.from == Cursor::new(PREV_TS - WL, 0)
                && r.to == Cursor::new(5 * 60_000 - 1, u64::MAX)
        }));
    }
}
