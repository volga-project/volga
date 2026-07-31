//! Watermark advance: meta → exact raw/tile ranges → slide | rebuild → emit.

use std::collections::BTreeMap;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::config::WindowConfig;
use crate::runtime::operators::window::frame_utils::{get_window_length_ms, require_range_frame};
use crate::runtime::operators::window::model::{
    Cursor, KeyEvaluationState, RawRun, WindowId, WindowTriggerKind,
};
use crate::runtime::operators::window::store::{DueWindowWork, WindowData, WindowOperatorStore};
use crate::runtime::operators::window::{window_supports_tile_slide, AccumulatorType};

use super::coverage_plan::{merge_raw_runs, merge_tile_runs};
use super::emit::emit_input_rows;
use super::eval_plan::{append_coverage_runs, plan_rebuilds, plan_slides};
use super::output::assemble_window_batch;
use super::rebuild::produce_rebuild;
use super::slide::produce_slide;

/// Evaluate one page of durable row triggers for a key.
pub async fn advance_due_work(
    store: &dyn WindowOperatorStore,
    work: DueWindowWork,
    window_configs: &BTreeMap<WindowId, WindowConfig>,
    emit: bool,
    ts_column_index: usize,
    output_schema: &SchemaRef,
    input_schema: &SchemaRef,
) -> Result<RecordBatch> {
    for cfg in window_configs.values() {
        require_range_frame(cfg.window_expr.get_window_frame());
    }

    let DueWindowWork {
        partition,
        mut key_state,
        triggers,
    } = work;
    let prev = key_state
        .evaluation
        .as_ref()
        .map(|evaluation| evaluation.through);
    let mut ends = triggers
        .into_iter()
        .filter(|trigger| matches!(trigger.kind, WindowTriggerKind::RowEmit))
        .map(|trigger| trigger.fire_at)
        .filter(|cursor| prev.map_or(true, |prev| *cursor > prev))
        .collect::<Vec<_>>();
    ends.sort_unstable();
    ends.dedup();
    let Some(first) = ends.first().copied() else {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    };
    let last = *ends.last().expect("non-empty trigger ends");

    let mut plans = BTreeMap::new();
    let mut raw_runs = vec![RawRun {
        from: prev.map(Cursor::next).unwrap_or(first),
        to: Cursor::after_timestamp(last.ts),
    }];
    let mut tile_runs = Vec::new();
    for (window_id, cfg) in window_configs {
        let wl = get_window_length_ms(cfg.window_expr.get_window_frame());
        let plans_for_window = if cfg.accumulator_type == AccumulatorType::RetractableAccumulator {
            let tile_cfg = cfg
                .tiling
                .as_ref()
                .filter(|_| window_supports_tile_slide(&cfg.window_expr));
            let plans = plan_slides(&ends, prev, wl, tile_cfg);
            append_coverage_runs(&plans, &mut raw_runs, &mut tile_runs);
            plans
        } else {
            let plans = plan_rebuilds(&ends, wl, cfg.tiling.as_ref());
            append_coverage_runs(&plans, &mut raw_runs, &mut tile_runs);
            plans
        };
        plans.insert(*window_id, plans_for_window);
    }

    let raw_runs = merge_raw_runs(raw_runs);
    let tile_runs = merge_tile_runs(tile_runs);
    let (raw_batches, tiles) = tokio::try_join!(
        store.load_raw(&partition, &raw_runs),
        store.load_tiles(&partition, &tile_runs),
    )?;
    let batch = WindowData::new(raw_batches, tiles);

    let mut values_by_window: Vec<Vec<ScalarValue>> = Vec::new();
    let mut accumulators = key_state
        .evaluation
        .take()
        .map(|evaluation| evaluation.accumulators)
        .unwrap_or_default();
    for (window_id, cfg) in window_configs {
        let acc = accumulators.get(window_id).cloned();
        let view = batch.for_window(*window_id, ts_column_index, &cfg.window_expr);
        let plans = plans.get(window_id).expect("window plan");
        let (values, new_acc) = if cfg.accumulator_type == AccumulatorType::RetractableAccumulator {
            produce_slide(&cfg.window_expr, &view, prev, acc.as_ref(), plans)
        } else {
            (produce_rebuild(&cfg.window_expr, &view, plans), None)
        };

        if let Some(acc) = new_acc {
            accumulators.insert(*window_id, acc);
        } else {
            accumulators.remove(window_id);
        }
        values_by_window.push(values);
    }

    key_state.evaluation = Some(KeyEvaluationState {
        through: last,
        accumulators,
    });
    store.commit_advance(&partition, &key_state).await?;

    if !emit {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    if ends.is_empty() || values_by_window.iter().all(|v| v.is_empty()) {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    let emit_input = emit_input_rows(batch.raw_batches(), ts_column_index, &ends, input_schema)?;
    let out = assemble_window_batch(emit_input, values_by_window, output_schema, input_schema);
    Ok(out)
}
