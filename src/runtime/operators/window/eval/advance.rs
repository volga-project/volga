//! Watermark advance: meta → exact raw/tile ranges → slide | rebuild → emit.

use std::collections::BTreeMap;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::config::WindowConfig;
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::frame_utils::{get_window_length_ms, require_range_frame};
use crate::runtime::operators::window::state::tile::{merge_raw_runs, merge_tile_runs, RawRun};
use crate::runtime::operators::window::store::{PartitionKey, WindowData, WindowOperatorStore};
use crate::runtime::operators::window::window_operator_state::WindowId;
use crate::runtime::operators::window::{window_supports_tile_slide, AggregatorType};

use super::emit::{emit_ends, emit_input_rows};
use super::output::assemble_window_batch;
use super::primitives::{append_coverage_runs, append_rebuild_runs, plan_rebuilds, plan_slides};
use super::rebuild::produce_rebuild;
use super::slide::produce_slide;

/// Advance one key to `advance_to` (typically `Cursor(wm, u64::MAX)`).
///
/// The WO is the sole mutator, so its data reads need no shared snapshot.
pub async fn advance_key(
    store: &dyn WindowOperatorStore,
    partition: &PartitionKey,
    window_configs: &BTreeMap<WindowId, WindowConfig>,
    advance_to: Cursor,
    emit: bool,
    ts_column_index: usize,
    output_schema: &SchemaRef,
    input_schema: &SchemaRef,
    lateness_ms: Option<i64>,
) -> Result<(RecordBatch, bool)> {
    let mut max_wl = 0i64;
    for cfg in window_configs.values() {
        require_range_frame(cfg.window_expr.get_window_frame());
        max_wl = max_wl.max(get_window_length_ms(cfg.window_expr.get_window_frame()));
    }

    let mut key_state = store.load_meta(partition).await?;

    let Some(max_seen) = key_state.max_seen else {
        return Ok((RecordBatch::new_empty(output_schema.clone()), false));
    };

    let effective = advance_to.min(Cursor::new(max_seen.ts, u64::MAX));
    let still_pending = max_seen > effective;

    if let Some(p) = key_state.processed_pos {
        if effective <= p {
            return Ok((RecordBatch::new_empty(output_schema.clone()), still_pending));
        }
    }

    let prev = key_state.processed_pos;
    let first_ingested = key_state.first_ingested;
    let update_from = prev
        .map(Cursor::next)
        .or(first_ingested)
        .unwrap_or(Cursor::new(effective.ts, 0));
    let update_run = RawRun {
        from: update_from,
        to: Cursor::after_timestamp(effective.ts),
    };
    let mut raw_batches = store.load_raw(partition, &[update_run]).await?;
    let ends = emit_ends(&raw_batches, ts_column_index, prev, effective);

    let mut plans = BTreeMap::new();
    let mut historical_raw_runs = Vec::new();
    let mut tile_runs = Vec::new();
    for (window_id, cfg) in window_configs {
        let wl = get_window_length_ms(cfg.window_expr.get_window_frame());
        let plans_for_window = if cfg.aggregator_type == AggregatorType::RetractableAccumulator {
            let tile_cfg = cfg
                .tiling
                .as_ref()
                .filter(|_| window_supports_tile_slide(&cfg.window_expr));
            let plans = plan_slides(&ends, prev, wl, tile_cfg);
            append_coverage_runs(&plans, &mut historical_raw_runs, &mut tile_runs);
            plans
        } else {
            let floor = window_start_floor(prev, first_ingested, wl);
            let plans = plan_rebuilds(&ends, wl, cfg.tiling.as_ref(), floor);
            append_rebuild_runs(&plans, wl, &mut historical_raw_runs, &mut tile_runs);
            plans
        };
        plans.insert(*window_id, plans_for_window);
    }

    let historical_raw_runs = merge_raw_runs(historical_raw_runs);
    let tile_runs = merge_tile_runs(tile_runs);
    let (historical_raw, tiles) = tokio::try_join!(
        store.load_raw(partition, &historical_raw_runs),
        store.load_tiles(partition, &tile_runs),
    )?;
    raw_batches.extend(historical_raw);
    let batch = WindowData::new(raw_batches, tiles);

    let mut values_by_window: Vec<Vec<ScalarValue>> = Vec::new();
    for (window_id, cfg) in window_configs {
        let acc = key_state.accumulators.get(window_id).cloned();
        let view = batch.for_window(*window_id, ts_column_index, &cfg.window_expr);
        let plans = plans.get(window_id).expect("window plan");
        let (values, new_acc) = if cfg.aggregator_type == AggregatorType::RetractableAccumulator {
            produce_slide(&cfg.window_expr, &view, prev, acc.as_ref(), plans)
        } else {
            (produce_rebuild(&cfg.window_expr, &view, plans), None)
        };

        if let Some(acc) = new_acc {
            key_state.accumulators.insert(*window_id, acc);
        } else {
            key_state.accumulators.remove(window_id);
        }
        values_by_window.push(values);
    }

    // Shared frontier: last emit cursor, else watermark (even when no rows).
    key_state.processed_pos = ends.last().copied().or(Some(effective));
    if let (Some(lateness), Some(processed)) = (lateness_ms, key_state.processed_pos) {
        key_state.retention_floor = Some(Cursor::new(
            processed
                .ts
                .saturating_sub(max_wl)
                .saturating_sub(lateness.max(0)),
            0,
        ));
    }
    store.store_meta(partition, &key_state).await?;

    if !emit {
        return Ok((RecordBatch::new_empty(output_schema.clone()), still_pending));
    }

    if ends.is_empty() || values_by_window.iter().all(|v| v.is_empty()) {
        return Ok((RecordBatch::new_empty(output_schema.clone()), still_pending));
    }

    let emit_input = emit_input_rows(batch.raw_batches(), ts_column_index, &ends, input_schema)?;
    let out = assemble_window_batch(emit_input, values_by_window, output_schema, input_schema);
    Ok((out, still_pending))
}

fn window_start_floor(
    prev: Option<Cursor>,
    first_ingested: Option<Cursor>,
    window_length: i64,
) -> Option<i64> {
    prev.or(first_ingested)
        .map(|cursor| cursor.ts.saturating_sub(window_length.max(0)))
}
