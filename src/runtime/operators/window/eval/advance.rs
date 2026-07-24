//! Watermark advance: estimate envelope → one meta+data load → slide | rebuild → emit.

use std::collections::BTreeMap;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::scalar::ScalarValue;

use crate::common::Key;
use crate::runtime::operators::window::config::WindowConfig;
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::frame_utils::{get_window_length_ms, require_range_frame};
use crate::runtime::operators::window::state::tile::TileConfig;
use crate::runtime::operators::window::store::WindowStateStore;
use crate::runtime::operators::window::window_operator_state::WindowId;

use super::can_slide;
use super::emit::{emit_ends, emit_input_rows};
use super::envelope::{cold_needed_from, estimate_wo_advance, window_start_floor};
use super::output::assemble_window_batch;
use super::primitives::plan_rebuild_prep;
use super::rebuild::produce_rebuild;
use super::slide::produce_slide;

/// Advance one key to `advance_to` (typically `Cursor(wm, u64::MAX)`).
///
/// Sole writer per key with ingest. Loads meta+data in one partition snapshot
/// (see [`super::envelope`]). Concurrent same-key readers rely on SortedKV
/// snapshot semantics (not window-level fencing).
///
/// `retention_pad_ms` is usually `spec.lateness` — widens the estimated envelope
/// for catch-up; `None` sizes for the hot path only.
pub async fn advance_key(
    store: &WindowStateStore,
    key: &Key,
    window_configs: &BTreeMap<WindowId, WindowConfig>,
    advance_to: Cursor,
    emit: bool,
    output_schema: &SchemaRef,
    input_schema: &SchemaRef,
    bucket_ms: i64,
    retention_pad_ms: Option<i64>,
) -> Result<(RecordBatch, bool)> {
    let mut max_wl = 0i64;
    let mut tile_cfgs: Vec<&TileConfig> = Vec::new();
    for cfg in window_configs.values() {
        require_range_frame(cfg.window_expr.get_window_frame());
        max_wl = max_wl.max(get_window_length_ms(cfg.window_expr.get_window_frame()));
        if let Some(t) = cfg.tiling.as_ref() {
            tile_cfgs.push(t);
        }
    }

    // Estimate without meta, then one snap (meta + data).
    let mut env = estimate_wo_advance(advance_to, max_wl, retention_pad_ms);
    let mut loaded = store
        .load_envelope(key, env.from, env.to, bucket_ms, &tile_cfgs)
        .await?;
    let mut key_state = loaded.key_state;

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

    // Estimate may underfetch cold/catch-up; widen left and reload data once (sole writer).
    let needed = cold_needed_from(&key_state, max_wl);
    if needed < env.from.ts {
        env.from = Cursor::new(needed, 0);
        loaded.data = store
            .load_data(key, env.from, effective, bucket_ms, &tile_cfgs)
            .await?;
    }

    let prev = key_state.processed_pos;
    let first_ingested = key_state.first_ingested;
    let batch = loaded.data;
    let ends = emit_ends(batch.chunks(), prev, effective);

    let mut values_by_window: Vec<Vec<ScalarValue>> = Vec::new();
    for (window_id, cfg) in window_configs {
        let acc = key_state.accumulators.get(window_id).cloned();
        let wl = get_window_length_ms(cfg.window_expr.get_window_frame());
        let view = batch.for_window(*window_id, &cfg.window_expr);
        let tile_cfg = cfg.tiling.as_ref();

        let (values, new_acc) = if can_slide(cfg) {
            produce_slide(
                &cfg.window_expr,
                &view,
                prev,
                effective,
                acc.as_ref(),
                wl,
                tile_cfg,
            )
        } else {
            let floor = window_start_floor(prev, first_ingested, wl);
            let units = plan_rebuild_prep(&ends, wl, tile_cfg, floor);
            (produce_rebuild(&cfg.window_expr, &view, &units), None)
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
    store.meta.put_key(key, &key_state).await?;

    if !emit {
        return Ok((RecordBatch::new_empty(output_schema.clone()), still_pending));
    }

    if ends.is_empty() || values_by_window.iter().all(|v| v.is_empty()) {
        return Ok((RecordBatch::new_empty(output_schema.clone()), still_pending));
    }

    let emit_input = emit_input_rows(batch.chunks(), &ends, input_schema)?;
    let out = assemble_window_batch(emit_input, values_by_window, output_schema, input_schema);
    Ok((out, still_pending))
}
