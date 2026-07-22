//! WO watermark advance entrypoint.
//!
//! ```text
//! peek emit band (plain+tiles) → plan → load →
//!   merge emit rows → KeyBatch::for_window →
//!   Plain        → produce::produce_plain_range
//!   Retractable  → produce::produce_retractable_range
//! → emit
//! ```
//!
//! Cold (no `processed_pos`): `win_start = first_ingested.ts − wl` — same rebuild/slide
//! paths, not a separate cold module.

use std::collections::BTreeMap;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::scalar::ScalarValue;

use crate::common::Key;
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::frame_utils::{get_window_length_ms, require_range_frame};
use crate::runtime::operators::window::config::WindowConfig;
use crate::runtime::operators::window::evaluate::assemble_window_batch;
use crate::runtime::operators::window::store::event_store::StoredRow;
use crate::runtime::operators::window::store::WindowStateStore;
use crate::runtime::operators::window::window_operator_state::WindowId;
use crate::runtime::operators::window::AggregatorType;

use super::super::primitives::{cursor_next, load, LoadPlan, WindowView};
use super::plan::plan_window_load;
use super::produce::{produce_plain_range, produce_retractable_range};

/// Advance one key to `advance_to` (typically Cursor(wm, u64::MAX)).
pub async fn advance_key(
    store: &WindowStateStore,
    key: &Key,
    window_configs: &BTreeMap<WindowId, WindowConfig>,
    advance_to: Cursor,
    emit: bool,
    output_schema: &SchemaRef,
    input_schema: &SchemaRef,
) -> Result<(RecordBatch, bool)> {
    let mut key_state = store.meta.get_key(key).await?;
    let Some(max_seen) = key_state.max_seen else {
        return Ok((RecordBatch::new_empty(output_schema.clone()), false));
    };

    let effective = advance_to.min(Cursor::new(max_seen.ts, u64::MAX));
    let still_pending = max_seen > effective;
    let prev = key_state.processed_pos;

    if let Some(p) = prev {
        if effective <= p {
            return Ok((RecordBatch::new_empty(output_schema.clone()), still_pending));
        }
    }

    let first_ingested = key_state.first_ingested;

    // Plain+tiles needs emit ends to union gap-only coverages; peek once.
    let needs_emit_peek = window_configs.values().any(|cfg| {
        cfg.aggregator_type == AggregatorType::PlainAccumulator && cfg.tiling.is_some()
    });
    let emit_from = match prev {
        Some(p) => cursor_next(p),
        None => first_ingested
            .map(|f| Cursor::new(f.ts, 0))
            .unwrap_or(Cursor::new(i64::MIN, 0)),
    };
    let emit_rows = if needs_emit_peek && emit_from.ts != i64::MIN {
        store.events.scan(key, emit_from, effective).await?
    } else {
        vec![]
    };
    let emit_ends: Vec<Cursor> = emit_rows.iter().map(|r| r.cursor).collect();

    let mut plans = Vec::with_capacity(window_configs.len());
    for (window_id, cfg) in window_configs {
        require_range_frame(cfg.window_expr.get_window_frame());
        let has_acc = key_state.accumulators.contains_key(window_id);
        plans.push(plan_window_load(
            cfg,
            prev,
            effective,
            has_acc,
            first_ingested,
            &emit_ends,
        ));
    }
    let batch = load(store, key, LoadPlan::union(plans))
        .await?
        .with_merged_rows(emit_rows);

    let mut values_by_window: Vec<Vec<ScalarValue>> = Vec::new();
    // Shared frontier: all windows on a key advance together; emit cursors / new_pos
    // come from the first window's nav (same emit band for every window).
    let mut emit_cursors: Option<Vec<Cursor>> = None;
    let mut new_pos = prev;

    for (window_id, cfg) in window_configs {
        let acc = key_state.accumulators.get(window_id).cloned();
        let wl = get_window_length_ms(cfg.window_expr.get_window_frame());
        let view = batch.for_window(*window_id, &cfg.window_expr);
        let tile_cfg = cfg.tiling.as_ref();

        let (values, pos, new_acc) = match cfg.aggregator_type {
            AggregatorType::PlainAccumulator => {
                produce_plain_range(&cfg.window_expr, &view, prev, effective, tile_cfg)
            }
            AggregatorType::RetractableAccumulator => produce_retractable_range(
                &cfg.window_expr,
                &view,
                prev,
                effective,
                acc.as_ref(),
                wl,
                tile_cfg,
            ),
        };

        if emit_cursors.is_none() {
            emit_cursors = Some(collect_emit_cursors(&view, prev, effective));
        }
        if let Some(p) = pos {
            new_pos = Some(p);
        } else if new_pos.is_none() {
            new_pos = Some(effective);
        }
        if let Some(acc) = new_acc {
            key_state.accumulators.insert(*window_id, acc);
        } else {
            key_state.accumulators.remove(window_id);
        }
        values_by_window.push(values);
    }

    key_state.processed_pos = new_pos.or(Some(effective));
    store.meta.put_key(key, &key_state).await?;

    if !emit {
        return Ok((RecordBatch::new_empty(output_schema.clone()), still_pending));
    }

    let emit_cursors = emit_cursors.unwrap_or_default();
    if emit_cursors.is_empty() || values_by_window.iter().all(|v| v.is_empty()) {
        return Ok((RecordBatch::new_empty(output_schema.clone()), still_pending));
    }

    let emit_input_rows = emit_input_rows_from_stored(batch.rows(), &emit_cursors, input_schema)?;
    let out = assemble_window_batch(emit_input_rows, values_by_window, output_schema, input_schema);
    Ok((out, still_pending))
}

fn emit_input_rows_from_stored(
    rows: &[StoredRow],
    emit_cursors: &[Cursor],
    input_schema: &SchemaRef,
) -> Result<Vec<Vec<ScalarValue>>> {
    let mut out = Vec::with_capacity(emit_cursors.len());
    let mut i = 0usize;
    for c in emit_cursors {
        while i < rows.len() && rows[i].cursor < *c {
            i += 1;
        }
        let Some(row) = rows.get(i).filter(|r| r.cursor == *c) else {
            out.push(vec![ScalarValue::Null; input_schema.fields().len()]);
            continue;
        };
        let mut vals = Vec::new();
        for col_idx in 0..input_schema.fields().len() {
            vals.push(ScalarValue::try_from_array(row.batch.column(col_idx), 0)?);
        }
        out.push(vals);
    }
    Ok(out)
}

fn collect_emit_cursors(
    view: &WindowView,
    prev: Option<Cursor>,
    advance_to: Cursor,
) -> Vec<Cursor> {
    let Some(mut idx) = view.nav.first_update_idx(prev) else {
        return vec![];
    };
    let Some(end) = view.nav.seek_le(advance_to) else {
        return vec![];
    };
    let mut out = Vec::new();
    loop {
        out.push(view.nav.cursor(idx));
        if idx == end {
            break;
        }
        idx = view.nav.next(idx).expect("end reachable");
    }
    out
}
