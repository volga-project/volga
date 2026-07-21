//! WO watermark advance: plan → one parallel load → produce → emit.

use std::collections::BTreeMap;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::scalar::ScalarValue;
use futures::future::{BoxFuture, FutureExt};

use crate::common::Key;
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::frame_utils::{get_window_length_ms, require_range_frame};
use crate::runtime::operators::window::shared::stack_concat_results;
use crate::runtime::operators::window::shared::WindowConfig;
use crate::runtime::operators::window::state::tile::{
    merge_tile_runs, project_tiles, TimeGranularity, WindowTiles,
};
use crate::runtime::operators::window::store::event_store::StoredRow;
use crate::runtime::operators::window::store::row_nav::RowNav;
use crate::runtime::operators::window::store::WindowStateStore;
use crate::runtime::operators::window::window_operator_state::WindowId;
use crate::runtime::operators::window::AggregatorType;

use super::load_plan::{plan_window_load, LoadPlan};
use super::plain::produce_plain_range;
use super::retractable::produce_retractable_range;

enum LoadPart {
    Raw(Vec<StoredRow>),
    Tile {
        gran: TimeGranularity,
        items: Vec<(i64, WindowTiles)>,
    },
}

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

    let mut plans = Vec::with_capacity(window_configs.len());
    for (window_id, cfg) in window_configs {
        require_range_frame(cfg.window_expr.get_window_frame());
        let has_acc = key_state.accumulators.contains_key(window_id);
        plans.push(plan_window_load(cfg, prev, effective, has_acc));
    }
    let load_plan = LoadPlan::union(plans);

    // One join: every independent KV scan (raw ranges + tile ranges).
    let mut futs: Vec<BoxFuture<'_, Result<LoadPart>>> = Vec::new();
    for run in &load_plan.raw_runs {
        let from = run.from;
        let to = run.to;
        futs.push(
            async move { Ok(LoadPart::Raw(store.events.scan(key, from, to).await?)) }.boxed(),
        );
    }
    for run in merge_tile_runs(load_plan.tile_runs) {
        let gran = run.granularity;
        let start_ts = run.start_ts;
        let end_ts = run.end_ts_exclusive;
        futs.push(
            async move {
                Ok(LoadPart::Tile {
                    gran,
                    items: store.tiles.scan(key, gran, start_ts, end_ts).await?,
                })
            }
            .boxed(),
        );
    }
    let parts = futures::future::try_join_all(futs).await?;

    let mut rows = Vec::new();
    let mut tile_by_key = BTreeMap::new();
    for part in parts {
        match part {
            LoadPart::Raw(chunk) => rows.extend(chunk),
            LoadPart::Tile { gran, items } => {
                for (tile_start, window_tiles) in items {
                    tile_by_key.insert((gran, tile_start), window_tiles);
                }
            }
        }
    }
    rows.sort_by_key(|r| r.cursor);
    rows.dedup_by(|a, b| a.cursor == b.cursor);

    let mut values_by_window: Vec<Vec<ScalarValue>> = Vec::new();
    let mut emit_cursors: Option<Vec<Cursor>> = None;
    let mut new_pos = prev;

    for (window_id, cfg) in window_configs {
        let acc = key_state.accumulators.get(window_id).cloned();
        let wl = get_window_length_ms(cfg.window_expr.get_window_frame());
        let nav = RowNav::from_stored_with_args(rows.clone(), &cfg.window_expr);

        let loaded_tiles = project_tiles(&tile_by_key, *window_id);
        let tile_cfg = cfg.tiling.as_ref();

        let (values, pos, new_acc) = match cfg.aggregator_type {
            AggregatorType::PlainAccumulator => produce_plain_range(
                &cfg.window_expr,
                &nav,
                prev,
                effective,
                tile_cfg,
                &loaded_tiles,
            ),
            AggregatorType::RetractableAccumulator => produce_retractable_range(
                &cfg.window_expr,
                &nav,
                prev,
                effective,
                acc.as_ref(),
                wl,
                tile_cfg,
                &loaded_tiles,
            ),
        };

        if emit_cursors.is_none() {
            emit_cursors = Some(collect_emit_cursors(&nav, prev, effective));
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

    let emit_input_rows = emit_input_rows_from_stored(&rows, &emit_cursors, input_schema)?;
    let out = stack_concat_results(emit_input_rows, values_by_window, output_schema, input_schema);
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

fn collect_emit_cursors(nav: &RowNav, prev: Option<Cursor>, advance_to: Cursor) -> Vec<Cursor> {
    let Some(mut idx) = nav.first_update_idx(prev) else {
        return vec![];
    };
    let Some(end) = nav.seek_le(advance_to) else {
        return vec![];
    };
    let mut out = Vec::new();
    loop {
        out.push(nav.cursor(idx));
        if idx == end {
            break;
        }
        idx = nav.next(idx).expect("end reachable");
    }
    out
}
