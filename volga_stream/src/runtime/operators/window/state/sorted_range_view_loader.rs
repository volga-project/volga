use indexmap::IndexSet;
use std::sync::Arc;

use datafusion::physical_plan::WindowExpr;

use crate::common::Key;
use crate::runtime::operators::window::state::index::{DataBounds, DataRequest, SortedRangeView};
use crate::runtime::operators::window::window_operator_state::WindowOperatorState;
use crate::storage::batch_store::Timestamp;
pub use crate::storage::sorted_range_view_loader::RangesLoadPlan;
use crate::storage::sorted_range_view_loader::{execute_planned_load, plan_load_from_index};

/// Load one `SortedRangeView` per non-overlapping `DataRequest`.
pub async fn load_sorted_ranges_view(
    state: &WindowOperatorState,
    key: &Key,
    ts_column_index: usize,
    requests: &[DataRequest],
    window_expr_for_args: &Arc<dyn WindowExpr>,
) -> Vec<SortedRangeView> {
    let plans = [RangesLoadPlan {
        requests: requests.to_vec(),
        window_expr_for_args: window_expr_for_args.clone(),
    }];
    load_sorted_ranges_views(state, key, ts_column_index, &plans)
        .await
        .into_iter()
        .next()
        .unwrap_or_default()
}

/// Load views for multiple independent request sets, sharing physical IO across all of them.
pub async fn load_sorted_ranges_views(
    state: &WindowOperatorState,
    key: &Key,
    ts_column_index: usize,
    plans: &[RangesLoadPlan],
) -> Vec<Vec<SortedRangeView>> {
    if plans.is_empty() {
        return Vec::new();
    }
    if plans.iter().all(|p| p.requests.is_empty()) {
        return vec![Vec::new(); plans.len()];
    }

    let Some(windows_state_guard) = state.get_windows_state(key).await else {
        return vec![Vec::new(); plans.len()];
    };

    // Before planning, ensure any persisted-only buckets we touch are rehydrated into the hot in-mem
    // representation, and then compact hot buckets into disjoint segments if needed.
    let mut buckets_to_compact: IndexSet<Timestamp> = IndexSet::new();
    let mut buckets_to_rehydrate: IndexSet<Timestamp> = IndexSet::new();
    {
        let idx = &windows_state_guard.value().bucket_index;
        for p in plans {
            for req in &p.requests {
                let effective_bucket_range = match req.bounds {
                    DataBounds::RowsTail { end_ts, rows } => idx
                        .plan_rows_tail(end_ts, rows, req.bucket_range)
                        .unwrap_or(req.bucket_range),
                    _ => req.bucket_range,
                };
                for bucket in idx.query_buckets_in_range(effective_bucket_range.start, effective_bucket_range.end) {
                    if bucket.hot_base_segments.is_empty() && !bucket.persisted_segments.is_empty() {
                        buckets_to_rehydrate.insert(bucket.timestamp);
                    }
                    if bucket.should_compact() {
                        buckets_to_compact.insert(bucket.timestamp);
                    }
                }
            }
        }
    }
    drop(windows_state_guard);

    for bucket_ts in buckets_to_rehydrate {
        state.rehydrate_bucket_on_read(key, bucket_ts, ts_column_index).await;
    }
    for bucket_ts in buckets_to_compact {
        state.compact_bucket_on_read(key, bucket_ts, ts_column_index).await;
    }

    // Snapshot only lightweight bucket metadata under the lock (no awaiting while holding it).
    let Some(windows_state_guard) = state.get_windows_state(key).await else {
        return vec![Vec::new(); plans.len()];
    };
    let planned = plan_load_from_index(&windows_state_guard.value().bucket_index, plans);
    drop(windows_state_guard);

    execute_planned_load(
        planned,
        state.get_batch_store().clone(),
        state.batch_pins().clone(),
        state.in_mem_batch_cache(),
        key,
        ts_column_index,
        plans,
    )
    .await
}

