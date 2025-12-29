use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use crate::common::Key;
use crate::runtime::operators::window::index::{DataBounds, DataRequest, SortedBucketBatch, SortedRangeBucket, SortedRangeView};
use crate::runtime::operators::window::window_operator_state::WindowOperatorState;
use crate::runtime::operators::window::{Cursor, SEQ_NO_COLUMN_NAME};
use crate::storage::batch_store::{BatchId, Timestamp};
use datafusion::physical_plan::WindowExpr;
use indexmap::IndexSet;

/// Load one `SortedRangeView` per non-overlapping `RangeRequest`.
///
/// First implementation loads full buckets and sorts them (no physical IO pruning yet).
pub async fn load_sorted_ranges_view(
    state: &WindowOperatorState,
    key: &Key,
    ts_column_index: usize,
    requests: &[DataRequest],
    window_expr_for_args: &Arc<dyn WindowExpr>,
) -> Vec<SortedRangeView> {
    if requests.is_empty() {
        return Vec::new();
    }

    let Some(windows_state_guard) = state.get_windows_state(key).await else {
        return Vec::new();
    };
    let bucket_granularity = windows_state_guard.value().bucket_index.bucket_granularity();
    let bucket_index = windows_state_guard.value().bucket_index.clone();
    drop(windows_state_guard);

    // Collect bucket -> batch_ids mapping for requested ranges, potentially pruning runs/batches.
    let key_hash = key.hash();
    #[derive(Debug, Clone)]
    struct BucketPlan {
        version: u64,
        load_all: bool,
        batch_ids: IndexSet<BatchId>,
    }
    let mut bucket_plans: HashMap<Timestamp, BucketPlan> = HashMap::new();
    let mut batch_ids_to_load: IndexSet<BatchId> = IndexSet::new();

    for req in requests {
        match req.bounds {
            DataBounds::All => {
                for bucket in bucket_index.query_buckets_in_range(req.bucket_range.start, req.bucket_range.end) {
                    let plan = bucket_plans.entry(bucket.timestamp).or_insert_with(|| BucketPlan {
                        version: bucket.version,
                        load_all: true,
                        batch_ids: IndexSet::new(),
                    });
                    plan.version = bucket.version;
                    plan.load_all = true;
                    for meta in &bucket.batches {
                        plan.batch_ids.insert(meta.batch_id);
                    }
                }
            }
            DataBounds::Time { start_ts, end_ts } => {
                let start_bucket = bucket_granularity.start(start_ts);
                let end_bucket = bucket_granularity.start(end_ts);

                for bucket in bucket_index.query_buckets_in_range(req.bucket_range.start, req.bucket_range.end) {
                    let bucket_ts = bucket.timestamp;
                    let is_edge = bucket_ts == start_bucket || bucket_ts == end_bucket;
                    let need_full_bucket = !is_edge;

                    let plan = bucket_plans.entry(bucket_ts).or_insert_with(|| BucketPlan {
                        version: bucket.version,
                        load_all: false,
                        batch_ids: IndexSet::new(),
                    });
                    plan.version = bucket.version;
                    plan.load_all |= need_full_bucket;

                    let wanted_start = if bucket_ts == start_bucket {
                        Cursor::new(start_ts, 0)
                    } else {
                        Cursor::new(i64::MIN, 0)
                    };
                    let wanted_end = if bucket_ts == end_bucket {
                        Cursor::new(end_ts, u64::MAX)
                    } else {
                        Cursor::new(i64::MAX, u64::MAX)
                    };

                    for meta in &bucket.batches {
                        if plan.load_all {
                            plan.batch_ids.insert(meta.batch_id);
                            continue;
                        }
                        if meta.max_pos < wanted_start || meta.min_pos > wanted_end {
                            continue;
                        }
                        plan.batch_ids.insert(meta.batch_id);
                    }
                }
            }
            DataBounds::RowsTail { end_ts, rows } => {
                let Some(tail_range) = bucket_index.plan_rows_tail(end_ts, rows, req.bucket_range) else {
                    continue;
                };
                let end_bucket_ts = bucket_granularity.start(end_ts);

                for bucket in bucket_index.query_buckets_in_range(tail_range.start, tail_range.end) {
                    let bucket_ts = bucket.timestamp;
                    let plan = bucket_plans.entry(bucket_ts).or_insert_with(|| BucketPlan {
                        version: bucket.version,
                        load_all: false,
                        batch_ids: IndexSet::new(),
                    });
                    plan.version = bucket.version;
                    plan.load_all = true;

                    // End bucket: prune runs strictly after end_ts.
                    let metas_filtered: Vec<_> = if bucket_ts == end_bucket_ts {
                        bucket
                            .batches
                            .iter()
                            .cloned()
                            .filter(|m| m.min_pos.ts <= end_ts)
                            .collect()
                    } else {
                        bucket.batches.clone()
                    };

                    // Simplified ROWS behavior: load all runs in all buckets within the planned tail span.
                    for m in metas_filtered {
                        plan.batch_ids.insert(m.batch_id);
                    }
                }
            }
        }
    }

    // First, satisfy what we can from sorted-bucket cache (only safe for full buckets).
    let mut all_sorted: HashMap<Timestamp, SortedBucketBatch> = HashMap::new();
    let mut buckets_to_build: Vec<(Timestamp, u64, bool, Vec<BatchId>)> = Vec::new();
    for (bucket_ts, plan) in bucket_plans {
        if plan.load_all {
            if let Some(existing) = state
                .get_sorted_bucket_cache()
                .get(&(key_hash, bucket_ts, plan.version))
                .map(|e| e.value().clone())
            {
                all_sorted.insert(bucket_ts, (*existing).clone());
                continue;
            }
        }
        let batch_ids: Vec<BatchId> = plan.batch_ids.into_iter().collect();
        for b in &batch_ids {
            batch_ids_to_load.insert(*b);
        }
        buckets_to_build.push((bucket_ts, plan.version, plan.load_all, batch_ids));
    }

    if !buckets_to_build.is_empty() {
        let loaded = state
            .get_batch_store()
            .load_batches(batch_ids_to_load.into_iter().collect(), key)
            .await;

        for (bucket_ts, version, load_all, batch_ids) in buckets_to_build {
            let mut bucket_batches: Vec<RecordBatch> = Vec::new();
            for batch_id in batch_ids {
                if let Some(b) = loaded.get(&batch_id) {
                    bucket_batches.push(b.clone());
                }
            }
            if bucket_batches.is_empty() {
                continue;
            }

            let schema = bucket_batches[0].schema();
            let concatenated = if bucket_batches.len() == 1 {
                bucket_batches.remove(0)
            } else {
                arrow::compute::concat_batches(&schema, &bucket_batches)
                    .expect("Should be able to concat bucket batches")
            };

            let seq_column_index = concatenated
                .schema()
                .fields()
                .iter()
                .position(|f| f.name() == SEQ_NO_COLUMN_NAME)
                .expect("Expected __seq_no column to exist");

            // TODO: avoid full re-sort here by ensuring each stored batch/run is already
            // sorted by (ts, seq_no) and performing a k-way merge when building a bucket.
            let sorted = crate::runtime::operators::window::aggregates::arrow_utils::sort_batch_by_rowpos(
                &concatenated,
                ts_column_index,
                seq_column_index,
            );

            let cached = SortedBucketBatch::new(bucket_ts, sorted, ts_column_index, seq_column_index);
            if load_all {
                state
                    .get_sorted_bucket_cache()
                    .insert((key_hash, bucket_ts, version), Arc::new(cached.clone()));
            }
            all_sorted.insert(bucket_ts, cached);
        }
    }

    let mut out: Vec<SortedRangeView> = Vec::with_capacity(requests.len());
    for req in requests {
        let (start, end) = match req.bounds {
            DataBounds::All => (Cursor::new(i64::MIN, 0), Cursor::new(i64::MAX, u64::MAX)),
            DataBounds::Time { start_ts, end_ts } => (Cursor::new(start_ts, 0), Cursor::new(end_ts, u64::MAX)),
            DataBounds::RowsTail { end_ts, .. } => (Cursor::new(i64::MIN, 0), Cursor::new(end_ts, u64::MAX)),
        };

        let effective_bucket_range = match req.bounds {
            DataBounds::RowsTail { end_ts, rows } => bucket_index
                .plan_rows_tail(end_ts, rows, req.bucket_range)
                .unwrap_or(req.bucket_range),
            _ => req.bucket_range,
        };
        let view_req = DataRequest {
            bucket_range: effective_bucket_range,
            bounds: req.bounds,
        };
        let mut buckets: HashMap<Timestamp, SortedRangeBucket> = HashMap::new();
        let mut ts = effective_bucket_range.start;
        while ts <= effective_bucket_range.end {
            if let Some(b) = all_sorted.get(&ts) {
                let args = window_expr_for_args
                    .evaluate_args(b.batch())
                    .expect("Should be able to evaluate window args");
                buckets.insert(
                    ts,
                    SortedRangeBucket::new(
                        ts,
                        b.batch().clone(),
                        b.ts_column_index(),
                        b.seq_column_index(),
                        Arc::new(args),
                    ),
                );
            }
            ts = bucket_granularity.next_start(ts);
        }
        out.push(SortedRangeView::new(view_req, bucket_granularity, start, end, buckets));
    }

    out
}
