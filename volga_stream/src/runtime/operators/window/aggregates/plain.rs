use std::sync::Arc;
use async_trait::async_trait;
use datafusion::logical_expr::WindowFrameUnits;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use tokio_rayon::rayon::ThreadPool;
use tokio_rayon::AsyncThreadPool;
use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::runtime::operators::window::index::{BucketIndex, SlideInfo, get_window_length_ms, get_window_size_rows};
use crate::runtime::operators::window::tiles::Tile;
use crate::runtime::operators::window::{Tiles, TimeGranularity};
use crate::storage::batch_store::Timestamp;
use crate::runtime::operators::window::index::SortedBucketView;
use crate::runtime::operators::window::index::window_logic;
use super::{Aggregation, AggregatorType, BucketRange};
use super::{WindowAggregator, create_window_aggregator, merge_accumulator_state};
use crate::runtime::operators::window::{Cursor, RowIndex, RowPtr};

#[derive(Debug)]
pub struct PlainAggregation {
    /// Per-entry logical isolation: each entry range has its own window bucket range.
    pub entry_plans: Vec<(BucketRange, BucketRange)>, // (entries, window)
    /// Optional cursor bounds for "delta mode" (streaming): process rows with Cursor in
    /// `(prev_processed_until, new_processed_until]`.
    pub cursor_bounds: Option<(Option<Cursor>, Cursor)>,
    pub window_expr: Arc<dyn WindowExpr>,
    pub tiles: Option<Tiles>,
    pub ts_column_index: usize,
    pub window_id: usize,
    pub bucket_granularity: TimeGranularity,
}

impl PlainAggregation {
    /// Default mode: evaluate for every row in `entry_ranges`.
    pub fn new(
        entry_ranges: Vec<BucketRange>,
        batch_index: &BucketIndex,
        window_expr: Arc<dyn WindowExpr>,
        tiles: Option<Tiles>,
        ts_column_index: usize,
        window_id: usize,
    ) -> Self {
        let window_frame = window_expr.get_window_frame();

        let mut entry_plans: Vec<(BucketRange, BucketRange)> = Vec::new();
        for r in entry_ranges {
            let window_range = match window_frame.units {
                WindowFrameUnits::Range => {
                    let wl = get_window_length_ms(window_frame);
                    batch_index.get_relevant_range_for_range_windows(r, wl)
                }
                WindowFrameUnits::Rows => {
                    let ws = get_window_size_rows(window_frame);
                    batch_index.get_relevant_range_for_rows_windows(r, ws)
                }
                _ => BucketRange::new(r.start, r.end),
            };
            entry_plans.push((r, window_range));
        }

        Self {
            entry_plans,
            cursor_bounds: None,
            window_expr,
            tiles,
            ts_column_index,
            window_id,
            bucket_granularity: batch_index.bucket_granularity(),
        }
    }

    /// Delta mode: only evaluate for rows in `entry_ranges` whose Cursor is in
    /// `(prev_processed_until, new_processed_until]`.
    pub fn new_bounded(
        entry_ranges: Vec<BucketRange>,
        batch_index: &BucketIndex,
        window_expr: Arc<dyn WindowExpr>,
        tiles: Option<Tiles>,
        ts_column_index: usize,
        window_id: usize,
        prev_processed_until: Option<Cursor>,
        new_processed_until: Cursor,
    ) -> Self {
        let mut out = Self::new(
            entry_ranges,
            batch_index,
            window_expr,
            tiles,
            ts_column_index,
            window_id,
        );
        out.cursor_bounds = Some((prev_processed_until, new_processed_until));
        out
    }
}

#[async_trait]
impl Aggregation for PlainAggregation {
    async fn produce_aggregates(
        &self,
        sorted_bucket_view: &std::collections::HashMap<Timestamp, SortedBucketView>,
        thread_pool: Option<&ThreadPool>,
        _exclude_current_row: Option<bool>,
    ) -> (Vec<ScalarValue>, Option<AccumulatorState>) {
        if self.entry_plans.is_empty() {
            return (vec![], None);
        }

        // Offload CPU-heavy work when thread pool is provided.
        if let Some(tp) = thread_pool {
            let sorted_bucket_view = sorted_bucket_view.clone();
            let entry_plans = self.entry_plans.clone();
            let cursor_bounds = self.cursor_bounds;
            let window_expr = self.window_expr.clone();
            let tiles = self.tiles.clone();
            let ts_column_index = self.ts_column_index;
            let window_id = self.window_id;
            let bucket_granularity = self.bucket_granularity;
            return tp
                .spawn_fifo_async(move || {
                    run_plain_aggregation(
                        &sorted_bucket_view,
                        &entry_plans,
                        cursor_bounds,
                        &window_expr,
                        tiles.as_ref(),
                        ts_column_index,
                        window_id,
                        bucket_granularity,
                    )
                })
                .await;
        }

        run_plain_aggregation(
            sorted_bucket_view,
            &self.entry_plans,
            self.cursor_bounds,
            &self.window_expr,
            self.tiles.as_ref(),
            self.ts_column_index,
            self.window_id,
            self.bucket_granularity,
        )
    }

    fn window_expr(&self) -> &Arc<dyn WindowExpr> {
        &self.window_expr
    }

    fn tiles(&self) -> Option<&Tiles> {
        self.tiles.as_ref()
    }

    fn aggregator_type(&self) -> AggregatorType {
        AggregatorType::PlainAccumulator
    }

    fn get_relevant_buckets(&self) -> Vec<BucketRange> {
        self.entry_plans.iter().map(|(_, w)| *w).collect()
    }
}

fn run_plain_aggregation(
    sorted_bucket_view: &std::collections::HashMap<Timestamp, SortedBucketView>,
    entry_plans: &[(BucketRange, BucketRange)],
    cursor_bounds: Option<(Option<Cursor>, Cursor)>,
    window_expr: &Arc<dyn WindowExpr>,
    tiles: Option<&Tiles>,
    _ts_column_index: usize,
    window_id: usize,
    bucket_granularity: TimeGranularity,
) -> (Vec<ScalarValue>, Option<AccumulatorState>) {
    if entry_plans.is_empty() {
        return (vec![], None);
    };

    let window_frame = window_expr.get_window_frame();
    let is_rows = window_frame.units == WindowFrameUnits::Rows;
    let window_length = get_window_length_ms(window_frame);
    let window_size = get_window_size_rows(window_frame);
    let spec = if is_rows {
        window_logic::WindowSpec::Rows { size: window_size }
    } else {
        window_logic::WindowSpec::Range { length_ms: window_length }
    };

    let mut results: Vec<ScalarValue> = Vec::new();

    // Each entry plan is evaluated within its own window range.
    for (entries_range, window_range) in entry_plans {
        let idx = RowIndex::new(*window_range, sorted_bucket_view, window_id, bucket_granularity);
        if idx.is_empty() {
            continue;
        }

        match cursor_bounds {
            None => {
                // Default mode: iterate all rows in entry buckets.
                let mut bucket_ts = entries_range.start;
                while bucket_ts <= entries_range.end {
                    let view = idx.bucket_view(&bucket_ts).expect("bucket view should exist");
                    let rows = view.size();

                    for row in 0..rows {
                        let window_end = RowPtr::new(bucket_ts, row);
                        if window_end.bucket_ts < window_range.start || window_end.bucket_ts > window_range.end {
                            continue;
                        }

                        let unclamped_start = window_logic::window_start_unclamped(&idx, window_end, spec);
                        let window_start = if unclamped_start.bucket_ts < window_range.start {
                            RowPtr::new(window_range.start, 0)
                        } else {
                            unclamped_start
                        };

                        let (front_args, middle_tiles, back_args) = if let Some(tiles) = tiles {
                            if let Some(split) =
                                window_logic::tiled_split(&idx, window_start, window_end, tiles)
                            {
                                let front_args = if split.front_end >= window_start {
                                    idx.get_args_in_range(&window_start, &split.front_end)
                                } else {
                                    vec![]
                                };
                                let back_args = if split.back_start <= window_end {
                                    idx.get_args_in_range(&split.back_start, &window_end)
                                } else {
                                    vec![]
                                };
                                (front_args, split.tiles, back_args)
                            } else {
                                (idx.get_args_in_range(&window_start, &window_end), vec![], vec![])
                            }
                        } else {
                            (idx.get_args_in_range(&window_start, &window_end), vec![], vec![])
                        };

                        let result =
                            run_plain_accumulator(window_expr, front_args, middle_tiles, back_args);
                        results.push(result);
                    }

                    bucket_ts = bucket_granularity.next_start(bucket_ts);
                }
            }
            Some((prev_processed_until, new_processed_until)) => {
                // Delta mode: iterate only the new update rows between prev and new cursor bounds,
                // but restrict outputs to this plan's entry bucket range.
                let Some(mut update_pos) = window_logic::first_update_pos(&idx, prev_processed_until) else {
                    continue;
                };

                let end_pos = {
                    let first_row_pos = idx.get_row_pos(&idx.first_pos());
                    if new_processed_until < first_row_pos {
                        continue;
                    }
                    match idx.seek_rowpos_gt(new_processed_until) {
                        Some(after_end) => idx
                            .prev_pos(after_end)
                            .expect("seek_rowpos_gt returned the first row; should be guarded above"),
                        None => idx.last_pos(),
                    }
                };

                if end_pos < update_pos {
                    continue;
                }

                loop {
                    if update_pos.bucket_ts >= entries_range.start && update_pos.bucket_ts <= entries_range.end {
                        let window_end = update_pos;

                        let unclamped_start = window_logic::window_start_unclamped(&idx, window_end, spec);
                        let window_start = if unclamped_start.bucket_ts < window_range.start {
                            RowPtr::new(window_range.start, 0)
                        } else {
                            unclamped_start
                        };

                        let (front_args, middle_tiles, back_args) = if let Some(tiles) = tiles {
                            if let Some(split) =
                                window_logic::tiled_split(&idx, window_start, window_end, tiles)
                            {
                                let front_args = if split.front_end >= window_start {
                                    idx.get_args_in_range(&window_start, &split.front_end)
                                } else {
                                    vec![]
                                };
                                let back_args = if split.back_start <= window_end {
                                    idx.get_args_in_range(&split.back_start, &window_end)
                                } else {
                                    vec![]
                                };
                                (front_args, split.tiles, back_args)
                            } else {
                                (idx.get_args_in_range(&window_start, &window_end), vec![], vec![])
                            }
                        } else {
                            (idx.get_args_in_range(&window_start, &window_end), vec![], vec![])
                        };

                        let result =
                            run_plain_accumulator(window_expr, front_args, middle_tiles, back_args);
                        results.push(result);
                    }

                    if update_pos == end_pos {
                        break;
                    }
                    update_pos = idx
                        .next_pos(update_pos)
                        .expect("end_pos should be reachable from update_pos");
                }
            }
        }
    }

    (results, None)
}

fn run_plain_accumulator(
    window_expr: &Arc<dyn WindowExpr>,
    front_args: Vec<arrow::array::ArrayRef>,
    middle_tiles: Vec<Tile>,
    back_args: Vec<arrow::array::ArrayRef>,
) -> ScalarValue {
    let mut accumulator = match create_window_aggregator(window_expr) {
        WindowAggregator::Accumulator(accumulator) => accumulator,
        WindowAggregator::Evaluator(_) => panic!("PlainAggregation should not use evaluator"),
    };

    if !front_args.is_empty() {
        accumulator.update_batch(&front_args).expect("update_batch failed");
    }

    for tile in middle_tiles {
        if let Some(tile_state) = tile.accumulator_state {
            merge_accumulator_state(accumulator.as_mut(), tile_state.as_ref());
        }
    }

    if !back_args.is_empty() {
        accumulator.update_batch(&back_args).expect("update_batch failed");
    }

    accumulator.evaluate().expect("evaluate failed")
}
