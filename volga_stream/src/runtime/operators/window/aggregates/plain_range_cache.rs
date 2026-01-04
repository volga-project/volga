use std::sync::Arc;

use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggregates::{create_window_aggregator, merge_accumulator_state, WindowAggregator};
use crate::runtime::operators::window::state::index::{RowPtr, SortedRangeIndex};
use crate::runtime::operators::window::state::index::window_logic;
use crate::runtime::operators::window::state::tiles::Tile;
use crate::runtime::operators::window::Tiles;
use crate::storage::batch_store::Timestamp;

#[derive(Default)]
pub(super) struct WindowSlicesCache {
    split: Option<CachedTiledSplit>,
    #[cfg(test)]
    stats: CacheStats,
}

#[cfg(test)]
#[derive(Default, Debug, Clone, Copy)]
struct CacheStats {
    full_recomputes: usize,
    tail_fetches: usize,
    front_drops: usize,
}

struct CachedTiledSplit {
    first_tile_start_ts: Timestamp,
    last_tile_end_ts: Timestamp,
    front_end: RowPtr,
    back_start: RowPtr,
    tiles: Vec<Tile>,
}

impl WindowSlicesCache {
    fn apply_stored_window(
        &mut self,
        accumulator: &mut dyn datafusion::logical_expr::Accumulator,
        row_index: &SortedRangeIndex,
        start: RowPtr,
        end: RowPtr,
        tiles: Option<&Tiles>,
    ) {
        let Some(tiles) = tiles else {
            let args = row_index.get_args_in_range(&start, &end);
            if !args.is_empty() {
                accumulator.update_batch(&args).expect("update_batch failed");
            }
            return;
        };

        let start_ts = row_index.get_timestamp(&start);
        let end_ts = row_index.get_timestamp(&end);

        // Try to reuse/update an existing split; fall back to full recompute if needed.
        let mut split = if let Some(s) = self.split.take() {
            s
        } else {
            #[cfg(test)]
            {
                self.stats.full_recomputes += 1;
            }
            match window_logic::tiled_split(row_index, start, end, tiles) {
                Some(s) => CachedTiledSplit {
                    first_tile_start_ts: s.tiles[0].tile_start,
                    last_tile_end_ts: s.tiles[s.tiles.len() - 1].tile_end,
                    front_end: s.front_end,
                    back_start: s.back_start,
                    tiles: {
                        let mut t = s.tiles;
                        t.sort_by_key(|x| (x.tile_start, x.tile_end));
                        t
                    },
                },
                None => {
                    let args = row_index.get_args_in_range(&start, &end);
                    if !args.is_empty() {
                        accumulator.update_batch(&args).expect("update_batch failed");
                    }
                    return;
                }
            }
        };

        // Drop tiles that are no longer strictly inside (start moved forward).
        if !split.tiles.is_empty() && split.tiles[0].tile_start <= start_ts {
            let mut drop = 0usize;
            for t in &split.tiles {
                if t.tile_start <= start_ts {
                    drop += 1;
                } else {
                    break;
                }
            }
            if drop > 0 {
                #[cfg(test)]
                {
                    self.stats.front_drops += 1;
                }
                split.tiles.drain(0..drop);
            }
        }
        if split.tiles.is_empty() {
            self.split = None;
            let args = row_index.get_args_in_range(&start, &end);
            if !args.is_empty() {
                accumulator.update_batch(&args).expect("update_batch failed");
            }
            return;
        }

        split.first_tile_start_ts = split.tiles[0].tile_start;

        // Extend tiles when the window end moved forward enough.
        let min_tile_ms = tiles.min_granularity_ms();
        if end_ts >= split.last_tile_end_ts + min_tile_ms {
            #[cfg(test)]
            {
                self.stats.tail_fetches += 1;
            }
            let mut tail = tiles.get_tiles_for_range(split.last_tile_end_ts, end_ts);
            tail.retain(|t| t.tile_start > start_ts && t.tile_end <= end_ts);
            if !tail.is_empty() {
                tail.sort_by_key(|t| (t.tile_start, t.tile_end));

                // Ensure disjoint append (should already hold due to start at last_tile_end_ts).
                let mut last_end = split.last_tile_end_ts;
                for t in tail {
                    if t.tile_start < last_end {
                        continue;
                    }
                    last_end = t.tile_end;
                    split.tiles.push(t);
                }
                split.last_tile_end_ts = last_end;
            }
        }

        // Recompute front/back cut row ptrs from current tile boundaries.
        let Some(first_tile_pos) = row_index.seek_ts_ge(split.first_tile_start_ts) else {
            self.split = None;
            let args = row_index.get_args_in_range(&start, &end);
            if !args.is_empty() {
                accumulator.update_batch(&args).expect("update_batch failed");
            }
            return;
        };
        let Some(front_end) = row_index.prev_pos(first_tile_pos) else {
            self.split = None;
            let args = row_index.get_args_in_range(&start, &end);
            if !args.is_empty() {
                accumulator.update_batch(&args).expect("update_batch failed");
            }
            return;
        };
        let Some(back_start) = row_index.seek_ts_ge(split.last_tile_end_ts) else {
            self.split = None;
            let args = row_index.get_args_in_range(&start, &end);
            if !args.is_empty() {
                accumulator.update_batch(&args).expect("update_batch failed");
            }
            return;
        };

        split.front_end = front_end;
        split.back_start = back_start;

        if split.front_end < start || split.back_start > end {
            self.split = Some(split);
            let args = row_index.get_args_in_range(&start, &end);
            if !args.is_empty() {
                accumulator.update_batch(&args).expect("update_batch failed");
            }
            return;
        }

        if split.front_end >= start {
            let front_args = row_index.get_args_in_range(&start, &split.front_end);
            if !front_args.is_empty() {
                accumulator
                    .update_batch(&front_args)
                    .expect("update_batch failed");
            }
        }

        for tile in &split.tiles {
            if let Some(tile_state) = &tile.accumulator_state {
                merge_accumulator_state(accumulator, tile_state.as_ref());
            }
        }

        if split.back_start <= end {
            let back_args = row_index.get_args_in_range(&split.back_start, &end);
            if !back_args.is_empty() {
                accumulator
                    .update_batch(&back_args)
                    .expect("update_batch failed");
            }
        }

        self.split = Some(split);
    }
}

#[cfg(test)]
impl WindowSlicesCache {
    fn stats(&self) -> CacheStats {
        self.stats
    }
}

pub(super) fn eval_stored_window_cached(
    window_expr: &Arc<dyn WindowExpr>,
    row_index: &SortedRangeIndex,
    start: RowPtr,
    end: RowPtr,
    tiles: Option<&Tiles>,
    cache: &mut WindowSlicesCache,
) -> ScalarValue {
    let mut accumulator = match create_window_aggregator(window_expr) {
        WindowAggregator::Accumulator(accumulator) => accumulator,
        WindowAggregator::Evaluator(_) => panic!("PlainAggregation should not use evaluator"),
    };
    cache.apply_stored_window(accumulator.as_mut(), row_index, start, end, tiles);
    accumulator.evaluate().expect("evaluate failed")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::logical_expr::WindowFrameUnits;

    use crate::runtime::operators::window::aggregates::test_utils;
    use crate::runtime::operators::window::aggregates::BucketRange;
    use crate::runtime::operators::window::state::index::{DataBounds, DataRequest, SortedRangeIndex};
    use crate::runtime::operators::window::state::tiles::TileConfig;
    use crate::runtime::operators::window::{Tiles, TimeGranularity};

    use super::{eval_stored_window_cached, window_logic, WindowSlicesCache};

    #[tokio::test]
    async fn test_window_slices_cache_reuses_tiled_split_and_updates_incrementally() {
        let sql = "SELECT \
            timestamp, value, partition_key, \
            SUM(value) OVER (PARTITION BY partition_key ORDER BY timestamp \
            RANGE BETWEEN INTERVAL '4000' MILLISECOND PRECEDING AND CURRENT ROW) as sum_val \
        FROM test_table";
        let window_expr = test_utils::window_expr_from_sql(sql).await;

        // Dense data so every tile exists.
        let base_ts: i64 = 0;
        let mut rows: Vec<(i64, f64, &str, u64)> = Vec::new();
        for i in 0u64..120 {
            rows.push((base_ts + (i as i64) * 100, 1.0, "a", i));
        }
        let batch = test_utils::batch(&rows);

        let config = TileConfig::new(vec![TimeGranularity::Seconds(1)]).expect("tile config");
        let mut tiles = Tiles::new(config);
        tiles.add_batch(&batch, &window_expr, 0);

        let request = DataRequest {
            bucket_range: BucketRange::new(0, 0),
            bounds: DataBounds::All,
        };
        let view = test_utils::make_view(
            TimeGranularity::Seconds(1),
            request,
            vec![(0, batch)],
            &window_expr,
        );
        let idx = SortedRangeIndex::new(&view);
        assert!(!idx.is_empty());

        // Start from a point where tiling is definitely useful.
        let start_end_ts = 6_000;
        let end_end_ts = 11_000;
        let mut pos = idx.seek_ts_ge(start_end_ts).expect("pos at start_end_ts");
        let end_pos = idx.seek_ts_ge(end_end_ts).expect("pos at end_end_ts");

        let spec = window_logic::WindowSpec::Range { length_ms: 4_000 };
        assert_eq!(window_expr.get_window_frame().units, WindowFrameUnits::Range);

        let mut cache = WindowSlicesCache::default();
        let mut iters: usize = 0;
        loop {
            let start = window_logic::window_start_unclamped(&idx, pos, spec);
            let _v =
                eval_stored_window_cached(&Arc::clone(&window_expr), &idx, start, pos, Some(&tiles), &mut cache);
            iters += 1;

            if pos == end_pos {
                break;
            }
            pos = idx.next_pos(pos).expect("next pos");
        }

        let stats = cache.stats();
        // We should build the tiled split only once, then reuse/update it incrementally.
        assert_eq!(stats.full_recomputes, 1);
        // Tail fetch should only happen when we cross at least one min tile boundary (1s).
        assert!(
            stats.tail_fetches <= (iters / 5) + 2,
            "tail_fetches too high: {stats:?}, iters={iters}"
        );
        // Start moves forward; we should drop front tiles occasionally.
        assert!(stats.front_drops > 0);
    }
}

