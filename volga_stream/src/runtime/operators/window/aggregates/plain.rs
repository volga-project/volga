use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch};
use datafusion::logical_expr::WindowFrameUnits;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use async_trait::async_trait;

use tokio_rayon::rayon::ThreadPool;
use tokio_rayon::AsyncThreadPool;

use crate::runtime::operators::window::window_operator_state::AccumulatorState;
use crate::runtime::operators::window::tiles::Tile;
use crate::runtime::operators::window::batch_index::{BatchIndex, Bucket, get_window_length_ms, get_window_size_rows};
use crate::runtime::operators::window::Tiles;
use crate::storage::batch_store::{BatchId, Timestamp};
use indexmap::IndexSet;

use super::{Aggregation, AggregatorType, WindowAggregator, create_window_aggregator, merge_accumulator_state};
use super::utils::{EntriesRange, Pos};

/// Entry bucket info with timestamp and batch IDs
#[derive(Debug, Clone)]
pub struct BucketRange {
    pub start_bucket_ts: Timestamp,
    pub end_bucket_ts: Timestamp,
    pub exact_batch_ids: HashMap<Timestamp, Vec<BatchId>>,
}

#[derive(Debug)]
pub struct PlainAggregation {
    pub entry_bucket_ranges: Vec<BucketRange>,
    /// All relevant buckets covering windows for all entries
    pub window_expr: Arc<dyn WindowExpr>,
    pub tiles: Option<Tiles>,
    pub ts_column_index: usize,
}

impl PlainAggregation {
    /// Create PlainAggregation from batch IDs
    pub fn new(
        entry_bucket_ranges: Vec<BucketRange>,
        window_expr: Arc<dyn WindowExpr>,
        tiles: Option<Tiles>,
        batch_index: &BatchIndex,
        ts_column_index: usize,
    ) -> Self {
        // Group entry batch IDs by their time buckets
        // let grouped = Self::group_batch_ids_by_time_bucket(&entry_batch_ids);
        
        // let entry_bucket_timestamps: Vec<Timestamp> = grouped.iter()
        //     .map(|(ts, _)| *ts)
        //     .collect();
        
        let window_frame = window_expr.get_window_frame();
        
        // Get all relevant buckets for all entries
        let relevant_buckets: Vec<Bucket> = match window_frame.units {
            WindowFrameUnits::Range => {
                let window_length = get_window_length_ms(window_frame);
                batch_index.get_relevant_buckets_for_range_windows(&entry_bucket_timestamps, window_length)
                    .into_iter().cloned().collect()
            }
            WindowFrameUnits::Rows => {
                let window_size = get_window_size_rows(window_frame);
                batch_index.get_relevant_buckets_for_rows_windows(&entry_bucket_timestamps, window_size)
                    .into_iter().cloned().collect()
            }
            WindowFrameUnits::Groups => {
                panic!("GROUPS window frame not supported");
            }
        };
        
        // Create entry bucket infos
        let entry_bucket_infos: Vec<EntryBucketInfo> = grouped.into_iter()
            .map(|(timestamp, batch_ids)| EntryBucketInfo { timestamp, batch_ids })
            .collect();
        
        // Compose batches to load
        let batches_to_load = Self::get_batches_to_load(&relevant_buckets, tiles.as_ref());
        
        Self {
            entry_batch_ids,
            entry_bucket_infos,
            relevant_buckets,
            window_expr,
            tiles,
            batches_to_load,
            ts_column_index,
        }
    }
    
    /// Group batch IDs by their time_bucket, return sorted by timestamp
    // fn group_batch_ids_by_time_bucket(batch_ids: &[BatchId]) -> Vec<(Timestamp, Vec<BatchId>)> {
    //     let mut bucket_map: HashMap<Timestamp, Vec<BatchId>> = HashMap::new();
        
    //     for &batch_id in batch_ids {
    //         let bucket_ts = batch_id.time_bucket() as Timestamp;
    //         bucket_map.entry(bucket_ts)
    //             .or_insert_with(Vec::new)
    //             .push(batch_id);
    //     }
        
    //     let mut result: Vec<_> = bucket_map.into_iter().collect();
    //     result.sort_by_key(|(ts, _)| *ts);
    //     result
    // }
    
    fn get_relevant_buckets(
        relevant_buckets: &[Bucket],
        tiles: Option<&Tiles>,
    ) -> Vec<BucketRange> {
        let mut relevant_buckets = Vec::new();
        
        for bucket in relevant_buckets {
            let bucket_start = bucket.timestamp;
            let bucket_end = bucket.batches.iter().map(|b| b.max_timestamp).max().unwrap_or(bucket_start);
            
            // TODO we should load batches only for buckets that move window
            let tiles_cover = tiles.map(|t| {
                let tile_coverage = t.compute_coverage(bucket_start, bucket_end);
                tile_coverage.is_full_coverage()
            }).unwrap_or(false);
            
            if !tiles_cover {
                for metadata in &bucket.batches {
                    batch_ids.insert(metadata.batch_id);
                }
            }
        }
        
        batch_ids
    }
}

#[async_trait]
impl Aggregation for PlainAggregation {
    async fn produce_aggregates(
        &self,
        batches: &HashMap<BatchId, RecordBatch>,
        thread_pool: Option<&ThreadPool>,
        exclude_current_row: Option<bool>,
    ) -> (Vec<ScalarValue>, Option<AccumulatorState>) {
        let parallelize = thread_pool.is_some();
        
        let aggregates = if parallelize {
            run_plain_aggregation_parallel(
                thread_pool.expect("ThreadPool should exist"),
                self.window_expr.clone(),
                self.entry_bucket_infos.clone(),
                self.relevant_buckets.clone(),
                self.tiles.clone(),
                Arc::new(batches.clone()),
                self.ts_column_index,
                exclude_current_row,
            ).await
        } else {
            run_plain_aggregation(
                &self.window_expr,
                &self.entry_bucket_infos,
                &self.relevant_buckets,
                self.tiles.as_ref(),
                batches,
                self.ts_column_index,
                exclude_current_row,
            )
        };
        
        (aggregates, None)
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
    
    fn get_batches_to_load(&self) -> IndexSet<BatchId> {
        self.batches_to_load.clone()
    }
    
    fn get_entry_batch_ids(&self) -> Vec<BatchId> {
        self.entry_batch_ids.clone()
    }
    
    fn get_entry_timestamps(&self) -> Vec<Timestamp> {
        self.entry_bucket_infos.iter()
            .flat_map(|info| std::iter::repeat(info.timestamp).take(info.batch_ids.len()))
            .collect()
    }
}

fn run_plain_aggregation(
    window_expr: &Arc<dyn WindowExpr>,
    entry_bucket_infos: &[EntryBucketInfo],
    relevant_buckets: &[Bucket],
    tiles: Option<&Tiles>,
    batches: &HashMap<BatchId, RecordBatch>,
    ts_column_index: usize,
    _exclude_current_row: Option<bool>,
) -> Vec<ScalarValue> {
    if relevant_buckets.is_empty() {
        return vec![];
    }
    
    let window_frame = window_expr.get_window_frame();
    let is_rows_window = window_frame.units == WindowFrameUnits::Rows;
    let window_length = get_window_length_ms(window_frame);
    let window_size = get_window_size_rows(window_frame);
    
    // Create EvaluatedBatches from all relevant buckets
    let all_data = EntriesRange::from_buckets(relevant_buckets, batches, window_expr, ts_column_index);
    
    if all_data.is_empty() {
        return vec![];
    }
    
    let mut results = Vec::new();
    
    for info in entry_bucket_infos {

        let (sorted_batch, batch_idx) = all_data.get_batch(info.timestamp);

        let mut window_end = Pos::new(batch_idx, 0);

        let mut window_start = if is_rows_window {
            all_data.pos_n_rows_before(&window_end, window_size)
        } else {
            all_data.pos_time_before(&window_end, window_length)
        };
        
        // Initialize incremental coverage
        let mut coverage = WindowCoverage::new(window_start, window_end);

        // iterate over sorted rows 
        for row in sorted_batch.rows() {
            // process only requested original batch_ids
            
            all_data.advance_window(
                &window_start, 
                &window_end, 
                if is_rows_window { window_size as i64 } else { window_length }, 
                is_rows_window, 
                tiles, 
                &mut coverage
            );
            
            if !info.batch_ids.contains(&row.batch_id) {
                window_start = new_window_start;
                window_end = new_window_end;
                continue;
            }

            let result = run_plain_accumulator(
                window_expr,
                &all_data,
                front_args,
                middle_tiles,
                back_args,
            );
            results.push(result);
            window_start = new_window_start;
            window_end = new_window_end;
        }
    }
    
    results
}

async fn run_plain_aggregation_parallel(
    thread_pool: &ThreadPool,
    window_expr: Arc<dyn WindowExpr>,
    entry_bucket_infos: Vec<EntryBucketInfo>,
    relevant_buckets: Vec<Bucket>,
    tiles: Option<Tiles>,
    batches: Arc<HashMap<BatchId, RecordBatch>>,
    ts_column_index: usize,
    exclude_current_row: Option<bool>,
) -> Vec<ScalarValue> {
    thread_pool.spawn_fifo_async(move || {
        run_plain_aggregation(
            &window_expr,
            &entry_bucket_infos,
            &relevant_buckets,
            tiles.as_ref(),
            &batches,
            ts_column_index,
            exclude_current_row,
        )
    }).await
}

fn run_plain_accumulator(
    window_expr: &Arc<dyn WindowExpr>,
    data: &EntriesRange,
    front_args: Vec<ArrayRef>,
    middle_tiles: Vec<Tile>,
    back_args: Vec<ArrayRef>,
) -> ScalarValue {
    let mut accumulator = match create_window_aggregator(window_expr) {
        WindowAggregator::Accumulator(accumulator) => accumulator,
        WindowAggregator::Evaluator(_) => panic!("Should not be evaluator"),
    };

    // Process front data
    if !front_args.is_empty() {
        accumulator.update_batch(&front_args)
            .expect("Should be able to update accumulator with front data");
    }

    // Process middle tiles
    for tile in middle_tiles {
        if let Some(tile_state) = tile.accumulator_state {
            merge_accumulator_state(accumulator.as_mut(), tile_state.as_ref());
        }
    }

    // Process back data
    if !back_args.is_empty() {
        accumulator.update_batch(&back_args)
            .expect("Should be able to update accumulator with back data");
    }

    accumulator.evaluate()
        .expect("Should be able to evaluate accumulator")
}
