use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, TimestampMillisecondArray};
use datafusion::physical_plan::WindowExpr;

use crate::runtime::operators::window::batch_index::Bucket;
use crate::runtime::operators::window::tiles::{Tile, Tiles};
use crate::storage::batch_store::{BatchId, Timestamp};
use crate::runtime::operators::window::tiles::TimeGranularity;

use super::arrow_utils::sort_batch_by_timestamp;

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub struct Pos {
    pub bucket_ts: Timestamp,
    pub row: usize,
}

impl Pos {
    pub fn new(bucket_ts: Timestamp, row: usize) -> Self {
        Self { bucket_ts, row }
    }
}

pub struct WindowCoverage {
    pub start: Pos,
    pub end: Pos,
    pub tiles: Option<(Pos, Vec<Tile>, Pos)>, // pos before first tile, tiles, pos after last tile
}

impl WindowCoverage {
    pub fn new(start: Pos, end: Pos) -> Self {
        Self {
            start,
            end,
            tiles: None,
        }
    }
}

pub struct SortedBucketView {

}

impl SortedBucketView {

    pub fn size() -> usize {
        return 0;
    }

    pub fn timestamp(row: usize) -> Timestamp {
        panic!("Not implemented");
    }

    pub fn timestamps(&self) -> &TimestampMillisecondArray {
        panic!("Not implemented");
    }
}

/// Sorted and evaluated batches - owns the data
pub struct EntriesRange {
    /// Sorted batches (one per bucket, in timestamp order)
    // bucket_batches: Vec<RecordBatch>,
    // /// Pre-evaluated args per bucket
    // bucket_args: Vec<Vec<ArrayRef>>,
    // /// Row count per bucket
    // rows_per_bucket: Vec<usize>,
    /// Total rows
    // total_rows: usize,
    /// Timestamp column index
    bucket_start_ts: Timestamp,
    bucket_end_ts: Timestamp,
    sorted_buckets_view: HashMap<Timestamp, SortedBucketView>, 
    // ts_column_index: usize,

    // bucket_timestamps: BTreeSet<Timestamp>,
    // bucket_positions: HashMap<Timestamp, usize>,
    /// Bucket granularity
    bucket_granularity: TimeGranularity,
}

impl EntriesRange {
    pub fn advance_window(
        &self,
        window_length: i64,
        is_rows: bool,
        tiles: Option<&Tiles>,
        coverage: &mut WindowCoverage,
    ) {
        // Advance window end (always row-based forward movement)
        self.advance(&mut coverage.end);

        // Compute new start position
        if is_rows {
            // row based also advances by one
            self.advance(&mut coverage.start);
        } else {
            // time-based window advances by window_length
            let end_ts = self.get_timestamp(&coverage.end);
            let target_ts = end_ts - window_length;
            coverage.start = self.find_pos(target_ts, true).expect("Should be able to find position for target timestamp");
        };

        if tiles.is_none() {
            return
        }

        let should_recalculate_tiled_coverage =

            if let Some((front_pos, _, back_pos)) = coverage.tiles {   
                // we crossed first tile
                front_pos <= coverage.start ||
                {
                    // or we have enough space for a new tile in the back
                    let back_padding_start_ts = self.get_timestamp(&back_pos);
                    let end_ts = self.get_timestamp(&coverage.end);
                    end_ts - back_padding_start_ts >= tiles.unwrap().granularities().first().unwrap().to_millis()
                }
            } else {
                // we had no tiles so far, check if we should try calculate
                let start_ts = self.get_timestamp(&coverage.start);
                let end_ts = self.get_timestamp(&coverage.end);

                // at least one smallest tile should fit
                end_ts - start_ts >= tiles.unwrap().granularities().first().unwrap().to_millis()
            };

        if should_recalculate_tiled_coverage {
            // recalculate coverage
            coverage.tiles = self.get_tiled_coverage(&coverage.start, &coverage.end, tiles);
        }
    }

    pub fn get_tiled_coverage(
        &self,
        start: &Pos,
        end: &Pos,
        tiles: Option<&Tiles>,
    ) -> Option<(Pos, Vec<Tile>, Pos)> {

        if tiles.is_none() {
            return None;
        }

        let tiles = tiles.unwrap();

        let start_ts = self.get_timestamp(start);
        let end_ts = self.get_timestamp(end);

        // Since tiles cover rows with same timestamps fully, we do not want to include start and end timestamps in case we may have duplicate ts
        let tiles_start_ts = start_ts + 1;
        let tiles_end_ts = end_ts - 1;

        if tiles_start_ts <= tiles_end_ts { 
            return None;
        }

        let tiles = tiles.get_tiles_for_range(tiles_start_ts, tiles_end_ts);

        if tiles.is_empty() {
            return None;
        }

        let first_tile_start_ts = tiles[0].tile_start;
        let last_tile_end_ts = tiles[tiles.len() - 1].tile_end;

        let first_tile_pos = self.find_pos(first_tile_start_ts, true).unwrap();
        let pos_before_first_tile = self.pos_n_rows(&first_tile_pos, 1, true);

        let last_tile_pos = self.find_pos(last_tile_end_ts, false).unwrap();
        let pos_after_last_tile = self.pos_n_rows(&last_tile_pos, 1, false);

        Some((pos_before_first_tile, tiles, pos_after_last_tile))
    }

    // pub fn pos_time_before(&self, pos: &Pos, time_diff: i64) -> Pos {
    //     if pos.batch == 0 && pos.row == 0 {
    //         return *pos;
    //     }
        
    //     // TODO why prev pos?
    //     let prev_pos = self.pos_n_rows_before(pos, 1);
    //     let current_ts = self.get_timestamp(&prev_pos);
    //     let target_ts = current_ts - time_diff;
        
    //     // TODO this should go back instead of going from start
    //     self.find_first(target_ts, FindMode::GreaterOrEqual, &Pos::new(0,0))
    // }

    /// Create from buckets - sorts and evaluates all data
    pub fn new(
        // buckets: &[Bucket],
        // loaded_batches: &HashMap<BatchId, RecordBatch>,
        bucket_start_ts: Timestamp,
        bucket_end_ts: Timestamp,
        sorted_buckets_view: HashMap<Timestamp, SortedBucketView>,
        // window_expr: &Arc<dyn WindowExpr>,
        // ts_column_index: usize,
        bucket_granularity: TimeGranularity,
    ) -> Self {
        // let mut batches = Vec::new();
        // let mut bucket_args = Vec::new();
        // let mut bucket_rows = Vec::new();
        // let mut bucket_positions = HashMap::new();
        // let mut bucket_timestamps = BTreeSet::new();
        
        // sort buckets by timestamp
        // buckets.sort_by_key(|b| b.timestamp);
        
        // for bucket in buckets {
        //     let bucket_batches: Vec<&RecordBatch> = bucket.batches.iter()
        //         .filter_map(|m| loaded_batches.get(&m.batch_id))
        //         .collect();

        //     // TODO add __batch_id column to the batches
            
        //     if bucket_batches.is_empty() {
        //         continue;
        //     }
            
        //     // Sort within bucket
        //     let sorted = if bucket_batches.len() == 1 {
        //         sort_batch_by_timestamp(bucket_batches[0], ts_column_index)
        //     } else {
        //         let schema = bucket_batches[0].schema();
        //         let concatenated = arrow::compute::concat_batches(&schema, bucket_batches)
        //             .expect("Should be able to concatenate batches within bucket");
        //         sort_batch_by_timestamp(&concatenated, ts_column_index)
        //     };
            
        //     // Evaluate args
        //     let args = window_expr.evaluate_args(&sorted)
        //         .expect("Should be able to evaluate window args");
            
        //     bucket_rows.push(sorted.num_rows());
        //     bucket_args.push(args);
        //     batches.push(sorted);
        //     bucket_positions.insert(bucket.timestamp, batches.len() - 1);
        //     bucket_timestamps.insert(bucket.timestamp);
        // }
        
        // let total_rows = bucket_rows.iter().sum();
        
        // Self {
        //     bucket_batches: batches,
        //     bucket_args,
        //     rows_per_bucket: bucket_rows,
        //     total_rows,
        //     ts_column_index,
        //     bucket_positions,
        //     bucket_timestamps,
        //     bucket_granularity
        // }
        Self {bucket_start_ts, bucket_end_ts, sorted_buckets_view, bucket_granularity}
    }
    
    // pub fn get_batch(&self, timestamp: Timestamp) -> Option<(&RecordBatch, usize)> {
    //     if let Some(pos) = self.bucket_positions.get(&timestamp) {
    //         Some((&self.bucket_batches[*pos], *pos))
    //     } else {
    //         None
    //     }
    // }

    // pub fn is_empty(&self) -> bool {
    //     self.total_rows == 0
    // }
    
    // pub fn total_rows(&self) -> usize {
    //     self.total_rows
    // }
    
    // pub fn num_batches(&self) -> usize {
    //     self.bucket_batches.len()
    // }
    
    pub fn first_pos(&self) -> Pos {
        Pos::new(self.bucket_start_ts, 0)
    }
    
    pub fn last_pos(&self) -> Pos {
        // if self.bucket_batches.is_empty() || self.total_rows == 0 {
        //     return Pos::new(0, 0);
        // }
        // let last_batch = self.bucket_batches.len() - 1;
        // Pos::new(last_batch, self.rows_per_bucket[last_batch].saturating_sub(1))
        let last_bucket = self.sorted_buckets_view.get(&self.bucket_end_ts).expect("should exist");
        Pos::new(self.bucket_end_ts, last_bucket.size() - 1);
    }
    
    fn get_timestamp(&self, pos: &Pos) -> Timestamp {
        // let batch = &self.bucket_batches[pos.bucket];
        // let ts_column = batch.column(self.ts_column_index);
        // let ts_array = ts_column.as_any().downcast_ref::<TimestampMillisecondArray>()
        //     .expect("Timestamp column should be TimestampMillisecondArray");
        // ts_array.value(pos.row)
        self.sorted_buckets_view.get(&pos.bucket_ts).expect("should exist").timestamp(pos.row);
    }
    
    // pub fn get_row_args(&self, pos: &Pos) -> Vec<arrow::array::ArrayRef> {
    //     self.bucket_args[pos.bucket].iter()
    //         .map(|arr| arr.slice(pos.row, 1))
    //         .collect()
    // }
    
    pub fn can_advance(&self, pos: &Pos) -> bool {
        if pos.bucket_ts > self.bucket_end_ts {
            return false;
        }
        if pos.bucket_ts < self.bucket_end_ts {
            return true;
        }
        pos.row < *self.sorted_buckets_view.get(pos.bucket_ts).expect("should exist").size()
    }
    
    pub fn advance(&self, pos: &mut Pos) {
        pos.row += 1;
        if pos.row >= *self.sorted_buckets_view.get(pos.bucket_ts).expect("should exist") {
            pos.bucket_ts = self.bucket_granularity.next_start(pos.bucket_ts);
            pos.row = 0;
        }
    }
    
    /// Get args for a range of positions (inclusive start, exclusive end)
    pub fn get_args_in_range(&self, _start: &Pos, _end: &Pos) -> Vec<arrow::array::ArrayRef> {
        // TODO
        // for each batch between start and end, get args for rows between start and end
        // use simple slicing to get args within batch (if batch is fully covered, simply copy whole args vec)
        // final result should be concatenated args from all batches
        vec![]
    }
    
    /// Count rows between positions (inclusive)
    pub fn count_between(&self, start: &Pos, end: &Pos) -> usize {
        if start.bucket_ts == end.bucket_ts {
            return end.row - start.row + 1;
        }
        let mut count = self.sorted_buckets_view.get(start.bucket_ts).expect("should exist").size() - start.row;
        for bucket_ts in (self.bucket_granularity.next_start(start.bucket_ts)..end.bucket_ts).step_by(self.bucket_granularity.to_millis()) {
            count += self.sorted_buckets_view.get(bucket_ts).expect("should exist").size();
        }
        count += end.row + 1;
        count
    }
    
    /// Get position N rows before/after a given position
    /// TODO make sure that if we go out of bound we return either first (before) or last (after) position
    pub fn pos_n_rows(&self, pos: &Pos, n: usize, before: bool) -> Pos {
        if n == 0 {
            return *pos;
        }

        let mut current_bucket_ts = pos.bucket_ts;
        let mut current_row = pos.row;
        let mut remaining = n;

        while remaining > 0 && current_bucket_ts <= self.bucket_end_ts {
            let rows_in_current_bucket = self.sorted_buckets_view.get(current_bucket_ts).expect("should exist").size();
            
            let (can_satisfy_in_bucket, available) = if before {
                (current_row >= remaining, current_row)
            } else {
                let available_in_bucket = rows_in_current_bucket - current_row;
                (available_in_bucket >= remaining, available_in_bucket)
            };

            if can_satisfy_in_bucket {
                if before {
                    current_row -= remaining;
                } else {
                    current_row += remaining;
                }
                remaining = 0;
            } else {
                if before {
                    remaining -= current_row + 1;
                    if current_bucket_ts == self.bucket_start_ts {
                        current_row = 0;
                        break;
                    }
                    current_bucket_ts = self.bucket_granularity.prev_start(current_bucket_ts);
                    current_row = self.sorted_buckets_view.get(current_bucket_ts).expect("should exist").size() - 1;
                } else {
                    remaining -= available;
                    current_bucket_ts = self.bucket_granularity.next_start(current_bucket_ts);
                    if current_bucket_ts > self.bucket_end_ts {
                        current_bucket_ts = self.bucket_end_ts;
                        current_row = self.sorted_buckets_view.get(current_bucket_ts).expect("should exist").size() - 1;
                        break;
                    }
                    current_row = 0; // first row in the next bucket
                }
            }
        }

        Pos::new(current_bucket_ts, current_row)
    }
    
    // TODO should be similar to pos_n_rows_before
    // pub fn pos_from_end(&self, rows_from_end: usize) -> Pos {
    //     if rows_from_end == 0 || rows_from_end > self.total_rows {
    //         return self.last_pos();
    //     }
    //     let target_global = self.total_rows - rows_from_end;
    //     let mut accumulated = 0;
    //     for batch_idx in 0..self.bucket_batches.len() {
    //         let batch_rows = self.rows_per_bucket[batch_idx];
    //         if accumulated + batch_rows > target_global {
    //             return Pos::new(batch_idx, target_global - accumulated);
    //         }
    //         accumulated += batch_rows;
    //     }
    //     panic!("pos_from_end failed");
    // }
    
    /// Find  position where timestamp matches condition
    /// If multiple positions match, returns based on first_or_last
    pub fn find_pos(&self, target_ts: Timestamp, first_or_last: bool) -> Option<Pos> {
        let mut bucket_ts = self.bucket_granularity.start(target_ts);
        // let bucket_idx = if let Some(bucket_idx) = self.bucket_positions.get(&batch_start_ts) {
        //     *bucket_idx
        // } else { 
        //     // lookup smallest bucket timestamp larger than batch_start_ts
        //     let closest_bucket_ts = self.bucket_timestamps.range(batch_start_ts..).next().unwrap();
        //     if let Some(bucket_idx) = self.bucket_positions.get(closest_bucket_ts) {
        //         *bucket_idx
        //     } else {
        //         return None;
        //     }
        // };

        if bucket_ts < self.bucket_start_ts {
            bucket_ts = self.bucket_start_ts;
        } else if bucket_ts > self.bucket_end_ts {
            panic!("target_ts is after the last bucket");
        }

        let timestamps = self.sorted_buckets_view.get(bucket_ts).expect("should exist").timestamps();
        // let timestamps_array = timestamps.as_any().downcast_ref::<TimestampMillisecondArray>()
        //     .expect("Timestamp column should be TimestampMillisecondArray");

        // timestamps_array is sorted, binary search target_ts position
        let row_idx = if first_or_last {
            timestamps.values().partition_point(|x| *x < target_ts)
        } else {
            timestamps.values().partition_point(|x| *x <= target_ts)
        };

        if row_idx == timestamps.values().len() {
            return None;
        }
        Some(Pos::new(bucket_ts, row_idx))
    }
}

/// Sort batches within each bucket and return vec of sorted batches (one per bucket, ordered by bucket timestamp)
pub fn sort_buckets(
    buckets: &[Bucket],
    batches: &HashMap<BatchId, RecordBatch>,
    ts_column_index: usize,
) -> Vec<RecordBatch> {
    let mut result = Vec::new();
    
    for bucket in buckets {
        let bucket_batches: Vec<&RecordBatch> = bucket.batches.iter()
            .filter_map(|m| batches.get(&m.batch_id))
            .collect();
        
        if bucket_batches.is_empty() {
            continue;
        }
        
        if bucket_batches.len() == 1 {
            result.push(sort_batch_by_timestamp(bucket_batches[0], ts_column_index));
        } else {
            // TODO if we pre-sort batches during insertion, we can just merge here instead of sorting again
            let schema = bucket_batches[0].schema();
            let concatenated = arrow::compute::concat_batches(&schema, bucket_batches)
                .expect("Should be able to concatenate batches within bucket");
            result.push(sort_batch_by_timestamp(&concatenated, ts_column_index));
        }
    }
    
    result
}
