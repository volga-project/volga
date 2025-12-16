use std::collections::{BTreeMap, HashSet};
use datafusion::logical_expr::{WindowFrame, WindowFrameUnits};
use crate::storage::batch_store::{BatchId, Timestamp};

/// Metadata stored per batch
#[derive(Debug, Clone)]
pub struct BatchMetadata {
    pub batch_id: BatchId,
    pub bucket_timestamp: Timestamp,
    pub min_timestamp: Timestamp,
    pub max_timestamp: Timestamp,
    pub row_count: usize,
}

/// A bucket containing batches with the same bucket timestamp
#[derive(Debug, Clone, Default)]
pub struct Bucket {
    pub timestamp: Timestamp,
    pub batches: Vec<BatchMetadata>,
    pub row_count: usize,
}

impl Bucket {
    pub fn new(timestamp: Timestamp) -> Self {
        Self { timestamp, batches: Vec::new(), row_count: 0 }
    }
    
    pub fn from_batches(timestamp: Timestamp, batches: Vec<BatchMetadata>) -> Self {
        let row_count = batches.iter().map(|b| b.row_count).sum();
        Self { timestamp, batches, row_count }
    }
    
    pub fn push(&mut self, metadata: BatchMetadata) {
        self.row_count += metadata.row_count;
        self.batches.push(metadata);
    }

    pub fn get_batch_idx(&self, batch_id: &BatchId) -> Option<usize> {
        for (i, batch) in self.batches.iter().enumerate() {
            if batch.batch_id == *batch_id {
                return Some(i);
            }
        }
        None
    }
}

/// Result of inserting a batch
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertResult {
    Normal,
    LateArrival,
}

/// Result of sliding the window - returns buckets for aggregation layer
#[derive(Debug)]
pub struct SlideInfo<'a> {
    pub update_buckets: Vec<&'a Bucket>,
    pub retract_buckets: Vec<&'a Bucket>,
    pub new_end_timestamp: Timestamp,
    /// Number of rows between retract's last bucket end and update's first bucket start
    /// Used for ROWS window exact retract calculation at aggregation layer
    pub row_distance: usize,
}

/// Batch-level index using bucket timestamps
#[derive(Debug, Clone)]
pub struct BatchIndex {
    buckets: BTreeMap<Timestamp, Bucket>, // TODO this can be hashmap for better performance
    total_rows: usize,
    max_timestamp_seen: Timestamp,
}

impl Default for BatchIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchIndex {
    pub fn new() -> Self {
        Self {
            buckets: BTreeMap::new(),
            total_rows: 0,
            max_timestamp_seen: i64::MIN,
        }
    }

    pub fn insert_batch(
        &mut self,
        batch_id: BatchId,
        min_ts: Timestamp,
        max_ts: Timestamp,
        row_count: usize,
    ) -> InsertResult {
        let bucket_ts = batch_id.time_bucket() as Timestamp;
        
        let metadata = BatchMetadata {
            batch_id,
            bucket_timestamp: bucket_ts,
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            row_count,
        };

        // TODO this is not correct, bucket is late any if any entry in batch is below max_timestamp_seen (or window_end?)
        let is_late = max_ts < self.max_timestamp_seen;

        self.buckets
            .entry(bucket_ts)
            .or_insert_with(|| Bucket::new(bucket_ts))
            .push(metadata);
        self.total_rows += row_count;
        self.max_timestamp_seen = self.max_timestamp_seen.max(max_ts);

        if is_late {
            InsertResult::LateArrival
        } else {
            InsertResult::Normal
        }
    }

    /// Query buckets with bucket_timestamp in range [start, end]
    pub fn query_buckets_in_range(&self, start: Timestamp, end: Timestamp) -> Vec<&Bucket> {
        self.buckets
            .range(start..=end)
            .map(|(_, bucket)| bucket)
            .collect()
    }
    
    /// Get buckets for RANGE windows given update bucket timestamps.
    /// For each update timestamp, finds all buckets that might contain window data
    /// (buckets with data in [timestamp - window_length_ms, timestamp])
    pub fn get_relevant_buckets_for_range_windows(
        &self,
        bucket_range: &BucketRange,
        window_length_ms: i64,
    ) -> Vec<&Bucket> {
    
        
        // Merge overlapping ranges
        let mut merged = vec![ranges[0]];
        for (start, end) in ranges.into_iter().skip(1) {
            let last = merged.last_mut().unwrap();
            if start <= last.1 {
                last.1 = last.1.max(end);
            } else {
                merged.push((start, end));
            }
        }
        
        // Query buckets for each merged range, deduplicate
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        
        for (start, end) in merged {
            for bucket in self.query_buckets_in_range(start, end) {
                if seen.insert(bucket.timestamp) {
                    result.push(bucket);
                }
            }
        }
        
        result
    }
    
    /// Get buckets for ROWS windows given update bucket timestamps.
    /// For each update timestamp, finds all buckets that might contain window data
    /// by going backwards and collecting window_size rows.
    pub fn get_relevant_buckets_for_rows_windows(
        &self,
        bucket_range: &BucketRange,
        window_size: usize,
    ) -> Vec<&Bucket> {
        
        let mut seen = HashSet::new();
        let mut result = Vec::new();
        
        for &update_ts in bucketed_update_timestamps {
            let mut rows_needed = window_size;
            
            for (&bucket_ts, bucket) in self.buckets.range(..=update_ts).rev() {
                if seen.insert(bucket_ts) {
                    result.push(bucket);
                }
                
                if rows_needed <= bucket.row_count {
                    break;
                }
                rows_needed -= bucket.row_count;
            }
        }
        
        result.sort_by_key(|b| b.timestamp);
        result
    }
    
    pub fn total_rows(&self) -> usize {
        self.total_rows
    }

    pub fn max_timestamp_seen(&self) -> Timestamp {
        self.max_timestamp_seen
    }

    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    pub fn bucket_timestamps(&self) -> Vec<Timestamp> {
        self.buckets.keys().copied().collect()
    }

    /// Get batch metadata by batch ID
    pub fn get_batch_metadata(&self, batch_id: &BatchId) -> Option<&BatchMetadata> {
        let bucket_ts = batch_id.time_bucket() as Timestamp;
        self.buckets.get(&bucket_ts)
            .and_then(|bucket| bucket.batches.iter().find(|m| m.batch_id == *batch_id))
    }

    pub fn prune(&mut self, cutoff_timestamp: Timestamp) -> Vec<BatchId> {
        let mut pruned_ids = Vec::new();
        let mut empty_buckets = Vec::new();

        for (&bucket_ts, bucket) in self.buckets.iter_mut() {
            bucket.batches.retain(|b| {
                if b.max_timestamp < cutoff_timestamp {
                    pruned_ids.push(b.batch_id);
                    self.total_rows -= b.row_count;
                    bucket.row_count -= b.row_count;
                    false
                } else {
                    true
                }
            });
            if bucket.batches.is_empty() {
                empty_buckets.push(bucket_ts);
            }
        }

        for bucket_ts in empty_buckets {
            self.buckets.remove(&bucket_ts);
        }

        pruned_ids
    }

    /// Slide window for retractable aggregations
    pub fn slide_window(
        &self,
        window_frame: &WindowFrame,
        previous_end_timestamp: Option<Timestamp>,
    ) -> Option<SlideInfo> {
        if self.buckets.is_empty() {
            return None;
        }

        let prev_end_ts = previous_end_timestamp.unwrap_or(i64::MIN);

        match window_frame.units {
            WindowFrameUnits::Range => {
                let window_length_ms = get_window_length_ms(window_frame);
                Some(self.slide_range_window(window_length_ms, prev_end_ts))
            }
            WindowFrameUnits::Rows => {
                let window_size = get_window_size_rows(window_frame);
                Some(self.slide_rows_window(window_size, prev_end_ts))
            }
            WindowFrameUnits::Groups => {
                panic!("Groups window frame not supported");
            }
        }
    }

    fn slide_range_window(&self, window_length_ms: i64, prev_end_ts: Timestamp) -> SlideInfo {
        let new_end_ts = self.max_timestamp_seen;
        
        // No new data if prev_end already at max
        if new_end_ts <= prev_end_ts {
            return SlideInfo {
                update_buckets: vec![],
                retract_buckets: vec![],
                new_end_timestamp: prev_end_ts,
                row_distance: 0,
            };
        }
        
        // Update buckets: buckets in range (prev_end_ts, new_end_ts]
        let update_buckets: Vec<&Bucket> = self.buckets
            .range((prev_end_ts + 1)..=new_end_ts)
            .map(|(_, bucket)| bucket)
            .collect();
        
        if update_buckets.is_empty() {
            return SlideInfo {
                update_buckets: vec![],
                retract_buckets: vec![],
                new_end_timestamp: prev_end_ts,
                row_distance: 0,
            };
        }

        let prev_start_ts = prev_end_ts.saturating_sub(window_length_ms);
        let new_start_ts = new_end_ts.saturating_sub(window_length_ms);

        // Retract buckets: buckets that might contain data in [prev_start, new_start)
        let retract_buckets = if new_start_ts > prev_start_ts && prev_end_ts > i64::MIN {
            self.buckets
                .range(prev_start_ts..new_start_ts)
                .map(|(_, bucket)| bucket)
                .collect()
        } else {
            vec![]
        };

        SlideInfo {
            update_buckets,
            retract_buckets,
            new_end_timestamp: new_end_ts,
            row_distance: 0, // Not used for RANGE windows
        }
    }

    fn slide_rows_window(&self, window_size: usize, prev_end_ts: Timestamp) -> SlideInfo {
        let new_end_ts = self.max_timestamp_seen;
        
        // No new data if prev_end already at max
        if new_end_ts <= prev_end_ts {
            return SlideInfo {
                update_buckets: vec![],
                retract_buckets: vec![],
                new_end_timestamp: prev_end_ts,
                row_distance: 0,
            };
        }
        
        // Update buckets: buckets in range (prev_end_ts, new_end_ts]
        let update_buckets: Vec<&Bucket> = self.buckets
            .range((prev_end_ts + 1)..=new_end_ts)
            .map(|(_, bucket)| bucket)
            .collect();
        
        if update_buckets.is_empty() {
            return SlideInfo {
                update_buckets: vec![],
                retract_buckets: vec![],
                new_end_timestamp: prev_end_ts,
                row_distance: 0,
            };
        }

        // To get retract buckets for ROWS:
        // - First update bucket is new_start_bucket, last update bucket is new_end_bucket
        // - Find old_start_bucket and old_end_bucket by going back window_size rows
        // - Return buckets in range [old_start_bucket, old_end_bucket]
        // - Set row_distance = rows between retract's last bucket and update's first bucket
        
        let (retract_buckets, row_distance) = if prev_end_ts > i64::MIN && self.total_rows > window_size {
            let new_start_bucket_ts = update_buckets.first().unwrap().timestamp;
            
            // Go back window_size rows from new_start_bucket to find old window range
            let mut rows_counted = 0;
            let mut old_end_bucket_ts: Option<Timestamp> = None;
            let mut old_start_bucket_ts: Option<Timestamp> = None;
            
            for (&bucket_ts, bucket) in self.buckets.range(..new_start_bucket_ts).rev() {
                if old_end_bucket_ts.is_none() {
                    old_end_bucket_ts = Some(bucket_ts);
                }
                old_start_bucket_ts = Some(bucket_ts);
                rows_counted += bucket.row_count;
                
                if rows_counted >= window_size {
                    break;
                }
            }
            
            match (old_start_bucket_ts, old_end_bucket_ts) {
                (Some(old_start), Some(old_end)) => {
                    // Get buckets in range [old_start_bucket, old_end_bucket]
                    let retract_buckets: Vec<&Bucket> = self.buckets
                        .range(old_start..=old_end)
                        .map(|(_, bucket)| bucket)
                        .collect();
                    
                    // row_distance = rows between retract's last bucket and update's first bucket
                    // This is the sum of rows in buckets between old_end_bucket and new_start_bucket (exclusive)
                    let row_distance: usize = self.buckets
                        .range((old_end + 1)..new_start_bucket_ts)
                        .map(|(_, bucket)| bucket.row_count)
                        .sum();
                    
                    (retract_buckets, row_distance)
                }
                _ => (vec![], 0)
            }
        } else {
            (vec![], 0)
        };

        SlideInfo {
            update_buckets,
            retract_buckets,
            new_end_timestamp: new_end_ts,
            row_distance,
        }
    }
}

pub fn get_window_length_ms(window_frame: &WindowFrame) -> i64 {
    use datafusion::logical_expr::WindowFrameBound;
    
    match &window_frame.start_bound {
        WindowFrameBound::Preceding(value) => {
            match value {
                datafusion::scalar::ScalarValue::IntervalMonthDayNano(Some(v)) => {
                    (v.nanoseconds / 1_000_000) + (v.days as i64 * 24 * 60 * 60 * 1000)
                }
                datafusion::scalar::ScalarValue::UInt64(Some(v)) => *v as i64,
                datafusion::scalar::ScalarValue::Int64(Some(v)) => *v,
                _ => panic!("Unsupported window frame bound type: {:?}", value),
            }
        }
        _ => panic!("Unsupported window frame start bound: {:?}", window_frame.start_bound),
    }
}

pub fn get_window_size_rows(window_frame: &WindowFrame) -> usize {
    use datafusion::logical_expr::WindowFrameBound;
    
    match &window_frame.start_bound {
        WindowFrameBound::Preceding(value) => {
            match value {
                datafusion::scalar::ScalarValue::UInt64(Some(v)) => (*v as usize) + 1,
                datafusion::scalar::ScalarValue::Int64(Some(v)) => (*v as usize) + 1,
                _ => panic!("Unsupported ROWS window frame bound type: {:?}", value),
            }
        }
        _ => panic!("Unsupported window frame start bound: {:?}", window_frame.start_bound),
    }
}

// TODO test row_distance
#[cfg(test)]
mod tests {
    use super::*;

    fn make_batch_id(time_bucket: u64, uid: u64) -> BatchId {
        BatchId::new(123, time_bucket, uid)
    }

    #[test]
    fn test_insert_and_query() {
        let mut index = BatchIndex::new();
        
        index.insert_batch(make_batch_id(1000, 1), 1000, 1500, 100);
        index.insert_batch(make_batch_id(2000, 2), 2000, 2500, 50);
        index.insert_batch(make_batch_id(3000, 3), 3000, 3500, 75);
        
        assert_eq!(index.total_rows(), 225);
        assert_eq!(index.max_timestamp_seen(), 3500);
        
        let buckets = index.query_buckets_in_range(1500, 2500);
        assert_eq!(buckets.len(), 1); // Only bucket 2000
        
        let buckets = index.query_buckets_in_range(1000, 3000);
        assert_eq!(buckets.len(), 3);
    }

    #[test]
    fn test_insert_same_bucket() {
        let mut index = BatchIndex::new();
        
        index.insert_batch(make_batch_id(1000, 1), 1000, 1500, 100);
        index.insert_batch(make_batch_id(1000, 2), 1100, 1600, 50);
        
        assert_eq!(index.buckets.len(), 1);
        let bucket = index.buckets.get(&1000).unwrap();
        assert_eq!(bucket.row_count, 150);
        assert_eq!(bucket.batches.len(), 2);
    }

    fn bucket_timestamps(buckets: &[&Bucket]) -> Vec<Timestamp> {
        buckets.iter().map(|b| b.timestamp).collect()
    }

    #[test]
    fn test_slide_range_window() {
        let mut index = BatchIndex::new();
        let window_length = 1000;
        
        // Step 1: Initial slide on empty index
        let result = index.slide_range_window(window_length, i64::MIN);
        assert_eq!(bucket_timestamps(&result.update_buckets), Vec::<Timestamp>::new());
        assert_eq!(bucket_timestamps(&result.retract_buckets), Vec::<Timestamp>::new());
        let mut prev_end = result.new_end_timestamp;
        
        // Step 2: Add data, slide
        index.insert_batch(make_batch_id(1000, 1), 1000, 1500, 100);
        index.insert_batch(make_batch_id(2000, 2), 2000, 2500, 50);
        
        let result = index.slide_range_window(window_length, prev_end);
        assert_eq!(bucket_timestamps(&result.update_buckets), vec![1000, 2000]);
        assert_eq!(bucket_timestamps(&result.retract_buckets), Vec::<Timestamp>::new());
        assert_eq!(result.new_end_timestamp, 2500);
        prev_end = result.new_end_timestamp;
        
        // Step 3: Add more data (including out-of-order), slide
        // prev_end = 2500, window_length = 1000
        // prev_start = 1500, new data max_ts = 4500 → new_start = 3500
        index.insert_batch(make_batch_id(4000, 3), 4000, 4500, 75);
        index.insert_batch(make_batch_id(1500, 4), 1500, 1800, 30); // Out-of-order
        index.insert_batch(make_batch_id(3000, 5), 3000, 3500, 60);
        
        let result = index.slide_range_window(window_length, prev_end);
        // Updates: buckets in (2500, 4500] → 3000, 4000
        assert_eq!(bucket_timestamps(&result.update_buckets), vec![3000, 4000]);
        // Retracts: buckets in [prev_start=1500, new_start=3500) → 1500, 2000, 3000
        assert_eq!(bucket_timestamps(&result.retract_buckets), vec![1500, 2000, 3000]);
        assert_eq!(result.new_end_timestamp, 4500);
    }

    #[test]
    fn test_slide_rows_window() {
        let mut index = BatchIndex::new();
        let window_size = 150;
        
        // Step 1: Initial slide on empty index
        let result = index.slide_rows_window(window_size, i64::MIN);
        assert_eq!(bucket_timestamps(&result.update_buckets), Vec::<Timestamp>::new());
        assert_eq!(bucket_timestamps(&result.retract_buckets), Vec::<Timestamp>::new());
        let mut prev_end = result.new_end_timestamp;
        
        // Step 2: Add data, slide (200 rows total, window=150, no retracts yet)
        index.insert_batch(make_batch_id(1000, 1), 1000, 1500, 100);
        index.insert_batch(make_batch_id(2000, 2), 2000, 2500, 100);
        
        let result = index.slide_rows_window(window_size, prev_end);
        assert_eq!(bucket_timestamps(&result.update_buckets), vec![1000, 2000]);
        assert_eq!(bucket_timestamps(&result.retract_buckets), Vec::<Timestamp>::new());
        assert_eq!(result.new_end_timestamp, 2500);
        prev_end = result.new_end_timestamp;
        
        // Step 3: Add more data (including out-of-order)
        // Total: 100+100+100+50+100 = 450 rows, window=150
        index.insert_batch(make_batch_id(4000, 3), 4000, 4500, 100);
        index.insert_batch(make_batch_id(1500, 4), 1500, 1800, 50);  // Out-of-order
        index.insert_batch(make_batch_id(3000, 5), 3000, 3500, 100);
        
        let result = index.slide_rows_window(window_size, prev_end);
        // Updates: buckets in (2500, 4500] → 3000, 4000
        assert_eq!(bucket_timestamps(&result.update_buckets), vec![3000, 4000]);
        // Retracts: go back window_size from first update bucket
        // First update bucket = 3000, go back 150 rows → hits bucket 2000 (100 rows) then 1500 (50 rows)
        assert_eq!(bucket_timestamps(&result.retract_buckets), vec![1500, 2000]);
        assert_eq!(result.new_end_timestamp, 4500);
    }

    #[test]
    fn test_get_relevant_buckets_for_range_windows() {
        let mut index = BatchIndex::new();
        
        index.insert_batch(make_batch_id(1000, 1), 1000, 1500, 100);
        index.insert_batch(make_batch_id(2000, 2), 2000, 2500, 50);
        index.insert_batch(make_batch_id(3000, 3), 3000, 3500, 75);
        index.insert_batch(make_batch_id(5000, 4), 5000, 5500, 60);
        
        // Empty updates
        let buckets = index.get_relevant_buckets_for_range_windows(&[], 1000);
        assert_eq!(bucket_timestamps(&buckets), Vec::<Timestamp>::new());
        
        // Single update at 3000, window=1000 → range [2000, 3000]
        let buckets = index.get_relevant_buckets_for_range_windows(&[3000], 1000);
        assert_eq!(bucket_timestamps(&buckets), vec![2000, 3000]);
        
        // Multiple overlapping updates at 2500 and 3500, window=1000
        // Windows: [1500, 2500] and [2500, 3500] → merged [1500, 3500]
        let buckets = index.get_relevant_buckets_for_range_windows(&[2500, 3500], 1000);
        assert_eq!(bucket_timestamps(&buckets), vec![2000, 3000]);
        
        // Non-overlapping updates at 2000 and 5000, window=500
        // Windows: [1500, 2000] and [4500, 5000] → distinct ranges
        let buckets = index.get_relevant_buckets_for_range_windows(&[2000, 5000], 500);
        assert_eq!(bucket_timestamps(&buckets), vec![2000, 5000]);
    }

    #[test]
    fn test_get_relevant_buckets_for_rows_windows() {
        let mut index = BatchIndex::new();
        
        index.insert_batch(make_batch_id(1000, 1), 1000, 1500, 100);
        index.insert_batch(make_batch_id(2000, 2), 2000, 2500, 100);
        index.insert_batch(make_batch_id(3000, 3), 3000, 3500, 100);
        index.insert_batch(make_batch_id(4000, 4), 4000, 4500, 100);
        
        // Empty updates
        let buckets = index.get_relevant_buckets_for_rows_windows(&[], 100);
        assert_eq!(bucket_timestamps(&buckets), Vec::<Timestamp>::new());
        
        // Update at 4000, window=50 → only need bucket 4000 (100 rows > 50)
        let buckets = index.get_relevant_buckets_for_rows_windows(&[4000], 50);
        assert_eq!(bucket_timestamps(&buckets), vec![4000]);
        
        // Update at 4000, window=150 → need buckets 4000 (100) + 3000 (100) 
        let buckets = index.get_relevant_buckets_for_rows_windows(&[4000], 150);
        assert_eq!(bucket_timestamps(&buckets), vec![3000, 4000]);
        
        // Update at 4000, window=350 → need all 4 buckets
        let buckets = index.get_relevant_buckets_for_rows_windows(&[4000], 350);
        assert_eq!(bucket_timestamps(&buckets), vec![1000, 2000, 3000, 4000]);
        
        // Window larger than all data
        let buckets = index.get_relevant_buckets_for_rows_windows(&[4000], 1000);
        assert_eq!(bucket_timestamps(&buckets), vec![1000, 2000, 3000, 4000]);
    }

    #[test]
    fn test_prune() {
        let mut index = BatchIndex::new();
        
        index.insert_batch(make_batch_id(1000, 1), 1000, 1500, 100);
        index.insert_batch(make_batch_id(2000, 2), 2000, 2500, 50);
        index.insert_batch(make_batch_id(3000, 3), 3000, 3500, 75);
        
        let pruned = index.prune(2000);
        
        assert_eq!(pruned.len(), 1);
        assert_eq!(index.total_rows(), 125);
        assert_eq!(index.buckets.len(), 2);
    }
}
