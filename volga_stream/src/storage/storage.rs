use std::collections::HashMap;
use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use arrow::compute::take_record_batch;
use arrow::array::UInt64Array;
use uuid::Uuid;
use datafusion::common::ScalarValue;
use arrow::array::Array;
use tokio::sync::RwLock;
use dashmap::DashMap;
use chrono::{Utc, TimeZone};

use crate::common::Key;

pub type Timestamp = u64;
pub type BatchId = Uuid; // light-weight batch id used for in-memory index
pub type BatchKey = String; // batch key including extra metadata for storage, maps 1:1 to batch id
pub type RowIdx = usize; // row index within a batch

#[derive(Debug, Clone, Copy)]
pub enum TimeGranularity {
    Day,
    Hour,
    Minute,
}


#[derive(Debug, Clone)]
pub struct StorageStats {
    pub total_batches: usize,
    // pub total_partitions: usize,
    pub total_rows: usize,
    pub memory_usage_bytes: usize,
}

#[derive(Debug)]
pub struct Storage {
    // Global lock for exclusive operations (stats, pruning, cleanup, rebalance)
    global_lock: Arc<RwLock<()>>,
    
    // Lock pool for partition-based locking
    lock_pool: Vec<Arc<RwLock<()>>>,
    lock_pool_size: usize,
    
    // In-memory storage
    batch_store: Arc<DashMap<BatchKey, RecordBatch>>, // TODO use foyer
    batch_id_to_key: Arc<DashMap<BatchId, BatchKey>>,
    
    // Time partitioning configuration
    time_granularity: TimeGranularity,
    max_batch_size: usize,
}

impl Default for Storage {
    fn default() -> Self {
        Self::new(4096, TimeGranularity::Minute, 1024)
    }
}

impl Storage {
    
    pub fn new(lock_pool_size: usize, time_granularity: TimeGranularity, max_batch_size: usize) -> Self {
        let lock_pool = (0..lock_pool_size)
            .map(|_| Arc::new(RwLock::new(())))
            .collect();
        
        Self {
            global_lock: Arc::new(RwLock::new(())),
            lock_pool,
            lock_pool_size,
            batch_store: Arc::new(DashMap::new()),
            batch_id_to_key: Arc::new(DashMap::new()),
            // partitioned_batch_ids: Arc::new(DashMap::new()),
            time_granularity,
            max_batch_size,
        }
    }

    // Get per-partition lock from pool using hash
    fn get_partition_lock(&self, partition_key: &Key) -> Arc<RwLock<()>> {
        let hash = partition_key.hash();
        let lock_index = (hash as usize) % self.lock_pool_size;
        
        self.lock_pool[lock_index].clone()
    }

    // Global exclusive lock
    pub async fn acquire_global_exclusive_lock(&self) -> tokio::sync::RwLockWriteGuard<'_, ()> {
        self.global_lock.write().await
    }

    pub async fn append_records(&self, batch: RecordBatch, partition_key: &Key, ts_column_index: usize) -> Vec<BatchId> {
        // Partition batch by time granularity and get batch ids and keys
        let time_partitioned_batches = Self::time_partition_batch(&batch, &self.batch_id_to_key, &partition_key, ts_column_index, self.time_granularity, self.max_batch_size);
        
        let mut batch_ids = Vec::new();
        for (batch_id, batch_key, sub_batch) in time_partitioned_batches {
            self.store_batch(batch_key.clone(), sub_batch, partition_key.clone()).await;
            batch_ids.push(batch_id);
        }
        
        batch_ids
    }

    pub fn time_partition_batch(
        batch: &RecordBatch, 
        batch_id_to_key: &Arc<DashMap<BatchId, BatchKey>>,
        partition_key: &Key, 
        ts_column_index: usize,
        time_granularity: TimeGranularity,
        max_batch_size: usize,
    ) -> Vec<(BatchId, BatchKey, RecordBatch)> {
        if batch.num_rows() == 0 {
            panic!("Batch has no rows");
        }

        // Group row indices by time partition
        let mut time_groups: HashMap<String, Vec<usize>> = HashMap::new();
        
        for row_idx in 0..batch.num_rows() {
            let timestamp = Self::extract_timestamp(batch.column(ts_column_index), row_idx);
            let time_partition = Self::timestamp_to_time_partition(timestamp, time_granularity);
            
            time_groups.entry(time_partition)
                .or_insert_with(Vec::new)
                .push(row_idx);
        }

        let mut result = Vec::new();
        for (_, row_indices) in time_groups {
            let sub_batches = Self::split_by_batch_size(
                batch, 
                batch_id_to_key,
                partition_key, 
                row_indices, 
                ts_column_index,
                max_batch_size,
            );
            result.extend(sub_batches);
        }
        result
    }


    pub fn split_by_batch_size(
        batch: &RecordBatch,
        batch_id_to_key: &Arc<DashMap<BatchId, BatchKey>>,
        partition_key: &Key,
        row_indices: Vec<usize>,
        ts_column_index: usize,
        max_batch_size: usize,
    ) -> Vec<(BatchId, BatchKey, RecordBatch)> {
        let mut result = Vec::new();
        
        if row_indices.len() <= max_batch_size {
            // No splitting needed
            let indices_array = UInt64Array::from(
                row_indices.into_iter().map(|idx| idx as u64).collect::<Vec<_>>()
            );
            
            let sub_batch = take_record_batch(batch, &indices_array)
                .expect("Take record batch operation should succeed");
            
            let (batch_id, batch_key) = Self::generate_batch_id_and_key(&sub_batch, batch_id_to_key, partition_key, ts_column_index);
            result.push((batch_id, batch_key, sub_batch));
            return result;
        }

        // Split into chunks of max_batch_size
        let chunks: Vec<_> = row_indices.chunks(max_batch_size).collect();
        
        for (_, chunk) in chunks.iter().enumerate() {
            let indices_array = UInt64Array::from(
                chunk.iter().map(|&idx| idx as u64).collect::<Vec<_>>()
            );
            
            let sub_batch = take_record_batch(batch, &indices_array)
                .expect("Take record batch operation should succeed");
            
            let (batch_id, batch_key) = Self::generate_batch_id_and_key(&sub_batch, batch_id_to_key, partition_key, ts_column_index);
            
            result.push((batch_id, batch_key, sub_batch));
        }

        result
    }

    // TODO this should also be prefixed with operator-id
    pub fn generate_batch_id_and_key(
        batch: &RecordBatch,
        batch_id_to_key: &Arc<DashMap<BatchId, BatchKey>>,
        partition_key: &Key,
        ts_column_index: usize,
    ) -> (BatchId, BatchKey) {
        let partition_hex = hex::encode(partition_key.to_bytes());
        let (min_ts, _) = Self::get_time_range_from_batch(batch, ts_column_index);
        let mut uuid = Uuid::new_v4();

        // avoid collisions
        while batch_id_to_key.contains_key(&uuid) {
            uuid = Uuid::new_v4();
        }
        
        let batch_key = format!("{}-{}-{}", partition_hex, min_ts, uuid.to_string());
        batch_id_to_key.insert(uuid, batch_key.clone());

        (uuid, batch_key)
    }

    fn get_time_range_from_batch(batch: &RecordBatch, ts_column_index: usize) -> (u64, u64) {
        if batch.num_rows() == 0 {
            panic!("Batch has no rows");
        }

        let timestamp_column = batch.column(ts_column_index);
        let mut min_ts = u64::MAX;
        let mut max_ts = 0;
        
        for row_idx in 0..batch.num_rows() {
            let ts = Self::extract_timestamp(timestamp_column, row_idx);
            min_ts = min_ts.min(ts);
            max_ts = max_ts.max(ts);
        }
        
        (min_ts, max_ts)
    }

    pub fn timestamp_to_time_partition(timestamp_ms: u64, time_granularity: TimeGranularity) -> String {
        // Convert milliseconds to UTC DateTime
        let datetime = Utc.timestamp_millis_opt(timestamp_ms as i64)
            .single()
            .expect("Timestamp should be valid");
        
        match time_granularity {
            TimeGranularity::Day => {
                // Format: YYYY-MM-DD
                datetime.format("%Y-%m-%d").to_string()
            },
            TimeGranularity::Hour => {
                // Format: YYYY-MM-DD-HH
                datetime.format("%Y-%m-%d-%H").to_string()
            },
            TimeGranularity::Minute => {
                // Format: YYYY-MM-DD-HH-MM
                datetime.format("%Y-%m-%d-%H-%M").to_string()
            },
        }
    }

    pub fn extract_timestamp(array: &dyn Array, index: usize) -> u64 {
        let scalar_value = ScalarValue::try_from_array(array, index)
            .expect("Should be able to extract scalar timestamp value from array");
        
        u64::try_from(scalar_value)
            .expect("Should be able to convert scalar timestamp value to u64")
    }

    // Store a batch with per-partition locking
    async fn store_batch(&self, batch_key: BatchKey, batch: RecordBatch, partition_key: Key) {
        // Acquire global read lock (allows concurrent operations unless global exclusive is held)
        let _global_guard = self.global_lock.read().await;
        
        // Acquire per-partition write lock for atomic operations
        let partition_lock = self.get_partition_lock(&partition_key);
        let _partition_guard = partition_lock.write().await;
        
        // Atomic operations under partition lock:
        // 1. Store batch in memory
        self.batch_store.insert(batch_key.clone(), batch);

        // TODO put to slatedb and store write handle
    }


    // Get multiple batches from in-memory storage
    // TODO should this be an iterator?
    async fn get_batches(&self, batch_ids: Vec<BatchId>, partition_key: &Key) -> HashMap<BatchId, RecordBatch> {
        // Acquire global read lock
        let _global_guard = self.global_lock.read().await;
        
        // Acquire per-partition read lock
        let partition_lock = self.get_partition_lock(partition_key);
        let _partition_guard = partition_lock.read().await;

        let mut result = HashMap::new();
        for batch_id in batch_ids {
            let batch_key = self.batch_id_to_key.get(&batch_id)
                .expect("Batch ID should have a corresponding batch key");
            if let Some(batch) = self.batch_store.get(batch_key.value()) {
                result.insert(batch_id, batch.clone());
            }
        }
        
        result
    }

    // TODO should this be an iterator?
    pub async fn load_events(&self, indices_list: Vec<Vec<(BatchId, RowIdx)>>, partition_key: &Key) -> Vec<RecordBatch> {
        // Pre-load all required batches from all indices lists
        let mut all_batch_ids = std::collections::HashSet::new();
        for indices in &indices_list {
            for (batch_id, _) in indices {
                all_batch_ids.insert(batch_id.clone());
            }
        }
        
        let batch_ids: Vec<BatchId> = all_batch_ids.into_iter().collect();
        let batches = self.get_batches(batch_ids, partition_key).await;
        
        let mut result_batches = Vec::new();
        
        for indices in indices_list {
            
            // Group indices by batch_id to minimize batch lookups
            let mut batch_indices: HashMap<BatchId, Vec<RowIdx>> = HashMap::new();
            for (batch_id, row_idx) in indices {
                batch_indices.entry(batch_id.clone())
                    .or_insert_with(Vec::new)
                    .push(row_idx);
            }
            
            // Collect all rows from different batches using take_record_batch
            let mut taken_batches = Vec::new();
            
            for (batch_id, row_indices) in batch_indices {
                let batch = batches.get(&batch_id)
                    .expect("Batch should exist in pre-loaded batches");
                
                // Convert row indices to Arrow UInt64Array for take operation
                let indices_array = UInt64Array::from(
                    row_indices.into_iter().map(|idx| idx as u64).collect::<Vec<_>>()
                );
                
                // Use Arrow's take_record_batch to extract rows
                let taken_batch = take_record_batch(batch, &indices_array)
                    .expect("Take record batch operation should succeed");
                
                taken_batches.push(taken_batch);
            }
            
            // Concatenate all taken batches if there are multiple
            let result_batch = if taken_batches.len() == 1 {
                taken_batches.into_iter().next().unwrap()
            } else if taken_batches.len() > 1 {
                let schema = taken_batches[0].schema();
                arrow::compute::concat_batches(&schema, &taken_batches)
                    .expect("Concatenation should succeed")
            } else {
                RecordBatch::new_empty(std::sync::Arc::new(arrow::datatypes::Schema::empty()))
            };
            
            result_batches.push(result_batch);
        }
        
        result_batches
    }

    // Get storage statistics with global exclusive lock
    pub async fn get_stats(&self) -> StorageStats {
        // Acquire global exclusive lock to get consistent snapshot
        let _global_exclusive = self.global_lock.write().await;
        
        // let total_partitions = self.partitioned_batch_ids.len();
        let total_batches = self.batch_store.len();
        
        // Calculate total rows and approximate memory usage
        let mut total_rows = 0;
        let mut memory_usage_bytes = 0;
        
        for batch_entry in self.batch_store.iter() {
            let batch = batch_entry.value();
            total_rows += batch.num_rows();
            
            // Approximate memory usage calculation
            for column in batch.columns() {
                memory_usage_bytes += column.get_array_memory_size();
            }
        }
        
        // Add overhead for metadata structures
        memory_usage_bytes += total_batches * 64; // BatchId keys
        // memory_usage_bytes += total_partitions * 128; // PartitionKey overhead
        
        StorageStats {
            total_batches,
            // total_partitions,
            total_rows,
            memory_usage_bytes,
        }
    }
}
