use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use arrow::compute::take_record_batch;
use arrow::array::UInt64Array;
use datafusion::common::ScalarValue;
use arrow::array::Array;
use tokio::sync::RwLock;
use dashmap::DashMap;

use std::hash::{Hash, Hasher};

use crate::common::Key;

pub type Timestamp = i64;
pub type RowIdx = usize; // row index within a batch

#[derive(Debug, Clone, Copy)]
pub struct BatchId {
    partition_key_hash: u64,
    time_bucket: u64, // bucket start timestamp for this batch
    uid: u64 // small unique id for this batch
}

impl BatchId {
    pub fn new(partition_key_hash: u64, time_bucket: u64, uid: u64) -> Self {
        Self { partition_key_hash, time_bucket, uid }
    }

    pub fn to_string(&self) -> String {
        format!("{}-{}-{}", self.partition_key_hash, self.time_bucket, self.uid)
    }

    pub fn nil() -> Self {
        Self {partition_key_hash: 0, time_bucket: 0, uid: 0}
    }

    pub fn random() -> Self {
        Self {partition_key_hash: rand::random(), time_bucket: rand::random(), uid: rand::random()}
    }
}

impl Eq for BatchId {}

impl PartialEq for BatchId {
    fn eq(&self, other: &Self) -> bool {
        self.partition_key_hash == other.partition_key_hash && self.time_bucket == other.time_bucket && self.uid == other.uid
    }
}

impl Hash for BatchId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.partition_key_hash.hash(state);
        self.time_bucket.hash(state);
        self.uid.hash(state);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TimeGranularity {
    Minutes(u32),
    Hours(u32),
    Days(u32),
}

impl TimeGranularity {
    pub fn to_millis(&self) -> i64 {
        match self {
            TimeGranularity::Minutes(m) => *m as i64 * 60 * 1000,
            TimeGranularity::Hours(h) => *h as i64 * 60 * 60 * 1000,
            TimeGranularity::Days(d) => *d as i64 * 24 * 60 * 60 * 1000,
        }
    }
}


#[derive(Debug, Clone)]
pub struct BatchStoreStats {
    pub total_batches: usize,
    pub total_rows: usize,
    pub memory_usage_bytes: usize,
}

#[derive(Debug)]
pub struct BatchStore {
    // Global lock for exclusive operations (stats, pruning, cleanup, rebalance)
    global_lock: Arc<RwLock<()>>,
    
    // Lock pool for partition-based locking
    lock_pool: Vec<Arc<RwLock<()>>>,
    
    // In-memory storage
    batch_store: Arc<DashMap<BatchId, RecordBatch>>, // TODO use foyer
    
    // Time partitioning configuration
    time_granularity: TimeGranularity,
    max_batch_size: usize,
}

impl Default for BatchStore {
    fn default() -> Self {
        Self::new(4096, TimeGranularity::Minutes(5), 1024)
    }
}

impl BatchStore {
    
    pub fn new(lock_pool_size: usize, time_granularity: TimeGranularity, max_batch_size: usize) -> Self {
        let lock_pool = (0..lock_pool_size)
            .map(|_| Arc::new(RwLock::new(())))
            .collect();
        
        Self {
            global_lock: Arc::new(RwLock::new(())),
            lock_pool,
            batch_store: Arc::new(DashMap::new()),
            time_granularity,
            max_batch_size,
        }
    }

    fn get_key_lock(&self, key: &Key) -> Arc<RwLock<()>> {
        let hash = key.hash();
        let lock_index = (hash as usize) % self.lock_pool.len();
        
        self.lock_pool[lock_index].clone()
    }

    pub async fn append_records(&self, batch: RecordBatch, partition_key: &Key, ts_column_index: usize) -> Vec<(BatchId, RecordBatch)> {
        // Partition batch by time granularity and get batch ids and keys
        let time_partitioned_batches = Self::time_partition_batch(&batch, &partition_key, ts_column_index, self.time_granularity, self.max_batch_size);
        
        self.store_batches(partition_key, &time_partitioned_batches).await;
        
        time_partitioned_batches
    }

    // TODO can we use arrow::compute::* kernels here for SIMD?
    fn time_partition_batch(
        batch: &RecordBatch, 
        partition_key: &Key, 
        ts_column_index: usize,
        time_granularity: TimeGranularity,
        max_batch_size: usize,
    ) -> Vec<(BatchId, RecordBatch)> {
        if batch.num_rows() == 0 {
            panic!("Batch has no rows");
        }

        // Group row indices by time buckets, keep the order of the rows
        let mut time_buckets: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
        
        for row_idx in 0..batch.num_rows() {
            let timestamp = extract_timestamp(batch.column(ts_column_index), row_idx);
            let time_bucket = Self::get_time_bucket_start(timestamp, time_granularity);
            
            time_buckets.entry(time_bucket)
                .or_insert_with(Vec::new)
                .push(row_idx);
        }

        let mut result = Vec::new();
        for (time_bucket, row_indices) in time_buckets {
            let sub_batches = Self::split_by_batch_size(
                batch, 
                partition_key, 
                time_bucket,
                row_indices, 
                max_batch_size,
            );
            result.extend(sub_batches);
        }
        result
    }

    fn split_by_batch_size(
        batch: &RecordBatch,
        partition_key: &Key,
        time_bucket: u64,
        row_indices: Vec<usize>,
        max_batch_size: usize,
    ) -> Vec<(BatchId, RecordBatch)> {
        let mut result = Vec::new();
        
        if row_indices.len() <= max_batch_size {
            // No splitting needed
            let indices_array = UInt64Array::from(
                row_indices.into_iter().map(|idx| idx as u64).collect::<Vec<_>>()
            );
            
            let sub_batch = take_record_batch(batch, &indices_array)
                .expect("Take record batch operation should succeed");
            
            let uid = rand::random::<u64>(); // should be unique enough within same key and bucket
            let batch_id = BatchId::new(partition_key.hash(), time_bucket, uid);
            result.push((batch_id, sub_batch));
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
            
            let uid = rand::random::<u64>(); // should be unique enough within same key and bucket
            let batch_id = BatchId::new(partition_key.hash(), time_bucket, uid);

            result.push((batch_id, sub_batch));
        }

        result
    }

    /// Returns the timestamp (in milliseconds) of the start of the time bucket that the given record's timestamp falls into.
    /// `timestamp_ms` is the timestamp (in milliseconds) of the record.
    /// `time_granularity` specifies the bucket size.
    fn get_time_bucket_start(timestamp_ms: i64, time_granularity: TimeGranularity) -> u64 {
        let granularity_ms = time_granularity.to_millis();
        ((timestamp_ms / granularity_ms) * granularity_ms) as u64
    }

    // Store a batch with per-partition locking
    async fn store_batches(&self, partition_key: &Key, batches: &Vec<(BatchId, RecordBatch)>) {
        // Acquire global read lock (allows concurrent operations unless global exclusive is held)
        let _global_guard = self.global_lock.read().await;
        
        let partition_lock = self.get_key_lock(&partition_key);
        let _partition_guard = partition_lock.write().await;
        for (batch_id, batch) in batches {
            self.batch_store.insert(batch_id.clone(), batch.clone());
        }

        // TODO put to slatedb and store write handle
    }


    // Get multiple batches from storage
    // TODO should this be an iterator?
    pub async fn load_batches(&self, batch_ids: Vec<BatchId>, partition_key: &Key) -> HashMap<BatchId, RecordBatch> {
        // Acquire global read lock
        let _global_guard = self.global_lock.read().await;
        
        // Acquire per-partition read lock
        let partition_lock = self.get_key_lock(partition_key);
        let _partition_guard = partition_lock.read().await;

        let mut result = HashMap::new();
        for batch_id in batch_ids {
            if let Some(batch) = self.batch_store.get(&batch_id) {
                result.insert(batch_id, batch.clone());
            }
        }
        
        result
    }

    // Remove multiple batches from storage
    pub async fn remove_batches(&self, batch_ids: &[BatchId], partition_key: &Key) {
        // Acquire global read lock
        let _global_guard = self.global_lock.read().await;
        
        // Acquire per-partition write lock
        let partition_lock = self.get_key_lock(partition_key);
        let _partition_guard = partition_lock.write().await;

        for batch_id in batch_ids {
            self.batch_store.remove(batch_id);
        }
    }

    // Get storage statistics with global exclusive lock
    pub async fn get_stats(&self) -> BatchStoreStats {
        // Acquire global exclusive lock to get consistent snapshot
        let _global_exclusive = self.global_lock.write().await;
        
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
        
        BatchStoreStats {
            total_batches,
            total_rows,
            memory_usage_bytes,
        }
    }
}

pub fn extract_timestamp(array: &dyn Array, index: usize) -> i64 {
    let scalar_value = ScalarValue::try_from_array(array, index)
        .expect("Should be able to extract scalar timestamp value from array");
    
    i64::try_from(scalar_value.clone())
        .expect("Should be able to convert scalar timestamp value to i64")
}
