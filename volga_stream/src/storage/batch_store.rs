use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use arrow::array::Array;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use dashmap::DashMap;

use std::hash::{Hash, Hasher};

use crate::{common::Key, runtime::operators::window::TimeGranularity};

pub type Timestamp = i64;
pub type RowIdx = usize; // row index within a batch

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BatchId {
    partition_key_hash: u64,
    time_bucket: Timestamp, // bucket start timestamp for this batch
    uid: u64 // small unique id for this batch
}

impl BatchId {
    pub fn new(partition_key_hash: u64, time_bucket: Timestamp, uid: u64) -> Self {
        Self { partition_key_hash, time_bucket, uid }
    }

    pub fn partition_key_hash(&self) -> u64 {
        self.partition_key_hash
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
    
    pub fn time_bucket(&self) -> Timestamp {
        self.time_bucket
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchStoreCheckpoint {
    pub bucket_granularity: TimeGranularity,
    pub max_batch_size: usize,
    pub batches: Vec<(BatchId, Vec<u8>)>, // RecordBatch serialized as Arrow IPC bytes
}

#[derive(Debug, Clone)]
pub struct BatchStoreStats {
    pub total_batches: usize,
    pub total_rows: usize,
    pub memory_usage_bytes: usize,
}

pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait BatchStore: Send + Sync + std::fmt::Debug {
    fn bucket_granularity(&self) -> TimeGranularity;
    fn max_batch_size(&self) -> usize;

    fn partition_records(
        &self,
        batch: &RecordBatch,
        partition_key: &Key,
        ts_column_index: usize,
    ) -> Vec<(Timestamp, RecordBatch)>;

    fn load_batches<'a>(&'a self, batch_ids: Vec<BatchId>, partition_key: &'a Key)
        -> BoxFut<'a, HashMap<BatchId, RecordBatch>>;
    fn remove_batches<'a>(&'a self, batch_ids: &'a [BatchId], partition_key: &'a Key) -> BoxFut<'a, ()>;
    
    // TODO we should put many 
    fn put_batch_with_id<'a>(&'a self, batch_id: BatchId, batch: RecordBatch, partition_key: &'a Key)
        -> BoxFut<'a, ()>;

    fn to_checkpoint(&self) -> BatchStoreCheckpoint;
    fn apply_checkpoint(&self, cp: BatchStoreCheckpoint);
}

#[derive(Debug)]
pub struct InMemBatchStore {
    // Global lock for exclusive operations (stats, pruning, cleanup, rebalance)
    global_lock: Arc<RwLock<()>>,
    
    // Lock pool for partition-based locking - TODO do we need pooling?
    lock_pool: Vec<Arc<RwLock<()>>>,
    
    // In-memory storage
    batch_store: Arc<DashMap<BatchId, RecordBatch>>, // TODO use foyer

    // Time partitioning configuration
    bucket_granularity: TimeGranularity,
    max_batch_size: usize,
}

impl InMemBatchStore {
    
    pub fn new(lock_pool_size: usize, bucket_granularity: TimeGranularity, max_batch_size: usize) -> Self {
        let lock_pool = (0..lock_pool_size)
            .map(|_| Arc::new(RwLock::new(())))
            .collect();
        
        Self {
            global_lock: Arc::new(RwLock::new(())),
            lock_pool,
            batch_store: Arc::new(DashMap::new()),
            bucket_granularity,
            max_batch_size,
        }
    }

    pub fn bucket_granularity(&self) -> TimeGranularity {
        self.bucket_granularity
    }

    pub fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    fn get_key_lock(&self, key: &Key) -> Arc<RwLock<()>> {
        let hash = key.hash();
        let lock_index = (hash as usize) % self.lock_pool.len();
        
        self.lock_pool[lock_index].clone()
    }

    /// Partition a batch into bucket-local batches (no ids, no storage write).
    ///
    /// This is used by the hot write path so it can keep hot buckets store-agnostic.
    pub fn partition_records(
        &self,
        batch: &RecordBatch,
        partition_key: &Key,
        ts_column_index: usize,
    ) -> Vec<(Timestamp, RecordBatch)> {
        let out: Vec<(BatchId, RecordBatch)> = Self::time_partition_batch(
            batch,
            partition_key,
            ts_column_index,
            self.bucket_granularity,
            self.max_batch_size,
        );

        out.into_iter().map(|(id, b)| (id.time_bucket(), b)).collect()
    }

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

        use arrow::array::{TimestampMillisecondArray, Int64Array, Array};
        use arrow::compute::kernels::numeric::{div, mul_wrapping};
        
        let ts_column = batch.column(ts_column_index);
        
        // Cast to TimestampMillisecondArray for timestamp operations
        let ts_array = ts_column.as_any().downcast_ref::<TimestampMillisecondArray>()
            .expect("Timestamp column should be TimestampMillisecondArray");
        
        let granularity_ms = time_granularity.to_millis();
        let num_rows = batch.num_rows();
        
        // Convert timestamp array to Int64Array for arithmetic operations
        // Use Arrow's cast to preserve nulls properly
        use arrow::compute::kernels::cast::cast;
        use arrow::datatypes::DataType;
        let ts_int64_array_owned = cast(ts_array, &DataType::Int64)
            .expect("Should be able to cast TimestampMillisecondArray to Int64Array");
        let ts_int64_array = ts_int64_array_owned.as_any().downcast_ref::<Int64Array>()
            .expect("Cast result should be Int64Array");
        
        // Create a constant array filled with granularity_ms for vectorized operations
        let granularity_array = Int64Array::from_value(granularity_ms, num_rows);
        
        // Compute buckets vectorized: bucket = (timestamp / granularity_ms) * granularity_ms
        // First, divide timestamps by granularity (integer division)
        let divided = div(&ts_int64_array, &granularity_array)
            .expect("Should be able to divide timestamps by granularity");
        let divided_int64 = divided.as_any().downcast_ref::<Int64Array>()
            .expect("Division result should be Int64Array");
        
        // Multiply back by granularity to get bucket start timestamps
        let bucket_timestamps = mul_wrapping(divided_int64, &granularity_array)
            .expect("Should be able to multiply bucket quotients by granularity");
        let bucket_array = bucket_timestamps.as_any().downcast_ref::<Int64Array>()
            .expect("Bucket array should be Int64Array");
        
        // Create original index array for stable sorting (preserve order within buckets)
        let original_indices = Int64Array::from_iter((0..num_rows).map(|i| i as i64));
        
        // Sort by (bucket, ts, __seq_no, original_index) so per-bucket batches are already sorted by rowpos.
        use arrow::compute::kernels::sort::{lexsort_to_indices, SortColumn, SortOptions};
        let seq_column_index = batch
            .schema()
            .fields()
            .iter()
            .position(|f| f.name() == "__seq_no");

        let mut sort_columns = vec![
            SortColumn {
                values: Arc::new(bucket_array.clone()) as Arc<dyn Array>,
                options: Some(SortOptions {
                    nulls_first: false,
                    descending: false,
                }),
            },
            SortColumn {
                values: Arc::new(ts_int64_array_owned) as Arc<dyn Array>,
                options: Some(SortOptions {
                    nulls_first: false,
                    descending: false,
                }),
            },
        ];

        if let Some(seq_column_index) = seq_column_index {
            sort_columns.push(SortColumn {
                values: Arc::clone(batch.column(seq_column_index)) as Arc<dyn Array>,
                options: Some(SortOptions {
                    nulls_first: false,
                    descending: false,
                }),
            });
        }

        sort_columns.push(SortColumn {
            values: Arc::new(original_indices) as Arc<dyn Array>,
            options: Some(SortOptions {
                nulls_first: false,
                descending: false,
            }),
        });
        
        let sort_indices = lexsort_to_indices(&sort_columns, None)
            .expect("Should be able to sort by bucket and rowpos");
        
        // Apply sort to all columns
        use arrow::compute::take;
        let mut sorted_columns = Vec::new();
        for i in 0..batch.num_columns() {
            let sorted_array = take(batch.column(i), &sort_indices, None)
                .expect("Should be able to take sorted columns");
            sorted_columns.push(sorted_array);
        }
        
        let sorted_batch = RecordBatch::try_new(batch.schema(), sorted_columns)
            .expect("Should be able to create sorted batch");
        
        // Apply sort to bucket array to get sorted buckets
        let bucket_array_ref: Arc<dyn Array> = Arc::new(bucket_array.clone());
        let sorted_bucket_array = take(bucket_array_ref.as_ref(), &sort_indices, None)
            .expect("Should be able to take sorted bucket array");
        let sorted_bucket_array_ref: Arc<dyn Array> = Arc::new(sorted_bucket_array.clone());
        let sorted_buckets = sorted_bucket_array.as_any().downcast_ref::<Int64Array>()
            .expect("Sorted bucket array should be Int64Array");
        
        // Group consecutive rows with the same bucket using partition
        use arrow::compute::kernels::partition::partition;
        let partition_ranges = partition(&[sorted_bucket_array_ref])
            .expect("Should be able to partition by bucket");
        
        let mut result = Vec::new();
        for range in partition_ranges.ranges() {
            let time_bucket = sorted_buckets.value(range.start);
            
            // Split the range into chunks of max_batch_size
            let mut start = range.start;
            while start < range.end {
                let end = (start + max_batch_size).min(range.end);
                let sub_batch = sorted_batch.slice(start, end - start);
                
                let uid = rand::random::<u64>();
                let batch_id = BatchId::new(partition_key.hash(), time_bucket, uid);
                result.push((batch_id, sub_batch));
                
                start = end;
            }
        }
        
        result
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

    /// Store a single batch under an explicit `BatchId`.
    ///
    /// Used for read-driven compaction materialization (base batches).
    pub async fn put_batch_with_id(&self, batch_id: BatchId, batch: RecordBatch, partition_key: &Key) {
        // Acquire global read lock
        let _global_guard = self.global_lock.read().await;
        let partition_lock = self.get_key_lock(partition_key);
        let _partition_guard = partition_lock.write().await;
        self.batch_store.insert(batch_id, batch);
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

    fn record_batch_to_ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
        let mut arrow_buffer = Vec::new();
        let mut writer = arrow::ipc::writer::FileWriter::try_new(
            std::io::Cursor::new(&mut arrow_buffer),
            batch.schema().as_ref(),
        ).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
        arrow_buffer
    }

    fn record_batch_from_ipc_bytes(bytes: &[u8]) -> RecordBatch {
        let mut reader = arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
        match reader.next() {
            Some(Ok(batch)) => batch,
            Some(Err(e)) => panic!("Failed to read record batch: {}", e),
            None => panic!("No record batch in IPC bytes"),
        }
    }

    pub fn to_checkpoint(&self) -> BatchStoreCheckpoint {
        let batches = self
            .batch_store
            .iter()
            .map(|entry| (*entry.key(), Self::record_batch_to_ipc_bytes(entry.value())))
            .collect::<Vec<_>>();

        BatchStoreCheckpoint {
            bucket_granularity: self.bucket_granularity,
            max_batch_size: self.max_batch_size,
            batches,
        }
    }

    pub fn apply_checkpoint(&self, cp: BatchStoreCheckpoint) {
        // We assume time_granularity/max_batch_size match for now (same job).
        // Clear and repopulate the in-memory store.
        self.batch_store.clear();
        for (batch_id, bytes) in cp.batches {
            let batch = Self::record_batch_from_ipc_bytes(&bytes);
            self.batch_store.insert(batch_id, batch);
        }
    }
}

impl BatchStore for InMemBatchStore {
    fn bucket_granularity(&self) -> TimeGranularity {
        InMemBatchStore::bucket_granularity(self)
    }

    fn max_batch_size(&self) -> usize {
        InMemBatchStore::max_batch_size(self)
    }

    fn partition_records(
        &self,
        batch: &RecordBatch,
        partition_key: &Key,
        ts_column_index: usize,
    ) -> Vec<(Timestamp, RecordBatch)> {
        InMemBatchStore::partition_records(self, batch, partition_key, ts_column_index)
    }

    fn load_batches<'a>(
        &'a self,
        batch_ids: Vec<BatchId>,
        partition_key: &'a Key,
    ) -> BoxFut<'a, HashMap<BatchId, RecordBatch>> {
        Box::pin(async move { InMemBatchStore::load_batches(self, batch_ids, partition_key).await })
    }

    fn remove_batches<'a>(&'a self, batch_ids: &'a [BatchId], partition_key: &'a Key) -> BoxFut<'a, ()> {
        Box::pin(async move { InMemBatchStore::remove_batches(self, batch_ids, partition_key).await })
    }

    fn put_batch_with_id<'a>(
        &'a self,
        batch_id: BatchId,
        batch: RecordBatch,
        partition_key: &'a Key,
    ) -> BoxFut<'a, ()> {
        Box::pin(async move { InMemBatchStore::put_batch_with_id(self, batch_id, batch, partition_key).await })
    }

    fn to_checkpoint(&self) -> BatchStoreCheckpoint {
        InMemBatchStore::to_checkpoint(self)
    }

    fn apply_checkpoint(&self, cp: BatchStoreCheckpoint) {
        InMemBatchStore::apply_checkpoint(self, cp)
    }
}
