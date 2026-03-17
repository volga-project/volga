use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::common::Key;
use crate::storage::index::TimeGranularity;

pub type Timestamp = i64;

pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BatchId {
    partition_key_hash: u64,
    time_bucket: Timestamp,
    uid: u64,
}

impl BatchId {
    pub fn new(partition_key_hash: u64, time_bucket: Timestamp, uid: u64) -> Self {
        Self {
            partition_key_hash,
            time_bucket,
            uid,
        }
    }

    pub fn partition_key_hash(&self) -> u64 {
        self.partition_key_hash
    }

    pub fn time_bucket(&self) -> Timestamp {
        self.time_bucket
    }

    pub fn to_string(&self) -> String {
        format!("{}-{}-{}", self.partition_key_hash, self.time_bucket, self.uid)
    }

    pub fn random() -> Self {
        Self {
            partition_key_hash: rand::random(),
            time_bucket: rand::random(),
            uid: rand::random(),
        }
    }
}

impl Eq for BatchId {}

impl PartialEq for BatchId {
    fn eq(&self, other: &Self) -> bool {
        self.partition_key_hash == other.partition_key_hash
            && self.time_bucket == other.time_bucket
            && self.uid == other.uid
    }
}

impl std::hash::Hash for BatchId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.partition_key_hash.hash(state);
        self.time_bucket.hash(state);
        self.uid.hash(state);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InMemBatchStoreCheckpoint {
    pub bucket_granularity: TimeGranularity,
    pub max_batch_size: usize,
    pub batches: Vec<(BatchId, Vec<u8>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteCheckpointToken {
    pub parent_db_path: String,
    pub checkpoint_uuid: String,
    pub manifest_id: u64,
    pub lifetime_secs: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct BatchStoreStats {
    pub total_batches: usize,
    pub total_rows: usize,
    pub memory_usage_bytes: usize,
}

pub fn record_batch_to_ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut arrow_buffer = Vec::new();
    let mut writer = arrow::ipc::writer::FileWriter::try_new(
        std::io::Cursor::new(&mut arrow_buffer),
        batch.schema().as_ref(),
    )
    .expect("ipc writer");
    writer.write(batch).expect("ipc write");
    writer.finish().expect("ipc finish");
    arrow_buffer
}

pub fn record_batch_from_ipc_bytes(bytes: &[u8]) -> RecordBatch {
    let mut reader =
        arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None).expect("ipc reader");
    match reader.next() {
        Some(Ok(batch)) => batch,
        Some(Err(e)) => panic!("Failed to read record batch: {}", e),
        None => panic!("No record batch in IPC bytes"),
    }
}

/// Partition a batch into bucket-local `RecordBatch`es.
///
/// Returns `(bucket_ts, batch)` pairs.
pub fn partition_records(
    batch: &RecordBatch,
    partition_key: &Key,
    ts_column_index: usize,
    bucket_granularity: TimeGranularity,
    max_batch_size: usize,
) -> Vec<(Timestamp, RecordBatch)> {
    let out: Vec<(BatchId, RecordBatch)> = time_partition_batch(
        batch,
        partition_key,
        ts_column_index,
        bucket_granularity,
        max_batch_size,
    );
    out.into_iter().map(|(id, b)| (id.time_bucket(), b)).collect()
}

/// Time-partition a batch into `(BatchId, RecordBatch)` segments.
///
/// This is a direct extraction of the previous in-mem store logic.
pub fn time_partition_batch(
    batch: &RecordBatch,
    partition_key: &Key,
    ts_column_index: usize,
    time_granularity: TimeGranularity,
    max_batch_size: usize,
) -> Vec<(BatchId, RecordBatch)> {
    use arrow::datatypes::{DataType, TimeUnit};
    use arrow::array::{Int64Array, TimestampMillisecondArray};

    if batch.num_rows() == 0 {
        return Vec::new();
    }

    // Normalize timestamp column to i64 milliseconds (per-row).
    let ts_col = batch.column(ts_column_index);
    let ts_ms: Arc<Int64Array> = match ts_col.data_type() {
        DataType::Int64 => Arc::new(
            ts_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64 ts")
                .clone(),
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let a = ts_col
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("ts ms");
            Arc::new(Int64Array::from_iter_values((0..a.len()).map(|i| a.value(i))))
        }
        other => panic!("unsupported timestamp type: {:?}", other),
    };

    let step_ms = time_granularity.to_millis();
    let step_ms = step_ms.max(1);

    // Build groups by bucket value, preserving order; split into chunks by max_batch_size.
    use std::collections::BTreeMap;
    let mut groups: BTreeMap<i64, Vec<usize>> = BTreeMap::new();
    for i in 0..batch.num_rows() {
        let ts = ts_ms.value(i);
        let b = (ts / step_ms) * step_ms;
        groups.entry(b).or_default().push(i);
    }

    let mut out: Vec<(BatchId, RecordBatch)> = Vec::new();
    for (bucket_ts, idxs) in groups {
        for chunk in idxs.chunks(max_batch_size.max(1)) {
            let uid: u64 = rand::random();
            let id = BatchId::new(partition_key.hash(), bucket_ts, uid);
            let taken = arrow::compute::take_record_batch(
                batch,
                &arrow::array::UInt32Array::from(chunk.iter().map(|v| *v as u32).collect::<Vec<_>>()),
            )
            .expect("take batch");
            out.push((id, taken));
        }
    }
    out
}

