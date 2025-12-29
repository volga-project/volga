use crate::storage::batch_store::Timestamp;

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub struct RowPtr {
    pub bucket_ts: Timestamp,
    pub row: usize,
}

impl RowPtr {
    pub fn new(bucket_ts: Timestamp, row: usize) -> Self {
        Self { bucket_ts, row }
    }
}

