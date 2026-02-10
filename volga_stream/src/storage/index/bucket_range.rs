use crate::storage::batch::Timestamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BucketRange {
    pub start: Timestamp,
    pub end: Timestamp,
}

impl BucketRange {
    pub fn new(start: Timestamp, end: Timestamp) -> Self {
        Self { start, end }
    }
}
