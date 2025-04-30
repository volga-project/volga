// use arrow::record_batch::RecordBatch;
use anyhow::Result;

type RecordBatch = Vec<String>; // TODO use arrow::record_batch::RecordBatch

#[derive(Debug, Clone)]
pub struct BaseDataBatch {
    pub stream_name: Option<String>,
    pub record_batch: RecordBatch,
}

impl BaseDataBatch {
    pub fn new(stream_name: Option<String>, record_batch: RecordBatch) -> Self {
        Self {
            stream_name,
            record_batch,
        }
    }
}

#[derive(Debug, Clone)]
pub struct KeyedDataBatch {
    pub base: BaseDataBatch,
    pub key: String,
}

impl KeyedDataBatch {
    pub fn new(base: BaseDataBatch, key: String) -> Self {
        Self {
            base,
            key,
        }
    }

    pub fn key(&self) -> String {
        self.key.clone()
    }
}

#[derive(Debug, Clone)]
pub enum DataBatch {
    Batch(BaseDataBatch),
    KeyedBatch(KeyedDataBatch),
}

impl DataBatch {
    pub fn new(stream_name: Option<String>, record_batch: RecordBatch) -> Self {
        DataBatch::Batch(BaseDataBatch::new(stream_name, record_batch))
    }

    pub fn new_keyed(stream_name: Option<String>, record_batch: RecordBatch, key_column: String) -> Self {
        DataBatch::KeyedBatch(KeyedDataBatch::new(
            BaseDataBatch::new(stream_name, record_batch),
            key_column,
        ))
    }

    pub fn stream_name(&self) -> Option<&str> {
        match self {
            DataBatch::Batch(batch) => batch.stream_name.as_deref(),
            DataBatch::KeyedBatch(batch) => batch.base.stream_name.as_deref(),
        }
    }

    pub fn record_batch(&self) -> &RecordBatch {
        match self {
            DataBatch::Batch(batch) => &batch.record_batch,
            DataBatch::KeyedBatch(batch) => &batch.base.record_batch,
        }
    }

    pub fn key(&self) -> Result<String> {
        match self {
            DataBatch::Batch(_) => Err(anyhow::anyhow!("Batch does not have a key")),
            DataBatch::KeyedBatch(batch) => Ok(batch.key.clone()),
        }
    }
} 