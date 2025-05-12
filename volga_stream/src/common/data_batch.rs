use arrow::record_batch::RecordBatch;
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct BaseDataBatch {
    pub upstream_vertex_id: Option<String>,
    pub record_batch: RecordBatch,
}

impl BaseDataBatch {
    pub fn new(upstream_vertex_id: Option<String>, record_batch: RecordBatch) -> Self {
        Self {
            upstream_vertex_id,
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

#[derive(Clone, Debug)]
pub enum DataBatch {
    Batch(BaseDataBatch),
    KeyedBatch(KeyedDataBatch),
}

impl DataBatch {
    pub fn new(upstream_vertex_id: Option<String>, record_batch: RecordBatch) -> Self {
        DataBatch::Batch(BaseDataBatch::new(upstream_vertex_id, record_batch))
    }

    pub fn new_keyed(upstream_vertex_id: Option<String>, record_batch: RecordBatch, key: String) -> Self {
        DataBatch::KeyedBatch(KeyedDataBatch::new(
            BaseDataBatch::new(upstream_vertex_id, record_batch),
            key,
        ))
    }

    pub fn upstream_vertex_id(&self) -> Option<&str> {
        match self {
            DataBatch::Batch(batch) => batch.upstream_vertex_id.as_deref(),
            DataBatch::KeyedBatch(batch) => batch.base.upstream_vertex_id.as_deref(),
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