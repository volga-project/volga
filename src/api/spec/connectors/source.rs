use serde::{Deserialize, Serialize};

use super::{DatagenSpec, KafkaSourceSpec, ParquetSourceSpec};
use super::schema_ipc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceSpec {
    pub table_name: String,
    #[serde(with = "schema_ipc::b64_bytes")]
    pub schema_ipc: Vec<u8>,
    pub source: SourceSpecKind,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SourceSpecKind {
    Datagen(DatagenSpec),
    Kafka(KafkaSourceSpec),
    Parquet(ParquetSourceSpec),
}

