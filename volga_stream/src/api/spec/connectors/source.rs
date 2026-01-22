use serde::{Deserialize, Serialize};

use super::{DatagenSpec, KafkaSourceSpec, ParquetSourceSpec};
use super::schema_ipc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceBindingSpec {
    pub table_name: String,
    #[serde(with = "schema_ipc::b64_bytes")]
    pub schema_ipc: Vec<u8>,
    pub source: SourceSpec,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SourceSpec {
    Datagen(DatagenSpec),
    Kafka(KafkaSourceSpec),
    Parquet(ParquetSourceSpec),
}

