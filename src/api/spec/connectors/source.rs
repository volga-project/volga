use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{DatagenSpec, KafkaSourceSpec, ParquetSourceSpec};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SourceSpec {
    pub table_name: String,
    pub schema_json: Value,
    pub source: SourceSpecKind,
}

impl SourceSpec {
    pub fn new(table_name: impl Into<String>, source: SourceSpecKind, schema_json: Value) -> Self {
        Self {
            table_name: table_name.into(),
            schema_json,
            source,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SourceSpecKind {
    Datagen(DatagenSpec),
    Kafka(KafkaSourceSpec),
    Parquet(ParquetSourceSpec),
}

