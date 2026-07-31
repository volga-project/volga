pub mod datagen_source;
pub mod json_utils;
pub mod kafka;
pub mod parquet;
pub mod request_source;
pub mod source_function;
pub mod vector_source;
pub mod word_count_source;

pub use datagen_source::DatagenSourceFunction;
pub use datagen_source::DatagenSpec;
pub use json_utils::{json_to_record_batch, record_batch_to_json};
pub use kafka::{KafkaOffsetSpec, KafkaSourceConfig, KafkaSourceFunction, KafkaSourceSpec};
pub use parquet::{ParquetSourceConfig, ParquetSourceFunction, ParquetSourceSpec};
pub use request_source::{HttpRequestSourceFunction, RequestSourceConfig, RequestSourceSinkSpec};
pub use source_function::{
    create_source_function, FetchResult, SourceFunction, SourceFunctionTrait,
};
pub use vector_source::VectorSourceFunction;
pub use word_count_source::WordCountSourceFunction;
