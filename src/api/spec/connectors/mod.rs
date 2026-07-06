// User-facing specs live next to their runtime implementations.
pub mod sink;
pub mod source;

pub use crate::runtime::functions::source::DatagenSpec;
pub use crate::runtime::functions::source::kafka::KafkaSourceSpec;
pub use crate::runtime::functions::source::parquet::ParquetSourceSpec;
pub use crate::runtime::functions::source::RequestSourceSinkSpec;
pub use crate::runtime::functions::sink::ParquetSinkSpec;
pub use sink::SinkSpec;
pub use source::{SourceSpec, SourceSpecKind};

