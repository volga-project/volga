// User-facing specs live next to their runtime implementations.
pub mod schema_ipc;
pub mod sink;
pub mod source;

pub use crate::runtime::functions::source::DatagenSpec;
pub use crate::runtime::functions::source::kafka::KafkaSourceSpec;
pub use crate::runtime::functions::source::RequestSourceSinkSpec;
pub use schema_ipc::{schema_from_ipc, schema_to_ipc};
pub use sink::SinkSpec;
pub use source::{SourceSpec, SourceBindingSpec};

