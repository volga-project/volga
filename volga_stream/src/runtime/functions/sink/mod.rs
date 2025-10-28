pub mod sink_function;
pub mod in_memory_storage_sink;
pub mod request_sink;

pub use sink_function::{SinkFunction, SinkFunctionTrait};
pub use request_sink::RequestSinkFunction; 