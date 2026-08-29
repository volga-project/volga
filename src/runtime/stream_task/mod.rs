mod actor;
mod checkpoint;
mod metrics;
mod output;
mod preprocess;
mod processor;
mod run;
mod source;
mod task;
mod watermark;

#[cfg(test)]
mod actor_test;
#[cfg(test)]
mod watermark_test;

pub use actor::{StreamTaskActor, StreamTaskMessage};
pub use task::{StreamTask, MESSAGE_TRACE_ENABLED};

pub(crate) use checkpoint::CheckpointAligner;
pub(crate) use preprocess::create_preprocessed_input_stream;
pub(crate) use watermark::WatermarkAssignerState;
