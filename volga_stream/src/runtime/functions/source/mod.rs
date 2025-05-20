pub mod source_function;
pub mod vector_source;
pub mod word_count_source;

pub use source_function::{SourceFunction, SourceFunctionTrait, create_source_function};
pub use vector_source::VectorSourceFunction;
pub use word_count_source::WordCountSourceFunction; 