pub mod map;
pub mod join;
pub mod sink;
pub mod source;
pub mod key_by;
pub mod operator;
#[cfg(test)]
mod operator_test;
pub mod kind;
pub mod aggregate;
pub mod window;

pub use kind::OperatorKind;