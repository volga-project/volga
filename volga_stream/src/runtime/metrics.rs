// Keep `crate::runtime::metrics::*` as the stable import path, but physically store the
// implementation under the observability umbrella.
#[path = "observability/metrics/mod.rs"]
mod moved_metrics;

pub use moved_metrics::*;