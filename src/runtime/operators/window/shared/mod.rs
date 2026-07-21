//! Shared WO/WRO construction helpers (config, schema, tiling resolution).
//! Not shared in-process state — WO and WRO each own a SortedKV client.

pub mod config;
pub mod init;
pub mod output;

pub use config::WindowConfig;
pub use init::{build_window_operator_parts, resolve_tiling_configs};
pub use output::{create_output_schema, stack_concat_results};
