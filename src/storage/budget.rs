use anyhow::bail;
use serde::{Deserialize, Serialize};

/// Worker-level hard limits for storage-related memory/IO.
///
/// This is intentionally backend-agnostic: it constrains in-memory caching and concurrency,
/// while backend-specific configs (e.g. SlateDB pools) will be validated against these budgets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageBudgetConfig {
    /// Hard cap for `InMemBatchCache`.
    pub in_mem_limit_bytes: usize,
    /// Target low watermark (per-mille, e.g. 700 = 70%) to stop eviction/dumping once below.
    pub in_mem_low_watermark_per_mille: u32,

    /// Hard cap for "work memory" used during reads/hydration.
    pub work_limit_bytes: usize,

    /// Max number of concurrently processed keys (limits work/hydration memory fanout).
    pub max_inflight_keys: usize,
    /// Max number of concurrent store loads per key/read (fanout control for singular store API).
    pub load_io_parallelism: usize,
}

impl StorageBudgetConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.in_mem_limit_bytes == 0 {
            bail!("in_mem_limit_bytes must be > 0");
        }
        if self.in_mem_low_watermark_per_mille > 1000 {
            bail!("in_mem_low_watermark_per_mille must be <= 1000");
        }
        if self.work_limit_bytes == 0 {
            bail!("work_limit_bytes must be > 0");
        }
        if self.max_inflight_keys == 0 {
            bail!("max_inflight_keys must be > 0");
        }
        if self.load_io_parallelism == 0 {
            bail!("load_io_parallelism must be > 0");
        }
        Ok(())
    }
}

impl Default for StorageBudgetConfig {
    fn default() -> Self {
        Self {
            // Conservative defaults; operators can override via their config.
            in_mem_limit_bytes: 256 * 1024 * 1024,
            in_mem_low_watermark_per_mille: 700,
            work_limit_bytes: 256 * 1024 * 1024,
            max_inflight_keys: 1,
            load_io_parallelism: 16,
        }
    }
}




