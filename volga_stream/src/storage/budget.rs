use anyhow::bail;
use serde::{Deserialize, Serialize};

use crate::storage::memory_pool::TenantBudget;

/// Per-tenant memory budgets used by `WorkerMemoryPool`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StorageMemoryBudgetConfig {
    pub total_limit_bytes: usize,
    pub write_buffer: TenantBudget,
    pub base_runs: TenantBudget,
    pub state_cache: TenantBudget,
}

/// Concurrency knobs for IO and per-key processing.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StorageConcurrencyConfig {
    pub max_inflight_keys: usize,
    pub load_io_parallelism: usize,
}

/// Policy knobs for pressure relief / eviction.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StoragePressureConfig {
    pub hot_low_watermark_per_mille: u32,
}

/// Unified storage backend configuration: memory budgets, concurrency knobs, and pressure policy.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct StorageBackendConfig {
    pub memory: StorageMemoryBudgetConfig,
    pub concurrency: StorageConcurrencyConfig,
    pub pressure: StoragePressureConfig,
    pub backlog_limit_bytes: usize,
}

impl StorageBackendConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.memory.total_limit_bytes == 0 {
            bail!("memory.total_limit_bytes must be > 0");
        }
        if self.pressure.hot_low_watermark_per_mille > 1000 {
            bail!("pressure.hot_low_watermark_per_mille must be <= 1000");
        }
        for (label, budget) in [
            ("write_buffer", self.memory.write_buffer),
            ("base_runs", self.memory.base_runs),
            ("state_cache", self.memory.state_cache),
        ] {
            if budget.soft_max_bytes == 0 {
                bail!("memory.{label}.soft_max_bytes must be > 0");
            }
            if budget.reserve_bytes > budget.soft_max_bytes {
                bail!("memory.{label}.reserve_bytes must be <= soft_max_bytes");
            }
        }
        if self.concurrency.max_inflight_keys == 0 {
            bail!("concurrency.max_inflight_keys must be > 0");
        }
        if self.concurrency.load_io_parallelism == 0 {
            bail!("concurrency.load_io_parallelism must be > 0");
        }
        if self.backlog_limit_bytes == 0 {
            bail!("backlog_limit_bytes must be > 0");
        }
        Ok(())
    }
}

impl Default for StorageBackendConfig {
    fn default() -> Self {
        Self {
            memory: StorageMemoryBudgetConfig {
                // Conservative defaults; operators can override via their config.
                total_limit_bytes: 896 * 1024 * 1024,
                write_buffer: TenantBudget {
                    reserve_bytes: 64 * 1024 * 1024,
                    soft_max_bytes: 256 * 1024 * 1024,
                },
                base_runs: TenantBudget {
                    reserve_bytes: 64 * 1024 * 1024,
                    soft_max_bytes: 256 * 1024 * 1024,
                },
                state_cache: TenantBudget {
                    reserve_bytes: 16 * 1024 * 1024,
                    soft_max_bytes: 64 * 1024 * 1024,
                },
            },
            concurrency: StorageConcurrencyConfig {
                max_inflight_keys: 1,
                load_io_parallelism: 16,
            },
            pressure: StoragePressureConfig {
                hot_low_watermark_per_mille: 700,
            },
            backlog_limit_bytes: 896 * 1024 * 1024,
        }
    }
}




