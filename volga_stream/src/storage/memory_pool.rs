use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::runtime::TaskId;
use crate::storage::budget::StorageMemoryBudgetConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TenantType {
    WriteBuffer,
    BaseRuns,
    StateCache,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TenantBudget {
    pub reserve_bytes: usize,
    pub soft_max_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
struct TenantUsage {
    used_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct WorkerMemoryPool {
    total_limit_bytes: usize,
    budgets: StorageMemoryBudgetConfig,
    used_total: Arc<AtomicUsize>,
    used_by_key: Arc<Mutex<HashMap<MemoryKey, TenantUsage>>>,
}

#[derive(Debug, Clone)]
struct MemoryKey {
    task_id: TaskId,
    tenant: TenantType,
}

impl PartialEq for MemoryKey {
    fn eq(&self, other: &Self) -> bool {
        self.tenant == other.tenant && self.task_id.as_ref() == other.task_id.as_ref()
    }
}

impl Eq for MemoryKey {}

impl Hash for MemoryKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.task_id.as_ref().hash(state);
        self.tenant.hash(state);
    }
}

impl WorkerMemoryPool {
    pub fn new(budgets: StorageMemoryBudgetConfig) -> Arc<Self> {
        Arc::new(Self {
            total_limit_bytes: budgets.total_limit_bytes.max(1),
            budgets,
            used_total: Arc::new(AtomicUsize::new(0)),
            used_by_key: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn tenant_budget(&self, tenant: TenantType) -> TenantBudget {
        match tenant {
            TenantType::WriteBuffer => self.budgets.write_buffer,
            TenantType::BaseRuns => self.budgets.base_runs,
            TenantType::StateCache => self.budgets.state_cache,
        }
    }

    pub fn total_limit_bytes(&self) -> usize {
        self.total_limit_bytes
    }

    pub fn total_used(&self) -> usize {
        self.used_total.load(Ordering::Relaxed)
    }

    pub fn used_for_tenant(&self, tenant: TenantType) -> usize {
        let map = self.used_by_key.lock().expect("memory pool");
        map.iter()
            .filter(|(k, _)| k.tenant == tenant)
            .map(|(_, v)| v.used_bytes)
            .sum()
    }

    pub fn used(&self, task_id: &TaskId, tenant: TenantType) -> usize {
        let map = self.used_by_key.lock().expect("memory pool");
        map.get(&MemoryKey {
            task_id: task_id.clone(),
            tenant,
        })
        .map(|u| u.used_bytes)
        .unwrap_or(0)
    }

    pub fn is_above_soft(&self, task_id: &TaskId, tenant: TenantType) -> bool {
        let used = self.used(task_id, tenant);
        used > self.tenant_budget(tenant).soft_max_bytes
    }

    /// Try to reserve bytes within the worker-wide limit. Returns false if the limit would be exceeded.
    pub fn try_reserve(&self, task_id: &TaskId, tenant: TenantType, bytes: usize) -> bool {
        if bytes == 0 {
            return true;
        }
        let mut map = self.used_by_key.lock().expect("memory pool");
        let total_used = self.used_total.load(Ordering::Relaxed);
        if total_used.saturating_add(bytes) > self.total_limit_bytes {
            return false;
        }
        self.reserve_locked(&mut map, task_id, tenant, bytes);
        true
    }

    /// Reserve bytes even if over limit (used for legacy paths that rely on pressure relief).
    pub fn reserve_over_limit(&self, task_id: &TaskId, tenant: TenantType, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let mut map = self.used_by_key.lock().expect("memory pool");
        self.reserve_locked(&mut map, task_id, tenant, bytes);
    }

    pub fn release(&self, task_id: &TaskId, tenant: TenantType, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let mut map = self.used_by_key.lock().expect("memory pool");
        let key = MemoryKey {
            task_id: task_id.clone(),
            tenant,
        };
        if let Some(entry) = map.get_mut(&key) {
            entry.used_bytes = entry.used_bytes.saturating_sub(bytes);
            if entry.used_bytes == 0 {
                map.remove(&key);
            }
        }
        self.used_total.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn reset_used(&self, task_id: &TaskId, tenant: TenantType, new_used: usize) {
        let mut map = self.used_by_key.lock().expect("memory pool");
        let key = MemoryKey {
            task_id: task_id.clone(),
            tenant,
        };
        let prev = map.get(&key).map(|u| u.used_bytes).unwrap_or(0);
        if new_used == 0 {
            map.remove(&key);
        } else {
            map.insert(key, TenantUsage { used_bytes: new_used });
        }
        if new_used >= prev {
            self.used_total.fetch_add(new_used - prev, Ordering::Relaxed);
        } else {
            self.used_total.fetch_sub(prev - new_used, Ordering::Relaxed);
        }
    }

    fn reserve_locked(
        &self,
        map: &mut HashMap<MemoryKey, TenantUsage>,
        task_id: &TaskId,
        tenant: TenantType,
        bytes: usize,
    ) {
        let key = MemoryKey {
            task_id: task_id.clone(),
            tenant,
        };
        let entry = map.entry(key).or_insert(TenantUsage { used_bytes: 0 });
        entry.used_bytes = entry.used_bytes.saturating_add(bytes);
        self.used_total.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Concurrency-only limiter for transient scratch usage (compaction + store IO).
#[derive(Clone, Debug)]
pub struct ScratchLimiter {
    semaphore: Arc<tokio::sync::Semaphore>,
}

impl ScratchLimiter {
    pub fn new(max_inflight: usize) -> Self {
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(max_inflight.max(1))),
        }
    }

    pub async fn acquire(&self) -> ScratchPermit {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("scratch limiter");
        ScratchPermit {
            _permit: Arc::new(permit),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScratchPermit {
    _permit: Arc<tokio::sync::OwnedSemaphorePermit>,
}
