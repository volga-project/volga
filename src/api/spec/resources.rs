use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceProfile {
    pub cpu_millis: Option<u32>,
    pub memory_bytes: Option<u64>,
}

impl Default for ResourceProfile {
    fn default() -> Self {
        Self {
            cpu_millis: None,
            memory_bytes: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ResourceStrategy {
    PerWorker,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceProfiles {
    pub worker_default: ResourceProfile,
}

impl Default for ResourceProfiles {
    fn default() -> Self {
        Self {
            worker_default: ResourceProfile::default(),
        }
    }
}
