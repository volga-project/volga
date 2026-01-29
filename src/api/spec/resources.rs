use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourceProfile {
    pub cpu_request: Option<String>,
    pub cpu_limit: Option<String>,
    pub mem_request: Option<String>,
    pub mem_limit: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ResourceStrategy {
    PerWorker,
    PerOperatorType,
}

impl Default for ResourceStrategy {
    fn default() -> Self {
        Self::PerWorker
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ResourceProfiles {
    pub master_default: ResourceProfile,
    pub worker_default: ResourceProfile,
    pub stateless_default: ResourceProfile,
    pub stateful_by_type: HashMap<String, ResourceProfile>,
}
