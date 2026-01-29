use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeAdapterSpec {
    Local,
    K8s,
}

impl Default for RuntimeAdapterSpec {
    fn default() -> Self {
        Self::Local
    }
}
