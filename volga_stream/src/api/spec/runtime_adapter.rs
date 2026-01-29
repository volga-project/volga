use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeAdapterKind {
    Local,
    K8s,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeAdapterSpec {
    Local,
    K8s,
}

impl RuntimeAdapterSpec {
    pub fn kind(&self) -> RuntimeAdapterKind {
        match self {
            RuntimeAdapterSpec::Local => RuntimeAdapterKind::Local,
            RuntimeAdapterSpec::K8s => RuntimeAdapterKind::K8s,
        }
    }
}

impl Default for RuntimeAdapterSpec {
    fn default() -> Self {
        Self::Local
    }
}
