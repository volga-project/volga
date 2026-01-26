use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum PlacementStrategy {
    SingleNode,
    OperatorPerNode,
    RoundRobin,
}

impl Default for PlacementStrategy {
    fn default() -> Self {
        Self::SingleNode
    }
}
