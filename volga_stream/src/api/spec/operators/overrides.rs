use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::transport::transport_spec::OperatorTransportSpec;
use crate::api::spec::operators::tuning::OperatorTuningSpec;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OperatorOverride {
    pub transport: Option<OperatorTransportSpec>,
    pub tuning: Option<OperatorTuningSpec>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OperatorOverrides {
    pub defaults: OperatorOverride,
    pub per_operator: HashMap<String, OperatorOverride>,
}

