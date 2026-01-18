use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::logical_expr::AggregateUDF;
use datafusion::scalar::ScalarValue;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum AggKind {
    Sum,
    Avg,
    Count,
    Min,
    Max,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum AggFlavor {
    Where,
    Cate,
    CateWhere,
}

#[derive(Debug, Clone)]
pub(crate) struct CateUdfSpec {
    pub(crate) name: String,
    pub(crate) kind: AggKind,
    pub(crate) flavor: AggFlavor,
    pub(crate) base_udaf: Arc<AggregateUDF>,
}

impl CateUdfSpec {
    pub(crate) fn new(
        name: &str,
        kind: AggKind,
        flavor: AggFlavor,
        base_udaf: Arc<AggregateUDF>,
    ) -> Self {
        Self {
            name: name.to_string(),
            kind,
            flavor,
            base_udaf,
        }
    }

    pub(crate) fn has_condition(&self) -> bool {
        matches!(self.flavor, AggFlavor::Where | AggFlavor::CateWhere)
    }

    pub(crate) fn has_category(&self) -> bool {
        matches!(self.flavor, AggFlavor::Cate | AggFlavor::CateWhere)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EncodedStateBytes {
    pub(crate) single: Option<Vec<Vec<u8>>>,
    pub(crate) cate: Option<Vec<(Vec<u8>, Vec<Vec<u8>>)>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CateKey {
    pub(crate) hash: u64,
    pub(crate) value: ScalarValue,
}

impl Hash for CateKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use the precomputed hash for fast lookup; ScalarValue disambiguates collisions
        // without per-row string conversion.
        self.hash.hash(state);
    }
}
