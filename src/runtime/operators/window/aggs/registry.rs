use std::collections::HashMap;
use std::sync::Arc;

use datafusion::logical_expr::Accumulator;
use datafusion::physical_expr::window::{PlainAggregateWindowExpr, SlidingAggregateWindowExpr};
use datafusion::physical_plan::WindowExpr;

use crate::runtime::operators::window::cate::types::{AggFlavor, CATE_KINDS};
use crate::runtime::operators::window::top::accumulators::frequency::{
    TOP1_RATIO_NAME, TOPN_FREQUENCY_NAME,
};
use crate::runtime::operators::window::top::accumulators::ratio::{
    TOP_N_KEY_RATIO_CATE_NAME, TOP_N_VALUE_RATIO_CATE_NAME,
};
use crate::runtime::operators::window::top::accumulators::value::TOP_NAME;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccumulatorType {
    /// Each result is rebuilt from raw rows and tiles.
    PlainAccumulator,
    /// Results can slide incrementally from the previous accumulator state.
    RetractableAccumulator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum AggKind {
    Sum,
    Avg,
    Count,
    Min,
    Max,
    Stddev,
    StddevPop,
    StddevSamp,
    VarPop,
    VarSamp,
    Variance,
}

impl AggKind {
    pub const ALL: [AggKind; 11] = [
        AggKind::Sum,
        AggKind::Avg,
        AggKind::Count,
        AggKind::Min,
        AggKind::Max,
        AggKind::Stddev,
        AggKind::StddevPop,
        AggKind::StddevSamp,
        AggKind::VarPop,
        AggKind::VarSamp,
        AggKind::Variance,
    ];

    pub fn name(&self) -> &'static str {
        match self {
            AggKind::Sum => "sum",
            AggKind::Avg => "avg",
            AggKind::Count => "count",
            AggKind::Min => "min",
            AggKind::Max => "max",
            AggKind::Stddev => "stddev",
            AggKind::StddevPop => "stddev_pop",
            AggKind::StddevSamp => "stddev_samp",
            AggKind::VarPop => "var_pop",
            AggKind::VarSamp => "var_samp",
            AggKind::Variance => "variance",
        }
    }

    pub fn accumulator_type(&self) -> AccumulatorType {
        match self {
            AggKind::Min | AggKind::Max => AccumulatorType::PlainAccumulator,
            _ => AccumulatorType::RetractableAccumulator,
        }
    }
}

struct AggregateRegistry {
    accumulator_types: HashMap<String, AccumulatorType>,
}

impl Default for AggregateRegistry {
    fn default() -> Self {
        let mut registry = Self {
            accumulator_types: HashMap::new(),
        };

        for kind in AggKind::ALL {
            registry.register_aggregate(kind.name(), kind.accumulator_type());
        }
        for kind in CATE_KINDS {
            for flavor in AggFlavor::ALL {
                let name = format!("{}{}", kind.name(), flavor.suffix());
                registry.register_aggregate(&name, kind.accumulator_type());
            }
        }
        // top_n_key/value_{sum|avg|count|min|max}_cate_where (same kinds as SQL UDAFs)
        for kind in [
            AggKind::Sum,
            AggKind::Avg,
            AggKind::Count,
            AggKind::Min,
            AggKind::Max,
        ] {
            let key = format!("top_n_key_{}_cate_where", kind.name());
            let value = format!("top_n_value_{}_cate_where", kind.name());
            registry.register_aggregate(&key, kind.accumulator_type());
            registry.register_aggregate(&value, kind.accumulator_type());
        }
        registry.register_aggregate(TOP_NAME, AccumulatorType::PlainAccumulator);
        registry.register_aggregate(TOPN_FREQUENCY_NAME, AccumulatorType::RetractableAccumulator);
        registry.register_aggregate(TOP1_RATIO_NAME, AccumulatorType::RetractableAccumulator);
        registry.register_aggregate(
            TOP_N_KEY_RATIO_CATE_NAME,
            AccumulatorType::RetractableAccumulator,
        );
        registry.register_aggregate(
            TOP_N_VALUE_RATIO_CATE_NAME,
            AccumulatorType::RetractableAccumulator,
        );

        registry
    }
}

impl AggregateRegistry {
    fn register_aggregate(&mut self, name: &str, accumulator_type: AccumulatorType) {
        self.accumulator_types
            .insert(name.to_lowercase(), accumulator_type);
    }

    fn get_accumulator_type(&self, name: &str) -> Option<AccumulatorType> {
        self.accumulator_types.get(&name.to_lowercase()).copied()
    }
}

pub fn create_window_accumulator(window_expr: &Arc<dyn WindowExpr>) -> Box<dyn Accumulator> {
    let registry = get_aggregate_registry();
    let agg_expr = extract_aggregate_expr(window_expr);
    let agg_name = agg_expr.fun().name();
    let accumulator_type = registry
        .get_accumulator_type(&agg_name)
        .unwrap_or_else(|| panic!("Unsupported aggregate function: {}", agg_name));

    match accumulator_type {
        AccumulatorType::PlainAccumulator => agg_expr
            .create_accumulator()
            .expect("Failed to create plain accumulator"),
        AccumulatorType::RetractableAccumulator => agg_expr
            .create_sliding_accumulator()
            .expect("Failed to create retractable accumulator"),
    }
}

/// Aggs whose sliding state is component-wise invertible (tile retract via state subtract).
/// Min/max/top-n/etc. cannot retract a tile without the underlying rows.
fn supports_tile_slide(agg_name: &str) -> bool {
    ["sum", "count", "avg"].contains(&agg_name.to_lowercase().as_str())
}

pub fn window_supports_tile_slide(window_expr: &Arc<dyn WindowExpr>) -> bool {
    supports_tile_slide(extract_aggregate_expr(window_expr).fun().name())
}

fn extract_aggregate_expr(
    window_expr: &Arc<dyn WindowExpr>,
) -> &datafusion::physical_expr::aggregate::AggregateFunctionExpr {
    if let Some(plain_expr) = window_expr
        .as_any()
        .downcast_ref::<PlainAggregateWindowExpr>()
    {
        return plain_expr.get_aggregate_expr();
    }

    if let Some(sliding_expr) = window_expr
        .as_any()
        .downcast_ref::<SlidingAggregateWindowExpr>()
    {
        return sliding_expr.get_aggregate_expr();
    }

    std::panic::panic_any(
        "Window expression is neither PlainAggregateWindowExpr nor SlidingAggregateWindowExpr",
    )
}

fn get_aggregate_registry() -> &'static AggregateRegistry {
    static REGISTRY: std::sync::OnceLock<AggregateRegistry> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(AggregateRegistry::default)
}

pub(crate) fn get_accumulator_type(window_expr: &Arc<dyn WindowExpr>) -> AccumulatorType {
    let registry = get_aggregate_registry();
    let agg_expr = extract_aggregate_expr(window_expr);
    let agg_name = agg_expr.fun().name();
    registry
        .get_accumulator_type(&agg_name)
        .unwrap_or_else(|| panic!("Unsupported aggregate function: {}", agg_name))
}
