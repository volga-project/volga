use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion::common::{exec_err, Result};
use datafusion::functions_aggregate::{
    average::avg_udaf,
    count::count_udaf,
    min_max::{max_udaf, min_udaf},
    sum::sum_udaf,
};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, GroupsAccumulator, ReversedUDAF, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;

use super::accumulator::CateAccumulator;
use crate::runtime::operators::window::top::accumulators::grouped_agg::GroupedAggTopKAccumulator;
use crate::runtime::operators::window::top::accumulators::ratio::{
    RatioTopKAccumulator, TOP_N_KEY_RATIO_CATE_NAME, TOP_N_VALUE_RATIO_CATE_NAME,
};
use crate::runtime::operators::window::top::heap::TopKOrder;
use super::types::{AggFlavor, CateUdfSpec};
use crate::runtime::operators::window::aggregates::AggKind;
use super::utils::{coerce_value_type, df_error};

#[derive(Debug, Clone)]
struct CateUdf {
    spec: CateUdfSpec,
    signature: Signature,
}

impl CateUdf {
    fn new(spec: CateUdfSpec) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            spec,
        }
    }

    fn state_fields_impl(&self, args: StateFieldsArgs) -> Result<Vec<Arc<Field>>> {
        let field = Field::new(
            format!("{}_state", args.name),
            DataType::Binary,
            true,
        );
        Ok(vec![Arc::new(field)])
    }
}

impl AggregateUDFImpl for CateUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.spec.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if self.spec.has_category() {
            return Ok(DataType::Utf8);
        }
        if arg_types.is_empty() {
            return exec_err!("{} expects at least 1 argument", self.spec.name);
        }
        let coerced =
            coerce_value_type(self.spec.kind, self.spec.base_udaf.as_ref(), &arg_types[0])?;
        self.spec.base_udaf.return_type(&[coerced])
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let expected_args = match self.spec.flavor {
            AggFlavor::Where => 2,
            AggFlavor::Cate => 2,
            AggFlavor::CateWhere => 3,
        };
        if arg_types.len() != expected_args {
            return exec_err!(
                "{} expects {} arguments, got {}",
                self.spec.name,
                expected_args,
                arg_types.len()
            );
        }

        let mut out: Vec<DataType> = Vec::with_capacity(arg_types.len());
        let coerced =
            coerce_value_type(self.spec.kind, self.spec.base_udaf.as_ref(), &arg_types[0])?;
        out.push(coerced);

        let mut idx = 1;
        if self.spec.has_condition() {
            out.push(DataType::Boolean);
            idx += 1;
        }
        if self.spec.has_category() {
            let cate_type = &arg_types[idx];
            let supported = matches!(
                cate_type,
                DataType::Utf8
                    | DataType::Int64
                    | DataType::Int32
                    | DataType::UInt64
                    | DataType::UInt32
                    | DataType::Float64
                    | DataType::Float32
                    | DataType::Boolean
            );
            if !supported {
                return exec_err!("{} unsupported category type {}", self.spec.name, cate_type);
            }
            out.push(cate_type.clone());
        }

        Ok(out)
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CateAccumulator::new(
            &self.spec,
            self.spec.base_udaf.clone(),
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Arc<Field>>> {
        self.state_fields_impl(args)
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        false
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Err(df_error("groups accumulator not supported"))
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }
}

fn cate_udaf(spec: CateUdfSpec) -> AggregateUDF {
    AggregateUDF::from(CateUdf::new(spec))
}

#[derive(Debug, Clone)]
enum CateTopMode {
    Agg { kind: AggKind, base_udaf: Arc<AggregateUDF> },
    Ratio,
}

#[derive(Debug, Clone)]
struct CateTopUdf {
    name: String,
    order: TopKOrder,
    mode: CateTopMode,
    signature: Signature,
}

impl CateTopUdf {
    fn new(name: &str, order: TopKOrder, mode: CateTopMode) -> Self {
        Self {
            name: name.to_string(),
            order,
            mode,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for CateTopUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 4 {
            return exec_err!("{} expects 4 arguments", self.name);
        }
        let mut out: Vec<DataType> = Vec::with_capacity(arg_types.len());
        match &self.mode {
            CateTopMode::Agg { kind, base_udaf } => {
                let coerced = coerce_value_type(*kind, base_udaf.as_ref(), &arg_types[0])?;
                out.push(coerced);
            }
            CateTopMode::Ratio => {
                out.push(arg_types[0].clone());
            }
        }
        out.push(DataType::Boolean);
        let cate_type = &arg_types[2];
        let supported = matches!(
            cate_type,
            DataType::Utf8
                | DataType::Int64
                | DataType::Int32
                | DataType::UInt64
                | DataType::UInt32
                | DataType::Float64
                | DataType::Float32
                | DataType::Boolean
        );
        if !supported {
            return exec_err!("{} unsupported category type {}", self.name, cate_type);
        }
        out.push(cate_type.clone());
        out.push(DataType::Int64);
        Ok(out)
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        match &self.mode {
            CateTopMode::Agg { kind, base_udaf } => Ok(Box::new(
                GroupedAggTopKAccumulator::new(*kind, base_udaf.clone(), self.order),
            )),
            CateTopMode::Ratio => Ok(Box::new(RatioTopKAccumulator::new(self.order))),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Arc<Field>>> {
        let field = Field::new(format!("{}_state", args.name), DataType::Binary, true);
        Ok(vec![Arc::new(field)])
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        false
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Err(df_error("groups accumulator not supported"))
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }
}

fn cate_top_udaf(name: &str, order: TopKOrder, mode: CateTopMode) -> AggregateUDF {
    AggregateUDF::from(CateTopUdf::new(name, order, mode))
}

fn base_udaf(kind: AggKind) -> Arc<AggregateUDF> {
    match kind {
        AggKind::Sum => sum_udaf(),
        AggKind::Avg => avg_udaf(),
        AggKind::Count => count_udaf(),
        AggKind::Min => min_udaf(),
        AggKind::Max => max_udaf(),
        _ => unreachable!("unsupported agg kind for cate UDAF"),
    }
}

pub fn register_cate_udafs(ctx: &SessionContext) {
    let fns = vec![
        cate_udaf(CateUdfSpec::new(
            "sum_where",
            AggKind::Sum,
            AggFlavor::Where,
            base_udaf(AggKind::Sum),
        )),
        cate_udaf(CateUdfSpec::new(
            "avg_where",
            AggKind::Avg,
            AggFlavor::Where,
            base_udaf(AggKind::Avg),
        )),
        cate_udaf(CateUdfSpec::new(
            "count_where",
            AggKind::Count,
            AggFlavor::Where,
            base_udaf(AggKind::Count),
        )),
        cate_udaf(CateUdfSpec::new(
            "min_where",
            AggKind::Min,
            AggFlavor::Where,
            base_udaf(AggKind::Min),
        )),
        cate_udaf(CateUdfSpec::new(
            "max_where",
            AggKind::Max,
            AggFlavor::Where,
            base_udaf(AggKind::Max),
        )),
        cate_udaf(CateUdfSpec::new(
            "sum_cate",
            AggKind::Sum,
            AggFlavor::Cate,
            base_udaf(AggKind::Sum),
        )),
        cate_udaf(CateUdfSpec::new(
            "avg_cate",
            AggKind::Avg,
            AggFlavor::Cate,
            base_udaf(AggKind::Avg),
        )),
        cate_udaf(CateUdfSpec::new(
            "count_cate",
            AggKind::Count,
            AggFlavor::Cate,
            base_udaf(AggKind::Count),
        )),
        cate_udaf(CateUdfSpec::new(
            "min_cate",
            AggKind::Min,
            AggFlavor::Cate,
            base_udaf(AggKind::Min),
        )),
        cate_udaf(CateUdfSpec::new(
            "max_cate",
            AggKind::Max,
            AggFlavor::Cate,
            base_udaf(AggKind::Max),
        )),
        cate_udaf(CateUdfSpec::new(
            "sum_cate_where",
            AggKind::Sum,
            AggFlavor::CateWhere,
            base_udaf(AggKind::Sum),
        )),
        cate_udaf(CateUdfSpec::new(
            "avg_cate_where",
            AggKind::Avg,
            AggFlavor::CateWhere,
            base_udaf(AggKind::Avg),
        )),
        cate_udaf(CateUdfSpec::new(
            "count_cate_where",
            AggKind::Count,
            AggFlavor::CateWhere,
            base_udaf(AggKind::Count),
        )),
        cate_udaf(CateUdfSpec::new(
            "min_cate_where",
            AggKind::Min,
            AggFlavor::CateWhere,
            base_udaf(AggKind::Min),
        )),
        cate_udaf(CateUdfSpec::new(
            "max_cate_where",
            AggKind::Max,
            AggFlavor::CateWhere,
            base_udaf(AggKind::Max),
        )),
    ];
    for f in fns {
        ctx.register_udaf(f);
    }

    let top_aggs = [
        AggKind::Sum,
        AggKind::Avg,
        AggKind::Count,
        AggKind::Min,
        AggKind::Max,
    ];
    for kind in top_aggs {
        let base = base_udaf(kind);
        let key_name = format!("top_n_key_{}_cate_where", kind.name());
        ctx.register_udaf(cate_top_udaf(
            &key_name,
            TopKOrder::KeyDesc,
            CateTopMode::Agg { kind, base_udaf: base.clone() },
        ));
        let value_name = format!("top_n_value_{}_cate_where", kind.name());
        ctx.register_udaf(cate_top_udaf(
            &value_name,
            TopKOrder::MetricDesc,
            CateTopMode::Agg { kind, base_udaf: base },
        ));
    }

    ctx.register_udaf(cate_top_udaf(
        TOP_N_KEY_RATIO_CATE_NAME,
        TopKOrder::KeyDesc,
        CateTopMode::Ratio,
    ));
    ctx.register_udaf(cate_top_udaf(
        TOP_N_VALUE_RATIO_CATE_NAME,
        TopKOrder::MetricDesc,
        CateTopMode::Ratio,
    ));
}
