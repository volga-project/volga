use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, GroupsAccumulator, ReversedUDAF, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;

use super::grouped_topk::{FrequencyMode, FrequencyTopKAccumulator};
use super::value_topk::TopValueAccumulator;

#[derive(Debug, Clone, Copy)]
enum TopUdfKind {
    TopValue,
    TopNFrequency,
    Top1Ratio,
}

#[derive(Debug, Clone)]
struct TopUdf {
    name: String,
    kind: TopUdfKind,
    signature: Signature,
}

impl TopUdf {
    fn new(name: &str, kind: TopUdfKind) -> Self {
        Self {
            name: name.to_string(),
            kind,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for TopUdf {
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
        match self.kind {
            TopUdfKind::TopValue | TopUdfKind::TopNFrequency => Ok(DataType::Utf8),
            TopUdfKind::Top1Ratio => Ok(DataType::Float64),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match self.kind {
            TopUdfKind::TopValue => {
                if arg_types.len() != 2 {
                    return exec_err!("{} expects 2 arguments", self.name);
                }
                Ok(vec![arg_types[0].clone(), DataType::Int64])
            }
            TopUdfKind::TopNFrequency => {
                if arg_types.len() != 2 {
                    return exec_err!("{} expects 2 arguments", self.name);
                }
                Ok(vec![arg_types[0].clone(), DataType::Int64])
            }
            TopUdfKind::Top1Ratio => {
                if arg_types.len() != 1 {
                    return exec_err!("{} expects 1 argument", self.name);
                }
                Ok(vec![arg_types[0].clone()])
            }
        }
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        match self.kind {
            TopUdfKind::TopValue => Ok(Box::new(TopValueAccumulator::new())),
            TopUdfKind::TopNFrequency => Ok(Box::new(FrequencyTopKAccumulator::new(
                FrequencyMode::TopN,
            ))),
            TopUdfKind::Top1Ratio => Ok(Box::new(FrequencyTopKAccumulator::new(
                FrequencyMode::Top1Ratio,
            ))),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Arc<arrow::datatypes::Field>>> {
        Ok(vec![Arc::new(arrow::datatypes::Field::new(
            format!("{}_state", args.name),
            DataType::Binary,
            true,
        ))])
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        false
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        exec_err!("groups accumulator not supported")
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }
}

fn top_udaf(name: &str, kind: TopUdfKind) -> AggregateUDF {
    AggregateUDF::from(TopUdf::new(name, kind))
}

pub fn register_top_udafs(ctx: &SessionContext) {
    let fns = vec![
        top_udaf("top", TopUdfKind::TopValue),
        top_udaf("topn_frequency", TopUdfKind::TopNFrequency),
        top_udaf("top1_ratio", TopUdfKind::Top1Ratio),
    ];
    for f in fns {
        ctx.register_udaf(f);
    }
}
