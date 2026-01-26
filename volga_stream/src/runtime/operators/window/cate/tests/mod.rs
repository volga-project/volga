use std::sync::Arc;

use datafusion::scalar::ScalarValue;

use crate::runtime::operators::window::aggregates::test_utils;
use crate::runtime::operators::window::{create_window_aggregator, WindowAggregator};

mod basic;
mod matrix;
mod ratio;
mod top_n_key;
mod top_n_value;

pub(super) fn eval_window_expr(
    window_expr: &Arc<dyn datafusion::physical_plan::WindowExpr>,
) -> ScalarValue {
    let batches = [
        test_utils::batch(&[(1000, 1.0, "A", 0), (2000, 3.0, "A", 1)]),
        test_utils::batch(&[(3000, 4.0, "B", 2), (4000, 2.0, "B", 3)]),
    ];
    let mut acc = match create_window_aggregator(window_expr) {
        WindowAggregator::Accumulator(acc) => acc,
        _ => panic!("expected accumulator"),
    };
    for batch in batches {
        let args = window_expr.evaluate_args(&batch).expect("eval args");
        acc.update_batch(&args).expect("update");
    }
    acc.evaluate().expect("evaluate")
}
