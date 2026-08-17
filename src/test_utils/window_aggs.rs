use std::sync::Arc;

use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::physical_plan::WindowExpr;
use datafusion::prelude::SessionContext;

use crate::api::planner::{Planner, PlanningContext};
use crate::runtime::functions::key_by::key_by_function::extract_datafusion_window_exec;
use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
use crate::runtime::operators::window::model::Timestamp;
use crate::runtime::operators::window::SEQ_NO_COLUMN_NAME;

pub fn test_schema_with_seq() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, false),
        Field::new("partition_key", DataType::Utf8, false),
        Field::new(SEQ_NO_COLUMN_NAME, DataType::UInt64, false),
    ]))
}

pub fn batch(rows: &[(Timestamp, f64, &str, u64)]) -> RecordBatch {
    let schema = test_schema_with_seq();
    let ts = Arc::new(TimestampMillisecondArray::from(
        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
    ));
    let values = Arc::new(Float64Array::from(
        rows.iter().map(|row| row.1).collect::<Vec<_>>(),
    ));
    let partition_keys = Arc::new(StringArray::from(
        rows.iter().map(|row| row.2).collect::<Vec<_>>(),
    ));
    let sequence_numbers = Arc::new(UInt64Array::from(
        rows.iter().map(|row| row.3).collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(schema, vec![ts, values, partition_keys, sequence_numbers])
        .expect("test batch")
}

pub async fn window_exec_from_sql(sql: &str) -> Arc<BoundedWindowAggExec> {
    let ctx = SessionContext::new();
    let mut planner = Planner::new(PlanningContext::new(ctx));
    planner.register_source(
        "test_table".to_string(),
        SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])),
        test_schema_with_seq(),
    );
    extract_datafusion_window_exec(sql, &mut planner).await
}

pub async fn window_expr_from_sql(sql: &str) -> Arc<dyn WindowExpr> {
    let exec = window_exec_from_sql(sql).await;
    exec.window_expr()[0].clone()
}
