//! Shared WO test harness (SortedKV / InMem).

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

use crate::api::planner::{Planner, PlanningContext};
use crate::common::message::Message;
use crate::common::{Key, WatermarkMessage};
use crate::runtime::functions::key_by::key_by_function::extract_datafusion_window_exec;
use crate::runtime::operators::operator::{OperatorConfig, OperatorPollResult, OperatorTrait};
use crate::runtime::operators::source::source_operator::{SourceConfig, VectorSourceConfig};
use crate::runtime::operators::window::store::StateNamespace;
use crate::runtime::operators::window::window_operator::{
    WindowEmitMode, WindowOperator, WindowOperatorConfig,
};
use crate::runtime::operators::window::window_request_operator::{
    WindowRequestOperator, WindowRequestOperatorConfig,
};
use crate::runtime::operators::window::window_tuning::WindowOperatorSpec;
use crate::runtime::operators::window::TileConfig;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::state::OperatorStates;
use crate::storage::{InMemSortedKV, SortedKV};

pub fn test_input_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, false),
        Field::new("partition_key", DataType::Utf8, false),
    ]))
}

pub fn batch(ts: Vec<i64>, vals: Vec<f64>, keys: Vec<&str>) -> RecordBatch {
    let schema = test_input_schema();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMillisecondArray::from(ts)),
            Arc::new(Float64Array::from(vals)),
            Arc::new(StringArray::from(keys)),
        ],
    )
    .expect("test batch")
}

pub fn key(partition: &str) -> Key {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "partition",
        DataType::Utf8,
        false,
    )]));
    let arr = StringArray::from(vec![partition]);
    let key_batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("key batch");
    Key::new(key_batch).expect("key")
}

pub fn keyed_message(b: RecordBatch, partition: &str) -> Message {
    Message::new_keyed(None, b, key(partition), None, None)
}

pub fn watermark_message(wm: u64) -> Message {
    Message::Watermark(WatermarkMessage::new("test".to_string(), wm, Some(0)))
}

pub async fn window_exec_from_sql(sql: &str) -> Arc<BoundedWindowAggExec> {
    let ctx = SessionContext::new();
    let mut planner = Planner::new(PlanningContext::new(ctx));
    planner.register_source(
        "test_table".to_string(),
        SourceConfig::VectorSourceConfig(VectorSourceConfig::new(vec![])),
        test_input_schema(),
    );
    extract_datafusion_window_exec(sql, &mut planner).await
}

pub fn runtime_context(kv: Arc<dyn SortedKV>) -> RuntimeContext {
    let mut ctx = RuntimeContext::new(
        "test_vertex".to_string().into(),
        0,
        1,
        None,
        Some(Arc::new(OperatorStates::new())),
        None,
    );
    ctx.set_sorted_kv(kv);
    ctx.set_window_state_namespace(StateNamespace::new(b"window_state"));
    ctx
}

pub struct Harness {
    pub op: WindowOperator,
}

impl Harness {
    pub async fn new(cfg: WindowOperatorConfig) -> Self {
        let kv: Arc<dyn SortedKV> = Arc::new(InMemSortedKV::new());
        let ctx = runtime_context(kv);
        let mut op = WindowOperator::new(OperatorConfig::WindowConfig(cfg));
        op.open(&ctx).await.expect("open");
        Self { op }
    }

    pub async fn ingest(&mut self, b: RecordBatch, partition: &str) {
        self.op.set_input(Some(Box::pin(futures::stream::iter(vec![
            keyed_message(b, partition),
        ]))));
        assert!(matches!(
            self.op.poll_next().await,
            OperatorPollResult::Continue
        ));
    }

    pub async fn watermark_and_output(&mut self, wm: u64) -> RecordBatch {
        self.op.set_input(Some(Box::pin(futures::stream::iter(vec![
            watermark_message(wm),
        ]))));
        match self.op.poll_next().await {
            OperatorPollResult::Ready(Message::Regular(base)) => base.record_batch,
            other => panic!("expected output on watermark, got {:?}", other),
        }
    }

    pub async fn drain_passthrough_watermark(&mut self) -> u64 {
        match self.op.poll_next().await {
            OperatorPollResult::Ready(Message::Watermark(wm)) => wm.watermark_value,
            other => panic!("expected passthrough watermark, got {:?}", other),
        }
    }
}

/// Shared SortedKV: WO advances state (request mode), WRO answers point lookups.
pub struct WoWroHarness {
    pub wo: WindowOperator,
    pub wro: WindowRequestOperator,
}

impl WoWroHarness {
    pub async fn new(sql: &str, tiling: Option<TileConfig>, exclude_current_row: bool) -> Self {
        let exec = window_exec_from_sql(sql).await;
        let kv: Arc<dyn SortedKV> = Arc::new(InMemSortedKV::new());
        let ns = StateNamespace::new(b"window_state");

        let mut wo_cfg = WindowOperatorConfig::new(exec.clone());
        wo_cfg.execution_mode = WindowEmitMode::Request;
        if tiling.is_some() {
            wo_cfg.spec = WindowOperatorSpec {
                tiling: tiling.clone(),
                ..Default::default()
            };
        }
        let mut wo_ctx = runtime_context(kv.clone());
        wo_ctx.set_window_state_namespace(ns.clone());
        let mut wo = WindowOperator::new(OperatorConfig::WindowConfig(wo_cfg));
        wo.open(&wo_ctx).await.expect("wo open");

        let mut req_cfg =
            WindowRequestOperatorConfig::from_window_operator_config(WindowOperatorConfig::new(
                exec,
            ));
        req_cfg.exclude_current_row = exclude_current_row;
        if tiling.is_some() {
            req_cfg.spec = WindowOperatorSpec {
                tiling,
                ..Default::default()
            };
        }
        let mut wro_ctx = runtime_context(kv);
        wro_ctx.set_window_state_namespace(ns);
        let mut wro = WindowRequestOperator::new(OperatorConfig::WindowRequestConfig(req_cfg));
        wro.open(&wro_ctx).await.expect("wro open");

        Self { wo, wro }
    }

    pub async fn ingest(&mut self, b: RecordBatch, partition: &str) {
        self.wo.set_input(Some(Box::pin(futures::stream::iter(vec![
            keyed_message(b, partition),
        ]))));
        assert!(matches!(
            self.wo.poll_next().await,
            OperatorPollResult::Continue
        ));
    }

    /// Request-mode advance: no emit batch, then passthrough watermark.
    pub async fn advance(&mut self, wm: u64) {
        self.wo.set_input(Some(Box::pin(futures::stream::iter(vec![
            watermark_message(wm),
        ]))));
        assert!(matches!(
            self.wo.poll_next().await,
            OperatorPollResult::Continue
        ));
        match self.wo.poll_next().await {
            OperatorPollResult::Ready(Message::Watermark(w)) => {
                assert_eq!(w.watermark_value, wm);
            }
            other => panic!("expected passthrough watermark, got {:?}", other),
        }
    }

    pub async fn request(&mut self, b: RecordBatch, partition: &str) -> RecordBatch {
        self.wro.set_input(Some(Box::pin(futures::stream::iter(vec![
            keyed_message(b, partition),
        ]))));
        match self.wro.poll_next().await {
            OperatorPollResult::Ready(Message::Regular(base)) => base.record_batch,
            other => panic!("expected WRO result, got {:?}", other),
        }
    }
}

pub fn col_f64(batch: &RecordBatch, idx: usize) -> &Float64Array {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("f64")
}

/// Run ingest → watermark → emit with optional default tiling.
pub async fn run_wo(
    sql: &str,
    tiling: Option<TileConfig>,
    events: RecordBatch,
    partition: &str,
    wm: u64,
) -> RecordBatch {
    run_wo_scenario(sql, tiling, partition, &[(events, wm)]).await
}

/// Multiple ingest/watermark steps; concatenates non-empty emit batches.
pub async fn run_wo_scenario(
    sql: &str,
    tiling: Option<TileConfig>,
    partition: &str,
    steps: &[(RecordBatch, u64)],
) -> RecordBatch {
    let exec = window_exec_from_sql(sql).await;
    let mut cfg = WindowOperatorConfig::new(exec);
    if tiling.is_some() {
        cfg.spec = WindowOperatorSpec {
            tiling,
            ..WindowOperatorSpec::default()
        };
    }
    let mut h = Harness::new(cfg).await;
    let mut outs = Vec::new();
    for (events, wm) in steps {
        h.ingest(events.clone(), partition).await;
        let out = h.watermark_and_output(*wm).await;
        let _ = h.drain_passthrough_watermark().await;
        if out.num_rows() > 0 {
            outs.push(out);
        }
    }
    concat_batches(&outs)
}

fn concat_batches(batches: &[RecordBatch]) -> RecordBatch {
    assert!(!batches.is_empty(), "no emit batches");
    if batches.len() == 1 {
        return batches[0].clone();
    }
    let schema = batches[0].schema();
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for c in 0..schema.fields().len() {
        let pieces: Vec<ArrayRef> = batches.iter().map(|b| b.column(c).clone()).collect();
        cols.push(
            arrow::compute::concat(&pieces.iter().map(|a| a.as_ref()).collect::<Vec<_>>())
                .expect("concat"),
        );
    }
    RecordBatch::try_new(schema, cols).expect("concat batch")
}

/// Assert window columns (after `input_cols`) equal `expected[row][window_col]`.
pub fn assert_window_values(
    out: &RecordBatch,
    input_cols: usize,
    expected: &[Vec<ScalarValue>],
    label: &str,
) {
    assert_eq!(
        out.num_rows(),
        expected.len(),
        "{label}: rows {} vs expected {}",
        out.num_rows(),
        expected.len()
    );
    let n_win = out.num_columns() - input_cols;
    for (r, row_exp) in expected.iter().enumerate() {
        assert_eq!(row_exp.len(), n_win, "{label}: row {r} window arity");
        for (w, exp) in row_exp.iter().enumerate() {
            let c = input_cols + w;
            let name = out.schema().field(c).name().clone();
            let got = ScalarValue::try_from_array(out.column(c), r).expect("scalar");
            if !scalar_eq(&got, exp) {
                panic!("{label}: col `{name}` row {r}: got {got:?} expected {exp:?}");
            }
        }
    }
}

pub fn scalar_eq(a: &ScalarValue, b: &ScalarValue) -> bool {
    match (a, b) {
        (ScalarValue::Float64(Some(x)), ScalarValue::Float64(Some(y))) => (x - y).abs() < 1e-9,
        (ScalarValue::Float32(Some(x)), ScalarValue::Float32(Some(y))) => {
            (*x as f64 - *y as f64).abs() < 1e-6
        }
        // COUNT may surface as Int64 or UInt64 depending on DF version.
        (ScalarValue::Int64(Some(x)), ScalarValue::Int64(Some(y))) => x == y,
        (ScalarValue::Int64(Some(x)), ScalarValue::UInt64(Some(y))) => *x as u64 == *y,
        (ScalarValue::UInt64(Some(x)), ScalarValue::Int64(Some(y))) => *x == *y as u64,
        (ScalarValue::UInt64(Some(x)), ScalarValue::UInt64(Some(y))) => x == y,
        _ => a == b,
    }
}
