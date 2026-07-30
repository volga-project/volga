use std::sync::Arc;

use datafusion::scalar::ScalarValue;

use crate::runtime::operators::operator::{OperatorConfig, OperatorPollResult, OperatorTrait};
use crate::runtime::operators::window::model::Cursor;
use crate::runtime::operators::window::operator::{WindowOperatorConfig, WindowOutputMode};
use crate::runtime::operators::window::request::{
    WindowRequestOperator, WindowRequestOperatorConfig,
};
use crate::runtime::operators::window::spec::WindowAdvancePolicy;
use crate::runtime::operators::window::spec::WindowSpec;
use crate::runtime::operators::window::store::{
    InMemWindowStore, PartitionKey, StateNamespace, WindowOperatorStore,
};
use crate::runtime::operators::window::tests::harness::{
    assert_window_values, batch, key, keyed_message, runtime_context_with_namespace,
    window_exec_from_sql, Harness, WoWroHarness,
};

const SQL: &str = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '5000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;

async fn open_wro(
    store: Arc<InMemWindowStore>,
    namespace: StateNamespace,
) -> WindowRequestOperator {
    let exec = window_exec_from_sql(SQL).await;
    let mut cfg =
        WindowRequestOperatorConfig::from_window_operator_config(WindowOperatorConfig::new(exec));
    cfg.exclude_current_row = true;
    let ctx = runtime_context_with_namespace(store, namespace);
    let mut wro = WindowRequestOperator::new(OperatorConfig::WindowRequestConfig(cfg));
    wro.open(&ctx).await.expect("wro open");
    wro
}

async fn request_sum(wro: &mut WindowRequestOperator, partition: &str, ts: i64) -> ScalarValue {
    wro.set_input(Some(Box::pin(futures::stream::iter(vec![keyed_message(
        batch(vec![ts], vec![0.0], vec![partition]),
        partition,
    )]))));
    let out = match wro.poll_next().await {
        OperatorPollResult::Ready(crate::common::message::Message::Regular(base)) => {
            base.record_batch
        }
        other => panic!("expected WRO result, got {other:?}"),
    };
    ScalarValue::try_from_array(out.column(out.num_columns() - 1), 0).expect("sum")
}

#[tokio::test]
async fn duplicate_timestamps_across_batches_follow_sequence_frontier() {
    let exec = window_exec_from_sql(SQL).await;
    let mut h = Harness::new(WindowOperatorConfig::new(exec)).await;

    h.ingest(batch(vec![1000, 1000], vec![1.0, 2.0], vec!["A", "A"]), "A")
        .await;
    h.ingest(batch(vec![1000], vec![3.0], vec!["A"]), "A").await;
    let first = h.watermark_and_output(1000).await;
    let _ = h.drain_passthrough_watermark().await;
    assert_window_values(
        &first,
        3,
        &[
            vec![ScalarValue::Float64(Some(1.0))],
            vec![ScalarValue::Float64(Some(3.0))],
            vec![ScalarValue::Float64(Some(6.0))],
        ],
        "duplicate batches",
    );

    h.ingest(batch(vec![999, 1000], vec![99.0, 4.0], vec!["A", "A"]), "A")
        .await;
    let second = h.watermark_and_output(1000).await;
    let _ = h.drain_passthrough_watermark().await;
    assert_window_values(
        &second,
        3,
        &[vec![ScalarValue::Float64(Some(10.0))]],
        "sequence-sensitive drop",
    );
}

#[tokio::test]
async fn partial_watermark_keeps_key_pending_without_more_ingest() {
    let exec = window_exec_from_sql(SQL).await;
    let mut h = Harness::new(WindowOperatorConfig::new(exec)).await;
    h.ingest(batch(vec![1000, 5000], vec![1.0, 5.0], vec!["A", "A"]), "A")
        .await;

    let partial = h.watermark_and_output(2000).await;
    let _ = h.drain_passthrough_watermark().await;
    assert_window_values(
        &partial,
        3,
        &[vec![ScalarValue::Float64(Some(1.0))]],
        "partial watermark",
    );

    let rest = h.watermark_and_output(5000).await;
    let _ = h.drain_passthrough_watermark().await;
    assert_window_values(
        &rest,
        3,
        &[vec![ScalarValue::Float64(Some(6.0))]],
        "pending key",
    );
}

#[tokio::test]
async fn watermark_without_ingest_is_empty_and_passes_through() {
    let exec = window_exec_from_sql(SQL).await;
    let mut h = Harness::new(WindowOperatorConfig::new(exec)).await;

    let out = h.watermark_and_output(1000).await;
    assert_eq!(out.num_rows(), 0);
    assert_eq!(h.drain_passthrough_watermark().await, 1000);
}

#[tokio::test]
async fn lateness_config_sets_retention_floor_in_meta() {
    let exec = window_exec_from_sql(SQL).await;
    let mut cfg = WindowOperatorConfig::new(exec);
    cfg.spec = WindowSpec {
        lateness: Some(2000),
        ..Default::default()
    };
    let mut h = Harness::new(cfg).await;
    h.ingest(batch(vec![10_000], vec![1.0], vec!["A"]), "A")
        .await;
    let _ = h.watermark_and_output(10_000).await;
    let _ = h.drain_passthrough_watermark().await;

    let partition = PartitionKey::new(&h.namespace, &key("A"));
    let meta = h.store.load_meta(&partition).await.expect("meta");
    assert_eq!(meta.retention_floor, Some(Cursor::new(3000, 0)));
}

#[tokio::test]
async fn on_ingest_advances_state_only_without_watermark() {
    let exec = window_exec_from_sql(SQL).await;
    let mut cfg = WindowOperatorConfig::new(exec);
    cfg.output_mode = WindowOutputMode::StateOnly;
    cfg.spec.advance_policy = WindowAdvancePolicy::OnIngest;
    let mut h = Harness::new(cfg).await;
    h.ingest(batch(vec![1000, 2000], vec![1.0, 2.0], vec!["A", "A"]), "A")
        .await;

    let partition = PartitionKey::new(&h.namespace, &key("A"));
    let meta = h.store.load_meta(&partition).await.expect("meta");
    assert_eq!(meta.processed_pos, Some(Cursor::new(2000, 1)));

    let mut wro = open_wro(h.store.clone(), h.namespace.clone()).await;
    assert_eq!(
        request_sum(&mut wro, "A", 2000).await,
        ScalarValue::Float64(Some(3.0))
    );
}

#[tokio::test]
async fn state_only_and_emit_publish_equivalent_request_state() {
    let events = batch(
        vec![1000, 2000, 3000],
        vec![1.0, 2.0, 3.0],
        vec!["A", "A", "A"],
    );

    let mut state_only = WoWroHarness::new(SQL, None, true).await;
    state_only.ingest(events.clone(), "A").await;
    state_only.advance(3000).await;
    let state_result = state_only
        .request(batch(vec![3000], vec![0.0], vec!["A"]), "A")
        .await;

    let exec = window_exec_from_sql(SQL).await;
    let mut emit = Harness::new(WindowOperatorConfig::new(exec)).await;
    emit.ingest(events, "A").await;
    let _ = emit.watermark_and_output(3000).await;
    let _ = emit.drain_passthrough_watermark().await;
    let mut emit_wro = open_wro(emit.store.clone(), emit.namespace.clone()).await;
    let emit_value = request_sum(&mut emit_wro, "A", 3000).await;

    assert_eq!(
        ScalarValue::try_from_array(state_result.column(state_result.num_columns() - 1), 0,)
            .expect("state-only sum"),
        emit_value,
    );
}

#[tokio::test]
async fn keys_and_namespaces_are_isolated() {
    let shared = Arc::new(InMemWindowStore::new());
    let mut left = WoWroHarness::with_store(
        SQL,
        None,
        true,
        shared.clone(),
        StateNamespace::new(b"left"),
    )
    .await;
    let mut right =
        WoWroHarness::with_store(SQL, None, true, shared, StateNamespace::new(b"right")).await;

    left.ingest(batch(vec![1000], vec![1.0], vec!["A"]), "A")
        .await;
    left.ingest(batch(vec![1000], vec![2.0], vec!["B"]), "B")
        .await;
    right
        .ingest(batch(vec![1000], vec![10.0], vec!["A"]), "A")
        .await;
    left.advance(1000).await;
    right.advance(1000).await;

    assert_eq!(
        request_sum(&mut left.wro, "A", 1000).await,
        ScalarValue::Float64(Some(1.0))
    );
    assert_eq!(
        request_sum(&mut left.wro, "B", 1000).await,
        ScalarValue::Float64(Some(2.0))
    );
    assert_eq!(
        request_sum(&mut right.wro, "A", 1000).await,
        ScalarValue::Float64(Some(10.0))
    );
}

#[tokio::test]
async fn wro_evaluates_each_window_expression_arguments() {
    let sql = r#"SELECT timestamp, value, partition_key,
  SUM(value) OVER w as sum_val,
  SUM(value * 10) OVER w as scaled_sum
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '5000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;
    let mut h = WoWroHarness::new(sql, None, false).await;
    h.ingest(batch(vec![1000, 2000], vec![1.0, 2.0], vec!["A", "A"]), "A")
        .await;
    h.advance(2000).await;

    let out = h
        .request(batch(vec![2000], vec![3.0], vec!["A"]), "A")
        .await;
    assert_window_values(
        &out,
        3,
        &[vec![
            ScalarValue::Float64(Some(6.0)),
            ScalarValue::Float64(Some(60.0)),
        ]],
        "per-expression request arguments",
    );
}
