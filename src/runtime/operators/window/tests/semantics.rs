use std::sync::Arc;

use datafusion::scalar::ScalarValue;
use futures::TryStreamExt;

use crate::runtime::checkpoint::SerializedRestore;
use crate::runtime::operators::operator::{OperatorConfig, OperatorPollResult, OperatorTrait};
use crate::runtime::operators::window::model::Cursor;
use crate::runtime::operators::window::operator::{WindowOperatorConfig, WindowOutputMode};
use crate::runtime::operators::window::request::{
    WindowRequestOperator, WindowRequestOperatorConfig,
};
use crate::runtime::operators::window::store::{
    InMemWindowStore, PartitionKey, StateNamespace, WindowOperatorStore,
};
use crate::test_utils::window::harness::{
    assert_window_values, batch, key, keyed_message, runtime_context, watermark_message,
    window_exec_from_sql, Harness, WoWroHarness,
};
use crate::runtime::operators::window::{
    TASK_METADATA_ROWS_ACCEPTED, TASK_METADATA_ROWS_DROPPED_LATE,
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
    let ctx = runtime_context();
    let mut wro = WindowRequestOperator::new(OperatorConfig::WindowRequestConfig(cfg));
    wro.set_state_with_store_and_ns(store, namespace);
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
async fn duplicate_timestamps_are_ordered_before_the_watermark() {
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
    assert_eq!(second.num_rows(), 0);
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
async fn watermark_rejects_late_rows_for_unseen_key() {
    let exec = window_exec_from_sql(SQL).await;
    let mut h = Harness::new(WindowOperatorConfig::new(exec)).await;
    let _ = h.watermark_and_output(10_000).await;
    let _ = h.drain_passthrough_watermark().await;
    h.ingest(batch(vec![9_000], vec![1.0], vec!["B"]), "B")
        .await;

    let partition = PartitionKey::new(&h.namespace, &key("B"));
    let meta = h.store.load_key_state(&partition).await.expect("state");
    assert_eq!(meta.next_seq, 0);
    assert_eq!(
        h.task_metadata.get(TASK_METADATA_ROWS_ACCEPTED).as_deref(),
        Some("0")
    );
    assert_eq!(
        h.task_metadata
            .get(TASK_METADATA_ROWS_DROPPED_LATE)
            .as_deref(),
        Some("1")
    );
}

#[tokio::test]
async fn state_only_publishes_on_ingest_and_advances_on_watermark() {
    let exec = window_exec_from_sql(SQL).await;
    let mut cfg = WindowOperatorConfig::new(exec);
    cfg.output_mode = WindowOutputMode::StateOnly;
    let mut h = Harness::new(cfg).await;
    h.ingest(batch(vec![1000, 2000], vec![1.0, 2.0], vec!["A", "A"]), "A")
        .await;

    let partition = PartitionKey::new(&h.namespace, &key("A"));
    let meta = h.store.load_key_state(&partition).await.expect("state");
    assert!(meta.evaluation.is_none());
    let mut due = h
        .store
        .stream_due(&h.namespace, None, Cursor::new(2000, u64::MAX));
    assert!(due.try_next().await.expect("due page").is_none());

    let mut wro = open_wro(h.store.clone(), h.namespace.clone()).await;
    assert_eq!(
        request_sum(&mut wro, "A", 2000).await,
        ScalarValue::Float64(Some(3.0))
    );

    h.op.set_input(Some(Box::pin(futures::stream::iter(vec![
        watermark_message(2000),
    ]))));
    assert!(matches!(
        h.op.poll_next().await,
        OperatorPollResult::Continue
    ));
    let meta = h.store.load_key_state(&partition).await.expect("state");
    assert!(meta.evaluation.is_none());
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
async fn operator_checkpoint_restores_into_fresh_store() {
    let exec = window_exec_from_sql(SQL).await;
    let mut original = Harness::new(WindowOperatorConfig::new(exec.clone())).await;
    original
        .ingest(
            batch(vec![1000, 2000], vec![1.0, 2.0], vec!["A", "A"]),
            "A",
        )
        .await;
    let _ = original.watermark_and_output(2000).await;
    let _ = original.drain_passthrough_watermark().await;

    let checkpoint = original.op.checkpoint(1).await.expect("checkpoint");
    let mut restored = Harness::new(WindowOperatorConfig::new(exec)).await;
    restored
        .op
        .restore(SerializedRestore::new(checkpoint.into_bytes()))
        .await
        .expect("restore");

    let mut wro = open_wro(restored.store.clone(), restored.namespace.clone()).await;
    assert_eq!(
        request_sum(&mut wro, "A", 2000).await,
        ScalarValue::Float64(Some(3.0))
    );
}

#[tokio::test]
async fn pending_triggers_survive_checkpoint_restore() {
    let exec = window_exec_from_sql(SQL).await;
    let mut original = Harness::new(WindowOperatorConfig::new(exec.clone())).await;
    original
        .ingest(
            batch(vec![1000, 2000], vec![1.0, 2.0], vec!["A", "A"]),
            "A",
        )
        .await;

    let checkpoint = original.op.checkpoint(1).await.expect("checkpoint");
    let mut restored = Harness::new(WindowOperatorConfig::new(exec)).await;
    restored
        .op
        .restore(SerializedRestore::new(checkpoint.into_bytes()))
        .await
        .expect("restore");

    let output = restored.watermark_and_output(2000).await;
    let _ = restored.drain_passthrough_watermark().await;
    assert_window_values(
        &output,
        3,
        &[
            vec![ScalarValue::Float64(Some(1.0))],
            vec![ScalarValue::Float64(Some(3.0))],
        ],
        "restored pending triggers",
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
