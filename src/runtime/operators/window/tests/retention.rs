//! WO retention: consumed triggers and raw/tile prune after watermark advance.

use futures::TryStreamExt;

use crate::runtime::checkpoint::SerializedRestore;
use crate::runtime::operators::operator::{OperatorPollResult, OperatorTrait};
use crate::runtime::operators::window::model::{Cursor, RawRun, TileRun};
use crate::runtime::operators::window::operator::{WindowOperatorConfig, WindowOutputMode};
use crate::runtime::operators::window::spec::WindowSpec;
use crate::runtime::operators::window::store::{PartitionKey, WindowOperatorStore};
use crate::runtime::operators::window::tests::harness::{
    batch, key, watermark_message, window_exec_from_sql, Harness,
};
use crate::runtime::operators::window::{TileConfig, TimeGranularity};

const SQL: &str = r#"SELECT timestamp, value, partition_key, SUM(value) OVER w as sum_val
FROM test_table
WINDOW w AS (
  PARTITION BY partition_key
  ORDER BY timestamp
  RANGE BETWEEN INTERVAL '5000' MILLISECOND PRECEDING AND CURRENT ROW
)"#;

async fn due_trigger_count(h: &Harness, through: i64) -> usize {
    let mut due = h
        .store
        .stream_due(&h.namespace, None, Cursor::new(through, u64::MAX));
    let mut count = 0;
    while let Some(page) = due.try_next().await.expect("due page") {
        count += page.into_iter().map(|work| work.triggers.len()).sum::<usize>();
    }
    count
}

async fn raw_timestamps(h: &Harness, partition: &str) -> Vec<i64> {
    let partition = PartitionKey::new(&h.namespace, &key(partition));
    let batches = h
        .store
        .load_raw(
            &partition,
            &[RawRun {
                from: Cursor::new(i64::MIN, 0),
                to: Cursor::new(i64::MAX, u64::MAX),
            }],
        )
        .await
        .expect("load raw");
    let mut out = Vec::new();
    for batch in batches {
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMillisecondArray>()
            .expect("ts");
        for i in 0..batch.num_rows() {
            out.push(ts.value(i));
        }
    }
    out.sort_unstable();
    out
}

async fn tile_starts(h: &Harness, partition: &str) -> Vec<i64> {
    let partition = PartitionKey::new(&h.namespace, &key(partition));
    let tiles = h
        .store
        .load_tiles(
            &partition,
            &[TileRun {
                granularity: TimeGranularity::Seconds(1),
                start_ts: i64::MIN / 2,
                end_ts_exclusive: i64::MAX / 2,
            }],
        )
        .await
        .expect("load tiles");
    let mut starts: Vec<i64> = tiles.keys().map(|(_, start)| *start).collect();
    starts.sort_unstable();
    starts
}

#[tokio::test]
async fn consumed_triggers_are_removed_after_completed_watermark() {
    let exec = window_exec_from_sql(SQL).await;
    let mut h = Harness::new(WindowOperatorConfig::new(exec)).await;
    h.ingest(
        batch(vec![1000, 2000, 8000], vec![1.0, 2.0, 3.0], vec!["A", "A", "A"]),
        "A",
    )
    .await;
    assert_eq!(due_trigger_count(&h, 8_000).await, 3);

    let _ = h.watermark_and_output(2_000).await;
    let _ = h.drain_passthrough_watermark().await;
    assert_eq!(due_trigger_count(&h, 2_000).await, 0);
    assert_eq!(due_trigger_count(&h, 8_000).await, 1);
}

#[tokio::test]
async fn future_triggers_remain_across_partial_watermarks() {
    let exec = window_exec_from_sql(SQL).await;
    let mut h = Harness::new(WindowOperatorConfig::new(exec)).await;
    h.ingest(
        batch(vec![1000, 2000, 5000], vec![1.0, 2.0, 3.0], vec!["A", "A", "A"]),
        "A",
    )
    .await;

    let partial = h.watermark_and_output(2_000).await;
    let _ = h.drain_passthrough_watermark().await;
    assert_eq!(partial.num_rows(), 2);
    assert_eq!(due_trigger_count(&h, 5_000).await, 1);

    let rest = h.watermark_and_output(5_000).await;
    let _ = h.drain_passthrough_watermark().await;
    assert_eq!(rest.num_rows(), 1);
    assert_eq!(due_trigger_count(&h, 5_000).await, 0);
}

#[tokio::test]
async fn raw_and_tiles_are_retained_through_wo_floor() {
    let exec = window_exec_from_sql(SQL).await;
    let mut cfg = WindowOperatorConfig::new(exec);
    cfg.spec = WindowSpec {
        lateness: 0,
        tiling: Some(TileConfig::new(vec![TimeGranularity::Seconds(1)]).unwrap()),
    };
    let mut h = Harness::new(cfg).await;
    h.ingest(
        batch(
            vec![1_000, 4_000, 5_000, 10_000],
            vec![1.0, 2.0, 3.0, 4.0],
            vec!["A", "A", "A", "A"],
        ),
        "A",
    )
    .await;

    let _ = h.watermark_and_output(10_000).await;
    let _ = h.drain_passthrough_watermark().await;

    // floor = 10000 - 5000 - 0 = 5000; keep ts >= 5000
    assert_eq!(raw_timestamps(&h, "A").await, vec![5_000, 10_000]);
    assert_eq!(tile_starts(&h, "A").await, vec![5_000, 10_000]);

    let partition = PartitionKey::new(&h.namespace, &key("A"));
    let meta = h.store.load_key_state(&partition).await.expect("meta");
    assert_eq!(meta.next_seq, 4);
    assert!(meta.evaluation.is_some());
}

#[tokio::test]
async fn lateness_extends_retention_floor() {
    let exec = window_exec_from_sql(SQL).await;
    let mut cfg = WindowOperatorConfig::new(exec);
    cfg.spec = WindowSpec {
        lateness: 2_000,
        ..WindowSpec::default()
    };
    let mut h = Harness::new(cfg).await;
    h.ingest(
        batch(
            vec![1_000, 3_000, 5_000, 10_000],
            vec![1.0, 2.0, 3.0, 4.0],
            vec!["A", "A", "A", "A"],
        ),
        "A",
    )
    .await;

    let _ = h.watermark_and_output(10_000).await;
    let _ = h.drain_passthrough_watermark().await;

    // floor = 10000 - 5000 - 2000 = 3000
    assert_eq!(raw_timestamps(&h, "A").await, vec![3_000, 5_000, 10_000]);
}

#[tokio::test]
async fn checkpoint_before_cleanup_restores_pre_prune_state() {
    let exec = window_exec_from_sql(SQL).await;
    let mut original = Harness::new(WindowOperatorConfig::new(exec.clone())).await;
    original
        .ingest(
            batch(vec![1_000, 2_000, 10_000], vec![1.0, 2.0, 3.0], vec!["A", "A", "A"]),
            "A",
        )
        .await;
    // Checkpoint with all rows/triggers still present (no watermark yet).
    let checkpoint = original.op.checkpoint(1).await.expect("checkpoint");

    let _ = original.watermark_and_output(10_000).await;
    let _ = original.drain_passthrough_watermark().await;
    // floor = 10000 - 5000 = 5000 → only ts=10000 remains
    assert_eq!(raw_timestamps(&original, "A").await, vec![10_000]);
    assert_eq!(due_trigger_count(&original, 10_000).await, 0);

    let mut restored = Harness::new(WindowOperatorConfig::new(exec)).await;
    restored
        .op
        .restore(SerializedRestore::new(checkpoint.into_bytes()))
        .await
        .expect("restore");

    assert_eq!(
        raw_timestamps(&restored, "A").await,
        vec![1_000, 2_000, 10_000]
    );
    assert_eq!(due_trigger_count(&restored, 10_000).await, 3);
}

#[tokio::test]
async fn state_only_prunes_without_triggers_or_evaluation_state() {
    let exec = window_exec_from_sql(SQL).await;
    let mut cfg = WindowOperatorConfig::new(exec);
    cfg.output_mode = WindowOutputMode::StateOnly;
    cfg.spec = WindowSpec {
        lateness: 0,
        tiling: Some(TileConfig::new(vec![TimeGranularity::Seconds(1)]).unwrap()),
    };
    let mut h = Harness::new(cfg).await;
    h.ingest(
        batch(
            vec![1_000, 5_000, 10_000],
            vec![1.0, 2.0, 3.0],
            vec!["A", "A", "A"],
        ),
        "A",
    )
    .await;
    assert_eq!(due_trigger_count(&h, 10_000).await, 0);

    h.op.set_input(Some(Box::pin(futures::stream::iter(vec![
        watermark_message(10_000),
    ]))));
    assert!(matches!(
        h.op.poll_next().await,
        OperatorPollResult::Continue
    ));
    h.run_maintenance().await;

    assert_eq!(raw_timestamps(&h, "A").await, vec![5_000, 10_000]);
    assert_eq!(tile_starts(&h, "A").await, vec![5_000, 10_000]);
    let partition = PartitionKey::new(&h.namespace, &key("A"));
    let meta = h.store.load_key_state(&partition).await.expect("meta");
    assert_eq!(meta.next_seq, 3);
    assert!(meta.evaluation.is_none());
}
