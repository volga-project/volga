use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::common::message::{CheckpointBarrierMessage, Message, WatermarkMessage};
use crate::runtime::watermark::{TimeHint, WatermarkAssignConfig};

use super::progress::InputProgress;
use super::watermark::WatermarkAssignerState;

#[test]
fn assigner_tracks_per_upstream_independently() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("ts_ms", DataType::Int64, false),
    ]));
    let b = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![100_i64])) as ArrayRef,
        ],
    )
    .unwrap();
    let msg = Message::new(None, b, None, None);

    let cfg = WatermarkAssignConfig {
        out_of_orderness_ms: 0,
        time_hint: TimeHint::ColumnName {
            name: "ts_ms".to_string(),
        },
        idle_timeout_ms: Some(WatermarkAssignConfig::DEFAULT_IDLE_TIMEOUT_MS),
        emit_interval: Duration::ZERO,
    };
    let mut assigner = WatermarkAssignerState::new(cfg);

    let wm1 = assigner.on_data_message("upA", &msg).unwrap();
    assert_eq!(wm1.metadata.upstream_vertex_id.as_deref(), Some("upA"));
    assert_eq!(wm1.watermark_value, 100);

    let wm2 = assigner.on_data_message("upB", &msg).unwrap();
    assert_eq!(wm2.metadata.upstream_vertex_id.as_deref(), Some("upB"));
    assert_eq!(wm2.watermark_value, 100);
}

fn ts_message(ts_ms: i64) -> Message {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("ts_ms", DataType::Int64, false),
    ]));
    let b = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![ts_ms])) as ArrayRef,
        ],
    )
    .unwrap();
    Message::new(None, b, None, None)
}

fn ts_up(upstream: &str, ts_ms: i64) -> Message {
    let mut m = ts_message(ts_ms);
    m.set_upstream_vertex_id(upstream.to_string());
    m
}

fn assigner_with_interval(interval: Duration) -> WatermarkAssignerState {
    WatermarkAssignerState::new(
        WatermarkAssignConfig::new(
            0,
            TimeHint::ColumnName {
                name: "ts_ms".to_string(),
            },
        )
        .with_emit_interval(interval),
    )
}

fn column_assign(idle_timeout_ms: Option<u64>) -> WatermarkAssignConfig {
    let mut cfg = WatermarkAssignConfig::new(
        0,
        TimeHint::ColumnName {
            name: "ts_ms".to_string(),
        },
    );
    cfg.idle_timeout_ms = idle_timeout_ms;
    cfg
}

#[test]
fn zero_interval_emits_on_every_advance() {
    let mut assigner = assigner_with_interval(Duration::ZERO);
    let t0 = Instant::now();
    let wm1 = assigner
        .on_data_message_at("u", &ts_message(100), t0)
        .unwrap();
    assert_eq!(wm1.watermark_value, 100);
    let wm2 = assigner
        .on_data_message_at("u", &ts_message(150), t0)
        .unwrap();
    assert_eq!(wm2.watermark_value, 150);
}

#[test]
fn coalesce_holds_until_interval() {
    let mut assigner = assigner_with_interval(Duration::from_millis(200));
    let t0 = Instant::now();
    let first = assigner
        .on_data_message_at("u", &ts_message(100), t0)
        .unwrap();
    assert_eq!(first.watermark_value, 100);

    let held = assigner.on_data_message_at("u", &ts_message(150), t0 + Duration::from_millis(10));
    assert!(held.is_none());
    assert!(assigner
        .try_emit_due(t0 + Duration::from_millis(10))
        .is_empty());

    let due = assigner.try_emit_due(t0 + Duration::from_millis(200));
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].watermark_value, 150);
}

#[test]
fn timer_does_not_emit_when_already_published() {
    let mut assigner = assigner_with_interval(Duration::from_millis(50));
    let t0 = Instant::now();
    assert!(assigner
        .on_data_message_at("u", &ts_message(100), t0)
        .is_some());
    assert!(assigner
        .try_emit_due(t0 + Duration::from_millis(50))
        .is_empty());
    assert!(assigner.next_emit_deadline().is_none());
}

#[test]
fn flush_emits_unpublished_before_interval() {
    let mut assigner = assigner_with_interval(Duration::from_millis(200));
    let t0 = Instant::now();
    assigner
        .on_data_message_at("u", &ts_message(100), t0)
        .unwrap();
    assert!(assigner
        .on_data_message_at("u", &ts_message(180), t0 + Duration::from_millis(5))
        .is_none());
    let flushed = assigner.flush(t0 + Duration::from_millis(5));
    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].watermark_value, 180);
}

#[test]
fn data_path_emits_when_interval_elapsed() {
    let mut assigner = assigner_with_interval(Duration::from_millis(200));
    let t0 = Instant::now();
    assigner
        .on_data_message_at("u", &ts_message(100), t0)
        .unwrap();
    assert!(assigner
        .on_data_message_at("u", &ts_message(120), t0 + Duration::from_millis(10))
        .is_none());
    let late = assigner
        .on_data_message_at("u", &ts_message(300), t0 + Duration::from_millis(200))
        .unwrap();
    assert_eq!(late.watermark_value, 300);
}

#[test]
fn event_time_interval_emits_without_waiting_for_wall() {
    let mut assigner = assigner_with_interval(Duration::from_millis(200));
    let t0 = Instant::now();
    assigner
        .on_data_message_at("u", &ts_message(100), t0)
        .unwrap();
    assert!(assigner
        .on_data_message_at("u", &ts_message(299), t0 + Duration::from_millis(1))
        .is_none());
    let crossed = assigner
        .on_data_message_at("u", &ts_message(300), t0 + Duration::from_millis(1))
        .unwrap();
    assert_eq!(crossed.watermark_value, 300);
}

#[tokio::test]
async fn input_progress_assigns_watermarks_after_data() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("ts_ms", DataType::Int64, false),
    ]));
    let b1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A", "A"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![100_i64, 120_i64])) as ArrayRef,
        ],
    )
    .unwrap();
    let b2 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![250_i64])) as ArrayRef,
        ],
    )
    .unwrap();

    let mut progress = InputProgress::for_test(Some(column_assign(None)), &["u0"]);
    let wm1 = progress
        .assign_and_merge(&Message::new(Some("u0".to_string()), b1, None, None))
        .await
        .unwrap();
    let wm2 = progress
        .assign_and_merge(&Message::new(Some("u0".to_string()), b2, None, None))
        .await
        .unwrap();
    assert_eq!(wm1.watermark_value, 120);
    assert_eq!(wm2.watermark_value, 250);
}

#[tokio::test]
async fn input_progress_is_per_upstream_and_min_merged() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("ts_ms", DataType::Int64, false),
    ]));
    let b_u0 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![100_i64])) as ArrayRef,
        ],
    )
    .unwrap();
    let b_u1 = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["B"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![50_i64])) as ArrayRef,
        ],
    )
    .unwrap();

    let mut progress = InputProgress::for_test(Some(column_assign(None)), &["u0", "u1"]);
    assert!(progress
        .assign_and_merge(&Message::new(Some("u0".to_string()), b_u0, None, None))
        .await
        .is_none());
    let wm = progress
        .assign_and_merge(&Message::new(Some("u1".to_string()), b_u1, None, None))
        .await
        .unwrap();
    assert_eq!(wm.watermark_value, 50);
}

#[tokio::test]
async fn input_progress_preserves_upstream_watermark_extras() {
    let mut progress = InputProgress::for_test(None, &["u0"]);
    let mut wm = WatermarkMessage::new("u0".to_string(), 100, Some(1));
    wm.metadata.extras = Some(HashMap::from([("trace".to_string(), "u0".to_string())]));
    let out = progress.on_upstream_watermark(wm).await;
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].watermark_value, 100);
    assert_eq!(out[0].metadata.upstream_vertex_id.as_deref(), Some("v0"));
    assert_eq!(
        out[0]
            .metadata
            .extras
            .as_ref()
            .and_then(|e| e.get("trace"))
            .map(String::as_str),
        Some("u0")
    );
}

#[tokio::test]
async fn idle_upstream_is_excluded_from_min_merge() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("ts_ms", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![100_i64])) as ArrayRef,
        ],
    )
    .unwrap();

    let mut progress = InputProgress::for_test(Some(column_assign(Some(1))), &["u0", "u1"]);
    tokio::time::sleep(Duration::from_millis(5)).await;
    let wm = progress
        .assign_and_merge(&Message::new(Some("u0".to_string()), batch, None, None))
        .await
        .unwrap();
    assert_eq!(wm.watermark_value, 100);
}

#[tokio::test]
async fn idle_timeout_disabled_blocks_min_merge() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("ts_ms", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![100_i64])) as ArrayRef,
        ],
    )
    .unwrap();

    let mut progress = InputProgress::for_test(Some(column_assign(None)), &["u0", "u1"]);
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(progress
        .assign_and_merge(&Message::new(Some("u0".to_string()), batch, None, None))
        .await
        .is_none());
}

fn interval_progress(interval: Duration) -> InputProgress {
    let cfg = WatermarkAssignConfig::new(
        0,
        TimeHint::ColumnName {
            name: "ts_ms".to_string(),
        },
    )
    .with_emit_interval(interval);
    InputProgress::for_test(Some(cfg), &["u0"])
}

#[tokio::test]
async fn input_progress_timer_emits_held_watermark() {
    let interval = Duration::from_millis(50);
    let mut progress = interval_progress(interval);

    let first = progress.assign_and_merge(&ts_up("u0", 100)).await.unwrap();
    assert_eq!(first.watermark_value, 100);
    // Event time moved by less than `interval`, so only the wall timer should emit.
    assert!(progress.assign_on_data(&ts_up("u0", 120)).is_none());

    tokio::time::sleep(interval + Duration::from_millis(10)).await;
    let due = progress.on_timer().await;
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].watermark_value, 120);
}

#[tokio::test]
async fn input_progress_flushes_held_watermark_before_aligned_barrier() {
    let mut progress = interval_progress(Duration::from_secs(30));

    let first = progress.assign_and_merge(&ts_up("u0", 100)).await.unwrap();
    assert_eq!(first.watermark_value, 100);
    assert!(progress.assign_on_data(&ts_up("u0", 180)).is_none());

    let barrier = CheckpointBarrierMessage::new("u0".to_string(), 1, 0, None);
    let (_, _, wms) = progress
        .on_aligned_barrier(&barrier)
        .await
        .unwrap()
        .expect("single-upstream barrier should align");
    assert_eq!(wms.len(), 1);
    assert_eq!(wms[0].watermark_value, 180);
}
