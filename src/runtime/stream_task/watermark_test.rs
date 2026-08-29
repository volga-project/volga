use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::stream;
use futures::StreamExt;
use tokio::sync::Mutex;

use crate::common::message::Message;
use crate::runtime::observability::snapshot_types::StreamTaskStatus;
use crate::runtime::watermark::WatermarkAssignConfig;
use crate::transport::transport_client::DataReaderControl;

use super::checkpoint::CheckpointAligner;
use super::preprocess::create_preprocessed_input_stream;
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

    let held = assigner.on_data_message_at(
        "u",
        &ts_message(150),
        t0 + Duration::from_millis(10),
    );
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

#[tokio::test]
async fn stream_task_preprocess_injects_watermarks_after_data() {
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
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![250_i64])) as ArrayRef,
        ],
    )
    .unwrap();

    let input = Box::pin(stream::iter(vec![
        Message::new(Some("u0".to_string()), b1, None, None),
        Message::new(Some("u0".to_string()), b2, None, None),
    ]));

    let cfg = WatermarkAssignConfig::new(
        0,
        TimeHint::ColumnName {
            name: "ts_ms".to_string(),
        },
    );

    let mut out = create_preprocessed_input_stream(
        input,
        Arc::<str>::from("v0"),
        Some(cfg),
        vec!["u0".to_string()],
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU8::new(StreamTaskStatus::Running as u8)),
        CheckpointAligner::new(&["u0".to_string()], DataReaderControl::empty_for_test()),
        None,
        0,
    );

    let mut seen = Vec::new();
    while let Some(m) = out.next().await {
        seen.push(m);
    }

    assert!(matches!(seen[0], Message::Regular(_)));
    assert!(matches!(seen[1], Message::Watermark(_)));
    assert!(matches!(seen[2], Message::Regular(_)));
    assert!(matches!(seen[3], Message::Watermark(_)));

    let wm1 = match &seen[1] {
        Message::Watermark(w) => w.watermark_value,
        _ => unreachable!(),
    };
    let wm2 = match &seen[3] {
        Message::Watermark(w) => w.watermark_value,
        _ => unreachable!(),
    };

    assert_eq!(wm1, 120);
    assert_eq!(wm2, 250);
}

#[tokio::test]
async fn stream_task_preprocess_is_per_upstream_and_min_merged() {
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
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["B"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![50_i64])) as ArrayRef,
        ],
    )
    .unwrap();

    let m0 = Message::new(Some("u0".to_string()), b_u0, None, None);
    let m1 = Message::new(Some("u1".to_string()), b_u1, None, None);

    let input = Box::pin(stream::iter(vec![m0, m1]));
    let cfg = WatermarkAssignConfig::new(
        0,
        TimeHint::ColumnName {
            name: "ts_ms".to_string(),
        },
    );

    let mut out = create_preprocessed_input_stream(
        input,
        Arc::<str>::from("v0"),
        Some(cfg),
        vec!["u0".to_string(), "u1".to_string()],
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU8::new(StreamTaskStatus::Running as u8)),
        CheckpointAligner::new(
            &["u0".to_string(), "u1".to_string()],
            DataReaderControl::empty_for_test(),
        ),
        None,
        0,
    );

    let mut seen = Vec::new();
    while let Some(m) = out.next().await {
        seen.push(m);
    }

    assert!(matches!(seen[0], Message::Regular(_)));
    assert!(matches!(seen[1], Message::Regular(_)));
    assert!(matches!(seen[2], Message::Watermark(_)));

    let wm = match &seen[2] {
        Message::Watermark(w) => w.watermark_value,
        _ => unreachable!(),
    };
    assert_eq!(wm, 50);
}

#[tokio::test]
async fn idle_upstream_is_excluded_from_min_merge() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("ts_ms", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![100_i64])) as ArrayRef,
        ],
    )
    .unwrap();

    let input = Box::pin(async_stream::stream! {
        tokio::time::sleep(Duration::from_millis(5)).await;
        yield Message::new(Some("u0".to_string()), batch, None, None);
    });

    let mut cfg = WatermarkAssignConfig::new(
        0,
        TimeHint::ColumnName {
            name: "ts_ms".to_string(),
        },
    );
    cfg.idle_timeout_ms = Some(1);

    let mut out = create_preprocessed_input_stream(
        input,
        Arc::<str>::from("v0"),
        Some(cfg),
        vec!["u0".to_string(), "u1".to_string()],
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU8::new(StreamTaskStatus::Running as u8)),
        CheckpointAligner::new(
            &["u0".to_string(), "u1".to_string()],
            DataReaderControl::empty_for_test(),
        ),
        None,
        0,
    );

    let mut seen = Vec::new();
    while let Some(m) = out.next().await {
        seen.push(m);
    }

    assert!(matches!(seen[0], Message::Regular(_)));
    assert!(matches!(seen[1], Message::Watermark(_)));
}

#[tokio::test]
async fn idle_timeout_disabled_blocks_min_merge() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("ts_ms", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["A"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![100_i64])) as ArrayRef,
        ],
    )
    .unwrap();

    let input = Box::pin(async_stream::stream! {
        tokio::time::sleep(Duration::from_millis(5)).await;
        yield Message::new(Some("u0".to_string()), batch, None, None);
    });

    let mut cfg = WatermarkAssignConfig::new(
        0,
        TimeHint::ColumnName {
            name: "ts_ms".to_string(),
        },
    );
    cfg.idle_timeout_ms = None;

    let mut out = create_preprocessed_input_stream(
        input,
        Arc::<str>::from("v0"),
        Some(cfg),
        vec!["u0".to_string(), "u1".to_string()],
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU8::new(StreamTaskStatus::Running as u8)),
        CheckpointAligner::new(
            &["u0".to_string(), "u1".to_string()],
            DataReaderControl::empty_for_test(),
        ),
        None,
        0,
    );

    let mut seen = Vec::new();
    while let Some(m) = out.next().await {
        seen.push(m);
    }

    assert!(matches!(seen[0], Message::Regular(_)));
    assert_eq!(seen.len(), 1);
}

fn ts_up(upstream: &str, ts_ms: i64) -> Message {
    let mut m = ts_message(ts_ms);
    m.set_upstream_vertex_id(upstream.to_string());
    m
}

fn preprocess_assign(
    input: crate::runtime::operators::operator::MessageStream,
    interval: Duration,
) -> crate::runtime::operators::operator::MessageStream {
    let cfg = WatermarkAssignConfig::new(
        0,
        TimeHint::ColumnName {
            name: "ts_ms".to_string(),
        },
    )
    .with_emit_interval(interval);
    create_preprocessed_input_stream(
        input,
        Arc::<str>::from("v0"),
        Some(cfg),
        vec!["u0".to_string()],
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU8::new(StreamTaskStatus::Running as u8)),
        CheckpointAligner::new(&["u0".to_string()], DataReaderControl::empty_for_test()),
        None,
        0,
    )
}

#[tokio::test]
async fn stream_task_preprocess_timer_emits_held_watermark_without_next_record() {
    let interval = Duration::from_millis(50);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut out = preprocess_assign(
        Box::pin(tokio_stream::wrappers::UnboundedReceiverStream::new(rx)),
        interval,
    );

    tx.send(ts_up("u0", 100)).unwrap();
    assert!(matches!(out.next().await, Some(Message::Regular(_))));
    match out.next().await {
        Some(Message::Watermark(w)) => assert_eq!(w.watermark_value, 100),
        other => panic!("expected first watermark, got {other:?}"),
    }

    tx.send(ts_up("u0", 150)).unwrap();
    assert!(matches!(out.next().await, Some(Message::Regular(_))));

    let held = tokio::time::timeout(Duration::from_millis(500), out.next())
        .await
        .expect("timer should emit held watermark with no following record")
        .expect("stream ended");
    match held {
        Message::Watermark(w) => assert_eq!(w.watermark_value, 150),
        other => panic!("expected coalesced watermark, got {other:?}"),
    }
    drop(tx);
}

#[tokio::test]
async fn stream_task_preprocess_flushes_held_watermark_before_aligned_barrier() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut out = preprocess_assign(
        Box::pin(tokio_stream::wrappers::UnboundedReceiverStream::new(rx)),
        Duration::from_secs(30),
    );

    tx.send(ts_up("u0", 100)).unwrap();
    assert!(matches!(out.next().await, Some(Message::Regular(_))));
    match out.next().await {
        Some(Message::Watermark(w)) => assert_eq!(w.watermark_value, 100),
        other => panic!("expected first watermark, got {other:?}"),
    }

    tx.send(ts_up("u0", 180)).unwrap();
    assert!(matches!(out.next().await, Some(Message::Regular(_))));

    tx.send(Message::CheckpointBarrier(
        crate::common::message::CheckpointBarrierMessage::new("u0".to_string(), 1, 0, None),
    ))
    .unwrap();

    match out.next().await {
        Some(Message::Watermark(w)) => assert_eq!(w.watermark_value, 180),
        other => panic!("expected flushed watermark before barrier, got {other:?}"),
    }
    assert!(
        matches!(out.next().await, Some(Message::CheckpointBarrier(_))),
        "aligned barrier should follow flushed watermark"
    );
    drop(tx);
}
