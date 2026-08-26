use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::{stream, FutureExt, StreamExt};

use crate::common::message::{Message, WatermarkMessage};
use crate::runtime::operators::operator::{
    NextInputs, OperatorBase, OperatorConfig, OperatorTrait,
};

fn int_batch(rows: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(rows))]).unwrap()
}

fn data(rows: Vec<i64>) -> Message {
    Message::new(None, int_batch(rows), None, None)
}

fn wm(v: u64) -> Message {
    Message::Watermark(WatermarkMessage::new("src".to_string(), v, None))
}

fn data_len(out: &NextInputs) -> usize {
    match out {
        NextInputs::Data(msgs) => msgs.len(),
        other => panic!("expected Data, got {other:?}"),
    }
}

fn base_with_input(
    stream: impl futures::Stream<Item = Message> + Send + Sync + 'static,
) -> OperatorBase {
    let mut base = OperatorBase::new(OperatorConfig::ChainedConfig(vec![]));
    base.set_input(Some(Box::pin(stream)));
    base
}

#[tokio::test]
async fn empty_stream() {
    let mut base = base_with_input(stream::empty());
    let out = base.next_inputs(8).await;
    assert!(matches!(out, NextInputs::Exhausted));
}

#[tokio::test]
async fn does_not_block_waiting_to_fill_budget() {
    let mut base = base_with_input(stream::iter(vec![data(vec![1])]).chain(stream::pending()));
    let out = tokio::time::timeout(Duration::from_millis(200), base.next_inputs(8))
        .await
        .expect("must not wait on pending input");
    assert_eq!(data_len(&out), 1);
}

#[tokio::test]
async fn stops_before_watermark() {
    let mut base = base_with_input(stream::iter(vec![data(vec![1]), data(vec![2]), wm(9)]));
    let out = base.next_inputs(8).await;
    assert_eq!(data_len(&out), 2);
    match Pin::new(base.input.as_mut().unwrap()).peek().now_or_never() {
        Some(Some(Message::Watermark(w))) => assert_eq!(w.watermark_value, 9),
        other => panic!("expected peeked watermark, got {other:?}"),
    }
    let again = base.next_inputs(8).await;
    match again {
        NextInputs::Control(Message::Watermark(w)) => assert_eq!(w.watermark_value, 9),
        other => panic!("next fetch should see the watermark, got {other:?}"),
    }
}

#[tokio::test]
async fn record_limit_leaves_over_budget_message_peeked() {
    let mut base = base_with_input(stream::iter(vec![data(vec![1, 2, 3]), data(vec![4, 5, 6])]));
    let out = base.next_inputs(5).await;
    assert_eq!(data_len(&out), 1);
    assert!(matches!(
        Pin::new(base.input.as_mut().unwrap()).peek().now_or_never(),
        Some(Some(Message::Regular(_)))
    ));
}

#[tokio::test]
async fn first_message_accepted_even_when_over_record_limit() {
    let mut base = base_with_input(stream::iter(vec![data(vec![1, 2, 3]), data(vec![4])]));
    let out = base.next_inputs(1).await;
    assert_eq!(data_len(&out), 1);
    assert!(matches!(
        Pin::new(base.input.as_mut().unwrap()).peek().now_or_never(),
        Some(Some(Message::Regular(_)))
    ));
}
